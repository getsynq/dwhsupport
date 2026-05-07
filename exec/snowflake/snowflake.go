package snowflake

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/snowflakedb/gosnowflake"
	_ "github.com/snowflakedb/gosnowflake"
	"github.com/youmark/pkcs8"
)

// https://github.com/snowflakedb/gosnowflake/blob/099708d318689634a558f705ccc19b3b7b278972/doc.go#L107
const SPNApplicationId = "SYNQ_Platform"

type SnowflakeConf struct {
	User                 string
	Password             string
	PrivateKey           []byte
	PrivateKeyFile       string
	PrivateKeyPassphrase string
	Account              string
	Warehouse            string
	Databases            []string
	Role                 string
	// Token is an OAuth access token for token-based authentication.
	// When set, AuthTypeOAuth is used automatically.
	Token string
	// AuthType specifies the authentication method: empty (default, uses password or private_key),
	// "externalbrowser" (SSO via browser). When set to "externalbrowser", opens browser for SSO login
	// and caches the ID token locally in the OS credential manager (Keychain on macOS, Credential Manager on Windows).
	AuthType string
	// Transporter is an optional http.RoundTripper to use for the underlying HTTP client. Use [gosnowflake.SnowflakeTransport] as a base.
	Transporter http.RoundTripper
}

type Executor interface {
	stdsql.StdSqlExecutor
	QueryRows(ctx context.Context, q string, args ...any) (*sqlx.Rows, error)
}

var _ stdsql.StdSqlExecutor = &SnowflakeExecutor{}

type SnowflakeExecutor struct {
	conf *SnowflakeConf
	db   *sqlx.DB
}

func (e *SnowflakeExecutor) GetDb() *sqlx.DB {
	return e.db
}

// parsePrivateKey decodes and parses a PEM-encoded private key.
// It supports both unencrypted PKCS8 keys and encrypted PKCS8 keys with a passphrase.
func parsePrivateKey(privateKeyPEM []byte, passphrase string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(privateKeyPEM)
	if block == nil {
		return nil, errors.New("failed to decode PEM block containing private key")
	}

	var privKey any
	var err error

	switch block.Type {
	case "ENCRYPTED PRIVATE KEY":
		if passphrase == "" {
			return nil, errors.New("encrypted private key is provided but no passphrase is set")
		}
		privKey, err = pkcs8.ParsePKCS8PrivateKey(block.Bytes, []byte(passphrase))
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse encrypted private key")
		}
	case "PRIVATE KEY":
		if passphrase != "" {
			return nil, errors.New("passphrase provided but private key is not encrypted")
		}
		privKey, err = x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse private key")
		}
	default:
		return nil, errors.Errorf("unsupported PEM block type: %s", block.Type)
	}

	rsaKey, ok := privKey.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("private key is not an RSA key")
	}

	return rsaKey, nil
}

// cleanAccountName removes the .snowflakecomputing.com suffix from the account name if present.
// The Snowflake driver automatically appends this suffix, so we need to ensure it's not duplicated.
// It also handles URLs by extracting the hostname first.
//
// Examples:
//   - "myaccount" -> "myaccount"
//   - "myaccount.snowflakecomputing.com" -> "myaccount"
//   - "https://myaccount.snowflakecomputing.com" -> "myaccount"
//   - "https://myaccount.snowflakecomputing.com/" -> "myaccount"
//   - "myaccount.us-east-1" -> "myaccount.us-east-1"
func cleanAccountName(account string) string {
	account = strings.TrimSpace(account)

	// If it looks like a URL, parse it and extract the hostname
	if strings.HasPrefix(account, "http://") || strings.HasPrefix(account, "https://") {
		parsed, err := url.Parse(account)
		if err == nil && parsed.Host != "" {
			account = parsed.Host
		}
	}

	// Remove trailing slash if present
	account = strings.TrimSuffix(account, "/")

	// Remove the .snowflakecomputing.com suffix
	return strings.TrimSuffix(account, ".snowflakecomputing.com")
}

// buildSnowflakeConfig creates a gosnowflake.Config from SnowflakeConf.
// This is separated from NewSnowflakeExecutor to allow unit testing of the configuration logic.
// Note: This function does not handle PrivateKeyFile loading (requires file I/O) - that's done in NewSnowflakeExecutor.
func buildSnowflakeConfig(conf *SnowflakeConf) (*gosnowflake.Config, error) {
	// When connecting as an individual user (OAuth token or SSO browser), don't set a
	// default database on the connection. The user's role may not have access to the
	// databases configured in the workspace integration. Queries use fully qualified names.
	isUserAuth := conf.Token != "" || strings.ToLower(conf.AuthType) == "externalbrowser"
	database := ""
	if !isUserAuth && len(conf.Databases) > 0 {
		database = conf.Databases[0]
	}

	c := &gosnowflake.Config{
		Account:             cleanAccountName(conf.Account),
		User:                conf.User,
		Password:            conf.Password,
		Warehouse:           conf.Warehouse,
		Role:                conf.Role,
		Database:            database,
		Application:         SPNApplicationId,
		Params:              map[string]*string{},
		DisableConsoleLogin: gosnowflake.ConfigBoolTrue,
		LoginTimeout:        30 * time.Second, // Timeout for JWT token authentication exchange
		Transporter:         conf.Transporter,
	}

	// Handle authentication type
	switch {
	case conf.Token != "":
		// OAuth token-based authentication (e.g., user OAuth credentials)
		c.Authenticator = gosnowflake.AuthTypeOAuth
		c.Token = conf.Token
	case strings.ToLower(conf.AuthType) == "externalbrowser":
		// SSO via browser - opens browser for login and caches ID token
		c.Authenticator = gosnowflake.AuthTypeExternalBrowser
		c.ExternalBrowserTimeout = 120 * time.Second
		// Enable token caching in OS credential manager (Keychain on macOS, Credential Manager on Windows)
		c.ClientStoreTemporaryCredential = gosnowflake.ConfigBoolTrue
		// Allow browser-based login (don't disable console login for SSO)
		c.DisableConsoleLogin = gosnowflake.ConfigBoolFalse
	default:
		// Default: password or private key authentication.
		// Handle inline private key (PrivateKeyFile is handled in NewSnowflakeExecutor).
		// Surface parse errors instead of silently falling through to password auth —
		// otherwise a bad PEM/passphrase combination becomes a misleading
		// "260002: password is empty" from the driver.
		if len(conf.PrivateKey) > 0 {
			privKey, err := parsePrivateKey(conf.PrivateKey, conf.PrivateKeyPassphrase)
			if err != nil {
				return nil, err
			}
			c.PrivateKey = privKey
			c.Authenticator = gosnowflake.AuthTypeJwt
		}
	}

	return c, nil
}

func NewSnowflakeExecutor(ctx context.Context, conf *SnowflakeConf) (*SnowflakeExecutor, error) {
	c, err := buildSnowflakeConfig(conf)
	if err != nil {
		return nil, exec.NewAuthError(err)
	}

	// Handle private key file loading (not in buildSnowflakeConfig to keep it side-effect free for testing)
	if strings.ToLower(conf.AuthType) != "externalbrowser" && c.PrivateKey == nil && len(conf.PrivateKeyFile) > 0 {
		keyData, err := os.ReadFile(conf.PrivateKeyFile)
		if err != nil {
			return nil, exec.NewAuthError(errors.Wrap(err, "failed to read private key file"))
		}
		privKey, err := parsePrivateKey(keyData, conf.PrivateKeyPassphrase)
		if err != nil {
			return nil, exec.NewAuthError(err)
		}
		c.PrivateKey = privKey
		c.Authenticator = gosnowflake.AuthTypeJwt
	}

	connector := gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, *c)
	stdDb := sql.OpenDB(connector)
	db := sqlx.NewDb(stdDb, "snowflake")

	if err := db.PingContext(ctx); err != nil {
		return nil, exec.NewAuthError(err)
	}

	return &SnowflakeExecutor{conf: conf, db: db}, nil
}

// enrichCtx adds Snowflake-specific context enrichment: query tag from QueryContext
// and query ID channel for stats collection.
func (e *SnowflakeExecutor) enrichCtx(ctx context.Context) context.Context {
	ctx = EnrichSnowflakeContext(ctx, e.db.DB)
	if qc := querycontext.GetQueryContext(ctx); qc != nil {
		if tag := qc.FormatAsJSON(); tag != "" {
			ctx = gosnowflake.WithQueryTag(ctx, tag)
		}
	}
	return ctx
}

func (e *SnowflakeExecutor) QueryRows(ctx context.Context, q string, args ...any) (*sqlx.Rows, error) {
	q = querycontext.AppendSQLComment(ctx, q)
	ctx = e.enrichCtx(ctx)
	return e.db.QueryxContext(ctx, q, args...)
}

func (e *SnowflakeExecutor) Select(ctx context.Context, dest any, query string, args ...any) error {
	query = querycontext.AppendSQLComment(ctx, query)
	ctx = e.enrichCtx(ctx)
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()
	return e.db.SelectContext(ctx, dest, query, args...)
}

// EnrichSnowflakeContext wraps the context with a query ID channel so the Snowflake driver
// reports the query ID. Creates or reuses a DriverStats accumulator and registers an
// onFinish hook to collect the query ID (and optionally detailed stats) when the
// Collector finishes.
func EnrichSnowflakeContext(ctx context.Context, db *sql.DB) context.Context {
	ds, ctx := querystats.GetOrCreateDriverStats(ctx)
	if ds == nil {
		return ctx
	}
	queryIDChan := make(chan string, 1)
	ctx = gosnowflake.WithQueryIDChan(ctx, queryIDChan)
	ds.AddOnFinish(func() {
		select {
		case queryID := <-queryIDChan:
			if queryID == "" {
				return
			}
			ds.Set(querystats.QueryStats{QueryID: queryID})
			if querystats.IsQueryStatsFetch(ctx) {
				fetchSnowflakeQueryStats(ctx, db, ds, queryID)
			}
		default:
		}
	})
	return ctx
}

// fetchSnowflakeQueryStats fetches detailed query statistics from Snowflake's monitoring API.
// This makes an extra HTTP call per query — only called when WithQueryStatsFetch is set.
func fetchSnowflakeQueryStats(ctx context.Context, db *sql.DB, ds *querystats.DriverStats, queryID string) {
	conn, err := db.Conn(ctx)
	if err != nil {
		logging.GetLogger(ctx).Printf("querystats: failed to get Snowflake connection for stats: %v", err)
		return
	}
	defer conn.Close()

	err = conn.Raw(func(driverConn any) error {
		sfConn, ok := driverConn.(gosnowflake.SnowflakeConnection)
		if !ok {
			return nil
		}
		status, err := sfConn.GetQueryStatus(ctx, queryID)
		if err != nil {
			return err
		}
		ds.Set(querystats.QueryStats{
			BytesRead:    querystats.Int64Ptr(status.ScanBytes),
			RowsProduced: querystats.Int64Ptr(status.ProducedRows),
		})
		return nil
	})
	if err != nil {
		logging.GetLogger(ctx).Printf("querystats: failed to fetch Snowflake query status for %s: %v", queryID, err)
	}
}

func (e *SnowflakeExecutor) Exec(ctx context.Context, query string, args ...any) error {
	query = querycontext.AppendSQLComment(ctx, query)
	ctx = e.enrichCtx(ctx)
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()
	_, err := e.db.ExecContext(ctx, query, args...)
	return err
}

func (e *SnowflakeExecutor) Close() error {
	return e.db.Close()
}

// IsNotFoundOrNoPermissionError checks if the error is a Snowflake "does not exist or not authorized" error
func IsNotFoundOrNoPermissionError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "does not exist or not authorized")
}

// IsUnsupportedFeatureDataLineageError checks if the error is a Snowflake "Unsupported feature 'Data Lineage'" error
func IsUnsupportedFeatureDataLineageError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Unsupported feature 'Data Lineage'")
}

type Mock struct {
	SnowflakeExecutor
	queryRowsReturns func() (*sqlx.Rows, error)
}

func (bq *Mock) QueryRows(ctx context.Context, q string, args ...any) (*sqlx.Rows, error) {
	if bq.queryRowsReturns == nil {
		return nil, fmt.Errorf("no return defined in query rows mock")
	}
	return bq.queryRowsReturns()
}
