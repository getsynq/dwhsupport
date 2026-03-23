package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/jmoiron/sqlx"

	mssqldb "github.com/microsoft/go-mssqldb"
	// Register Azure AD authentication driver alongside the standard "sqlserver" driver.
	_ "github.com/microsoft/go-mssqldb/azuread"
)

type MSSQLConf struct {
	User     string
	Password string
	Host     string
	Port     int
	Database string

	// TrustCert skips TLS certificate verification.
	TrustCert bool
	// Encrypt controls connection encryption: "disable", "false", "true" (default).
	Encrypt string

	// FedAuth specifies the Azure AD authentication method.
	// When set, the azuread driver is used instead of the standard sqlserver driver.
	// Supported values: ActiveDirectoryDefault, ActiveDirectoryPassword,
	// ActiveDirectoryMSI, ActiveDirectoryServicePrincipal, ActiveDirectoryAzCli, etc.
	// See https://pkg.go.dev/github.com/microsoft/go-mssqldb/azuread
	FedAuth string

	// AccessToken is a pre-acquired Azure AD access token.
	// When set, it takes precedence over all other authentication methods.
	AccessToken string

	// ApplicationClientID is the Azure AD application (client) ID.
	// Used with ActiveDirectoryServicePrincipal and ActiveDirectoryMSI (user-assigned).
	ApplicationClientID string
}

var _ stdsql.StdSqlExecutor = &MSSQLExecutor{}

type MSSQLExecutor struct {
	conf *MSSQLConf
	db   *sqlx.DB
}

func (e *MSSQLExecutor) GetDb() *sqlx.DB {
	return e.db
}

func NewMSSQLExecutor(ctx context.Context, conf *MSSQLConf) (*MSSQLExecutor, error) {
	if conf.Port == 0 {
		conf.Port = 1433
	}

	var db *sqlx.DB

	switch {
	case conf.AccessToken != "":
		// Token-based auth: use the access token connector directly.
		connector, err := mssqldb.NewAccessTokenConnector(
			buildConnectionString(conf),
			func() (string, error) { return conf.AccessToken, nil },
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create access token connector: %w", err)
		}
		db = sqlx.NewDb(sql.OpenDB(connector), "sqlserver")

	case conf.FedAuth != "":
		// Azure AD federated auth: use the azuread driver.
		connStr := buildConnectionString(conf)
		var err error
		db, err = sqlx.Open("azuresql", connStr)
		if err != nil {
			return nil, err
		}

	default:
		// Standard SQL Server authentication: username/password.
		connStr := buildConnectionString(conf)
		var err error
		db, err = sqlx.Open("sqlserver", connStr)
		if err != nil {
			return nil, err
		}
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, exec.NewAuthError(err)
	}

	return &MSSQLExecutor{conf: conf, db: db}, nil
}

func buildConnectionString(conf *MSSQLConf) string {
	query := url.Values{}
	query.Add("database", conf.Database)
	query.Add("app name", "synq.io")
	if conf.TrustCert {
		query.Add("TrustServerCertificate", "true")
	}
	if conf.Encrypt != "" {
		query.Add("encrypt", conf.Encrypt)
	}
	if conf.FedAuth != "" {
		query.Add("fedauth", conf.FedAuth)
	}
	if conf.ApplicationClientID != "" {
		query.Add("applicationclientid", conf.ApplicationClientID)
	}

	u := &url.URL{
		Scheme:   "sqlserver",
		Host:     fmt.Sprintf("%s:%d", conf.Host, conf.Port),
		RawQuery: query.Encode(),
	}

	// Only set user info for password-based auth.
	if conf.AccessToken == "" && conf.User != "" {
		u.User = url.UserPassword(conf.User, conf.Password)
	}

	return u.String()
}

func (e *MSSQLExecutor) QueryRows(ctx context.Context, sql string, args ...interface{}) (*sqlx.Rows, error) {
	sql = querycontext.AppendSQLComment(ctx, sql)
	return e.db.QueryxContext(ctx, sql, args...)
}

func (e *MSSQLExecutor) Select(ctx context.Context, dest any, query string, args ...any) error {
	query = querycontext.AppendSQLComment(ctx, query)
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()
	return e.db.SelectContext(ctx, dest, query, args...)
}

func (e *MSSQLExecutor) Exec(ctx context.Context, query string, args ...any) error {
	query = querycontext.AppendSQLComment(ctx, query)
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()
	_, err := e.db.ExecContext(ctx, query, args...)
	return err
}

func (e *MSSQLExecutor) Close() error {
	return e.db.Close()
}
