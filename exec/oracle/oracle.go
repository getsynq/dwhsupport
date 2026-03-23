package oracle

import (
	"context"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/jmoiron/sqlx"
	go_ora "github.com/sijms/go-ora/v2"
)

// OracleConf configures a connection to an Oracle Database.
//
// Authentication methods (in order of priority):
//  1. Oracle Wallet (mTLS): set WalletPath to a wallet directory. Used by OCI Autonomous Database.
//     The wallet provides TLS certificates and can store credentials.
//  2. Username/Password: set User and Password for standard Oracle database authentication.
//
// For OCI Autonomous Database, SSL is required and WalletPath should point to
// the downloaded wallet directory from the OCI console.
type OracleConf struct {
	// User is the Oracle database username. Leave empty when using wallet-stored credentials.
	User string
	// Password is the Oracle database password. Leave empty when using wallet-stored credentials.
	Password string
	// Host is the hostname or IP address of the Oracle instance.
	// For OCI: e.g. "adb.eu-frankfurt-1.oraclecloud.com"
	Host string
	// Port is the Oracle listener port. Default: 1521. OCI Autonomous DB with mTLS typically uses 1522.
	Port int
	// ServiceName is the Oracle service name (PDB name).
	// For OCI Autonomous DB: e.g. "mydb_high", "mydb_low", "mydb_tp"
	ServiceName string

	// SSL enables TLS/SSL encryption (TCPS protocol).
	// Required for OCI Autonomous Database. When WalletPath is set, SSL is enabled automatically.
	SSL bool
	// SSLVerify enables server certificate verification when SSL is true.
	// Set to false for self-signed certificates or when using a wallet that handles trust.
	SSLVerify bool

	// WalletPath is the path to an Oracle Wallet directory for mTLS authentication.
	// Download from OCI console → Database connection → Download wallet.
	// The wallet contains TLS certificates (cwallet.sso) and optionally stored credentials.
	// When set, SSL is enabled automatically.
	WalletPath string
}

var _ stdsql.StdSqlExecutor = &OracleExecutor{}

type OracleExecutor struct {
	conf *OracleConf
	db   *sqlx.DB
}

func (e *OracleExecutor) GetDb() *sqlx.DB {
	return e.db
}

func NewOracleExecutor(ctx context.Context, conf *OracleConf) (*OracleExecutor, error) {
	if conf.Port == 0 {
		conf.Port = 1521
	}

	urlOptions := map[string]string{}

	if conf.SSL {
		urlOptions["ssl"] = "true"
		if conf.SSLVerify {
			urlOptions["ssl verify"] = "true"
		} else {
			urlOptions["ssl verify"] = "false"
		}
	}

	if conf.WalletPath != "" {
		urlOptions["wallet"] = conf.WalletPath
		// Wallet connections use TCPS (SSL) by default.
		if !conf.SSL {
			urlOptions["ssl"] = "true"
			urlOptions["ssl verify"] = "false"
		}
	}

	connStr := go_ora.BuildUrl(conf.Host, conf.Port, conf.ServiceName, conf.User, conf.Password, urlOptions)

	db, err := sqlx.Open("oracle", connStr)
	if err != nil {
		return nil, err
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, exec.NewAuthError(err)
	}

	return &OracleExecutor{conf: conf, db: db}, nil
}

func (e *OracleExecutor) QueryRows(ctx context.Context, sql string, args ...interface{}) (*sqlx.Rows, error) {
	sql = querycontext.AppendSQLComment(ctx, sql)
	return e.db.QueryxContext(ctx, sql, args...)
}

func (e *OracleExecutor) Select(ctx context.Context, dest any, query string, args ...any) error {
	query = querycontext.AppendSQLComment(ctx, query)
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()
	return e.db.SelectContext(ctx, dest, query, args...)
}

func (e *OracleExecutor) Exec(ctx context.Context, query string, args ...any) error {
	query = querycontext.AppendSQLComment(ctx, query)
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()
	_, err := e.db.ExecContext(ctx, query, args...)
	return err
}

func (e *OracleExecutor) Close() error {
	return e.db.Close()
}
