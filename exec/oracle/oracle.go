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

type OracleConf struct {
	User        string
	Password    string
	Host        string
	Port        int
	ServiceName string

	// SSL enables TLS/SSL encryption (TCPS protocol, typically port 2484).
	SSL bool
	// SSLVerify enables server certificate verification when SSL is true.
	SSLVerify bool

	// WalletPath is the path to an Oracle Wallet directory for mTLS authentication.
	// When set, the wallet is used for both TLS certificates and (optionally) stored credentials.
	// Common with Oracle Cloud (OCI) Autonomous Database which requires mTLS by default.
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
