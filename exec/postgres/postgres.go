package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"

	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/sshtunnel"
	"github.com/lib/pq"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type PostgresConf struct {
	User          string
	Password      string
	Database      string
	Host          string
	Port          int
	AllowInsecure bool
	SSHTunnel     *sshtunnel.SshTunnel
}

type Executor interface {
	QueryRows(ctx context.Context, q string, args ...interface{}) (*sqlx.Rows, error)
}

var _ stdsql.StdSqlExecutor = &PostgresExecutor{}

type PostgresExecutor struct {
	conf            *PostgresConf
	db              *sqlx.DB
	sshTunnelDialer *sshtunnel.SshTunnelDialer
}

func (e *PostgresExecutor) GetDb() *sqlx.DB {
	return e.db
}

func NewPostgresExecutor(ctx context.Context, conf *PostgresConf) (*PostgresExecutor, error) {
	if conf.Port == 0 {
		conf.Port = 5439
	}

	queryStringBuilder := url.URL{}
	queryStringBuilder.User = url.UserPassword(conf.User, conf.Password)
	queryStringBuilder.Host = fmt.Sprintf("%s:%d", conf.Host, conf.Port)
	queryStringBuilder.Path = fmt.Sprintf("/%s", conf.Database)
	queryStringBuilder.Scheme = "postgres"
	q := queryStringBuilder.Query()
	q.Set("application_name", "synq.io")
	if conf.AllowInsecure {
		q.Set("sslmode", "disable")
	}
	queryStringBuilder.RawQuery = q.Encode()

	queryString := queryStringBuilder.String()

	var err error
	var db *sqlx.DB
	var sshTunnelDialer *sshtunnel.SshTunnelDialer

	if conf.SSHTunnel.IsEnabled() {
		logging.GetLogger(ctx).Infof("using ssh tunnel to connect to %s", conf.Host)
		sshTunnelDialer, err = sshtunnel.NewSshTunnelDialer(conf.SSHTunnel)
		if err != nil {
			return nil, err
		}
		connector, err := pq.NewConnector(queryString)
		if err != nil {
			return nil, err
		}
		connector.Dialer(sshTunnelDialer)

		stdDb := sql.OpenDB(connector)
		db = sqlx.NewDb(stdDb, "postgres")
	} else {
		db, err = sqlx.Open("postgres", queryString)
	}

	if err != nil {
		return nil, err
	}
	err = db.PingContext(ctx)
	if err != nil {
		return nil, exec.NewAuthError(err)
	}

	return &PostgresExecutor{conf: conf, db: db, sshTunnelDialer: sshTunnelDialer}, nil
}

func (e *PostgresExecutor) QueryRows(ctx context.Context, sql string, args ...interface{}) (*sqlx.Rows, error) {
	sql = querycontext.AppendSQLComment(ctx, sql)
	return e.db.QueryxContext(ctx, sql, args...)
}

func (e *PostgresExecutor) Select(ctx context.Context, dest any, query string, args ...any) error {
	query = querycontext.AppendSQLComment(ctx, query)
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()
	return e.db.SelectContext(ctx, dest, query, args...)
}

func (e *PostgresExecutor) Exec(ctx context.Context, query string, args ...any) error {
	query = querycontext.AppendSQLComment(ctx, query)
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()
	_, err := e.db.ExecContext(ctx, query, args...)
	return err
}

func (e *PostgresExecutor) Close() error {
	var errs []error
	if err := e.db.Close(); err != nil {
		errs = append(errs, err)
	}

	if e.sshTunnelDialer != nil {
		if err := e.sshTunnelDialer.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
