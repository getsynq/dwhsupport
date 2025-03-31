package redshift

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"

	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/sshtunnel"
	"github.com/lib/pq"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type RedshiftConf struct {
	User      string
	Password  string
	Database  string
	Host      string
	Port      int
	SSHTunnel *sshtunnel.SshTunnel
}

type Executor interface {
	stdsql.StdSqlExecutor
	QueryRows(ctx context.Context, q string, args ...interface{}) (*sqlx.Rows, error)
}

var _ stdsql.StdSqlExecutor = &RedshiftExecutor{}

type RedshiftExecutor struct {
	conf            *RedshiftConf
	db              *sqlx.DB
	sshTunnelDialer *sshtunnel.SshTunnelDialer
}

func (e *RedshiftExecutor) GetDb() *sqlx.DB {
	return e.db
}

func NewRedshiftExecutor(ctx context.Context, conf *RedshiftConf) (*RedshiftExecutor, error) {
	if conf.Port == 0 {
		conf.Port = 5439
	}

	queryStringBuilder := url.URL{}
	queryStringBuilder.User = url.UserPassword(conf.User, conf.Password)
	queryStringBuilder.Host = fmt.Sprintf("%s:%d", conf.Host, conf.Port)
	queryStringBuilder.Path = fmt.Sprintf("/%s", conf.Database)
	queryStringBuilder.Scheme = "postgres"

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

	return &RedshiftExecutor{conf: conf, db: db, sshTunnelDialer: sshTunnelDialer}, nil
}

func (e *RedshiftExecutor) QueryRows(ctx context.Context, sql string, args ...interface{}) (*sqlx.Rows, error) {
	return e.db.QueryxContext(ctx, sql, args...)
}

func (e *RedshiftExecutor) Exec(ctx context.Context, q string) error {
	if _, err := e.db.Exec(q); err != nil {
		return err
	}

	return nil
}

func (e *RedshiftExecutor) Close() error {
	var errs []error
	if e.sshTunnelDialer != nil {
		if err := e.sshTunnelDialer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := e.db.Close(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
