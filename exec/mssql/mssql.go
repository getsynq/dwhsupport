package mssql

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/sshtunnel"
	"github.com/jmoiron/sqlx"

	_ "github.com/denisenkom/go-mssqldb"
)

type MSSQLConf struct {
	User        string
	Password    string
	Host        string
	Port        int
	Database    string
	SSHTunnel   *sshtunnel.SshTunnel
	TrustCert   bool
	Encrypt     string // "disable", "false", "true" (default)
}

var _ stdsql.StdSqlExecutor = &MSSQLExecutor{}

type MSSQLExecutor struct {
	conf            *MSSQLConf
	db              *sqlx.DB
	sshTunnelDialer *sshtunnel.SshTunnelDialer
}

func (e *MSSQLExecutor) GetDb() *sqlx.DB {
	return e.db
}

func NewMSSQLExecutor(ctx context.Context, conf *MSSQLConf) (*MSSQLExecutor, error) {
	if conf.Port == 0 {
		conf.Port = 1433
	}

	query := url.Values{}
	query.Add("database", conf.Database)
	query.Add("app name", "synq.io")
	if conf.TrustCert {
		query.Add("TrustServerCertificate", "true")
	}
	if conf.Encrypt != "" {
		query.Add("encrypt", conf.Encrypt)
	}

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(conf.User, conf.Password),
		Host:     fmt.Sprintf("%s:%d", conf.Host, conf.Port),
		RawQuery: query.Encode(),
	}

	connStr := u.String()

	var err error
	var db *sqlx.DB
	var sshTunnelDialer *sshtunnel.SshTunnelDialer

	if conf.SSHTunnel.IsEnabled() {
		sshTunnelDialer, err = sshtunnel.NewSshTunnelDialer(conf.SSHTunnel)
		if err != nil {
			return nil, err
		}
		db, err = sqlx.Open("sqlserver", connStr)
	} else {
		db, err = sqlx.Open("sqlserver", connStr)
	}

	if err != nil {
		return nil, err
	}

	err = db.PingContext(ctx)
	if err != nil {
		return nil, exec.NewAuthError(err)
	}

	return &MSSQLExecutor{conf: conf, db: db, sshTunnelDialer: sshTunnelDialer}, nil
}

func (e *MSSQLExecutor) QueryRows(ctx context.Context, sql string, args ...interface{}) (*sqlx.Rows, error) {
	return e.db.QueryxContext(ctx, sql, args...)
}

func (e *MSSQLExecutor) Exec(ctx context.Context, q string) error {
	if _, err := e.db.Exec(q); err != nil {
		return err
	}
	return nil
}

func (e *MSSQLExecutor) Close() error {
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
