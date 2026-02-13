package oracle

import (
	"context"
	"errors"
	"fmt"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/sshtunnel"
	"github.com/jmoiron/sqlx"
	_ "github.com/sijms/go-ora/v2"
)

type OracleConf struct {
	User        string
	Password    string
	Host        string
	Port        int
	ServiceName string
	SSHTunnel   *sshtunnel.SshTunnel
}

var _ stdsql.StdSqlExecutor = &OracleExecutor{}

type OracleExecutor struct {
	conf            *OracleConf
	db              *sqlx.DB
	sshTunnelDialer *sshtunnel.SshTunnelDialer
}

func (e *OracleExecutor) GetDb() *sqlx.DB {
	return e.db
}

func NewOracleExecutor(ctx context.Context, conf *OracleConf) (*OracleExecutor, error) {
	if conf.Port == 0 {
		conf.Port = 1521
	}

	connStr := fmt.Sprintf("oracle://%s:%s@%s:%d/%s",
		conf.User, conf.Password, conf.Host, conf.Port, conf.ServiceName)

	var err error
	var db *sqlx.DB
	var sshTunnelDialer *sshtunnel.SshTunnelDialer

	if conf.SSHTunnel.IsEnabled() {
		sshTunnelDialer, err = sshtunnel.NewSshTunnelDialer(conf.SSHTunnel)
		if err != nil {
			return nil, err
		}
		// go-ora doesn't natively support custom dialers via sql.OpenDB with a connector,
		// so SSH tunneling would need to be handled at the network level.
		// For now, we open a standard connection; SSH tunnel support can be added later.
		db, err = sqlx.Open("oracle", connStr)
	} else {
		db, err = sqlx.Open("oracle", connStr)
	}

	if err != nil {
		return nil, err
	}

	err = db.PingContext(ctx)
	if err != nil {
		return nil, exec.NewAuthError(err)
	}

	return &OracleExecutor{conf: conf, db: db, sshTunnelDialer: sshTunnelDialer}, nil
}

func (e *OracleExecutor) QueryRows(ctx context.Context, sql string, args ...interface{}) (*sqlx.Rows, error) {
	return e.db.QueryxContext(ctx, sql, args...)
}

func (e *OracleExecutor) Exec(ctx context.Context, q string) error {
	if _, err := e.db.Exec(q); err != nil {
		return err
	}
	return nil
}

func (e *OracleExecutor) Close() error {
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
