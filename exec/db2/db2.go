package db2

import (
	"context"
	"database/sql"
	"strings"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	godb2 "github.com/getsynq/go-db2"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// Db2Conf configures a connection to IBM Db2 for Linux, UNIX and Windows.
//
// The fields follow the keywords of a Db2 CLI/ODBC connection string, so a
// DBA can fill them from what they already use, e.g.
// `HOSTNAME=db2.example.com;PORT=50000;DATABASE=SAMPLE;UID=reader;PWD=...;SECURITY=SSL`.
// The driver speaks DRDA itself (github.com/getsynq/go-db2): no IBM client,
// clidriver or db2dsdriver.cfg is involved.
type Db2Conf struct {
	// Hostname is the Db2 server (HOSTNAME).
	Hostname string
	// Port is the TCP/IP port of the instance (PORT, the SVCENAME of the
	// instance). Default: 50000, or 50001 when Security is SSL.
	Port int
	// Database is the database name or alias to connect to (DATABASE).
	Database string
	// User is the authorization ID (UID). Db2 authenticates it against the
	// server's operating system or LDAP, not against a user stored in the
	// database.
	User string
	// Password is the password of User (PWD).
	Password string
	// Security is "SSL" to connect over TLS (SECURITY=SSL); empty for a plain
	// TCP/IP connection.
	Security string
	// SSLServerCertificate is the path to a PEM file holding the server's
	// certificate or the CA that signed it (SSLServerCertificate). Empty uses
	// the system trust store.
	SSLServerCertificate string
	// Authentication selects how the password is sent (AUTHENTICATION):
	// "SERVER" (default) sends it in the clear, so pair it with SSL;
	// "SERVER_ENCRYPT" encrypts user ID and password with a Diffie-Hellman
	// key (DRDA security mechanism 9) and needs the instance's AUTHENTICATION
	// to allow it.
	Authentication string
}

// Authentication values.
const (
	AuthenticationServer        = "SERVER"
	AuthenticationServerEncrypt = "SERVER_ENCRYPT"
)

var _ stdsql.StdSqlExecutor = &Db2Executor{}

type Db2Executor struct {
	conf *Db2Conf
	db   *sqlx.DB
}

func (e *Db2Executor) GetDb() *sqlx.DB {
	return e.db
}

func driverConfig(conf *Db2Conf) (*godb2.Config, error) {
	cfg := godb2.NewConfig()
	cfg.Host = conf.Hostname
	cfg.Port = conf.Port
	cfg.Database = conf.Database
	cfg.User = conf.User
	cfg.Password = conf.Password
	cfg.ClientApplName = "synq.io"

	switch strings.ToUpper(conf.Security) {
	case "":
	case "SSL":
		cfg.UseSSL = true
		cfg.SSLRootCAPath = conf.SSLServerCertificate
	default:
		return nil, errors.Errorf("db2: unsupported Security %q, expected SSL or empty", conf.Security)
	}
	if cfg.Port == 0 {
		cfg.Port = 50000
		if cfg.UseSSL {
			cfg.Port = 50001
		}
	}

	switch strings.ToUpper(conf.Authentication) {
	case "", AuthenticationServer:
		cfg.SecurityMechanism = 3 // USRIDPWD
	case AuthenticationServerEncrypt:
		cfg.SecurityMechanism = 9 // EUSRIDPWD
	default:
		return nil, errors.Errorf("db2: unsupported Authentication %q, expected SERVER or SERVER_ENCRYPT", conf.Authentication)
	}
	return cfg, nil
}

func NewDb2Executor(ctx context.Context, conf *Db2Conf) (*Db2Executor, error) {
	cfg, err := driverConfig(conf)
	if err != nil {
		return nil, err
	}

	db := sqlx.NewDb(sql.OpenDB(godb2.NewConnector(cfg)), "db2")
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, exec.NewAuthError(err)
	}

	return &Db2Executor{conf: conf, db: db}, nil
}

func (e *Db2Executor) QueryRows(ctx context.Context, sql string, args ...interface{}) (*sqlx.Rows, error) {
	sql = querycontext.AppendSQLComment(ctx, sql)
	return e.db.QueryxContext(ctx, sql, args...)
}

func (e *Db2Executor) Select(ctx context.Context, dest any, query string, args ...any) error {
	query = querycontext.AppendSQLComment(ctx, query)
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()
	return e.db.SelectContext(ctx, dest, query, args...)
}

func (e *Db2Executor) Exec(ctx context.Context, query string, args ...any) error {
	query = querycontext.AppendSQLComment(ctx, query)
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()
	_, err := e.db.ExecContext(ctx, query, args...)
	return err
}

func (e *Db2Executor) Close() error {
	return e.db.Close()
}
