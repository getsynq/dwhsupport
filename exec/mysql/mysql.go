package mysql

import (
	"context"
	"fmt"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type MySQLConf struct {
	User          string
	Password      string
	Host          string
	Port          int
	Database      string
	AllowInsecure bool
	Params        map[string]string
}

type Executor interface {
	QueryRows(ctx context.Context, q string, args ...interface{}) (*sqlx.Rows, error)
}

var _ stdsql.StdSqlExecutor = &MySQLExecutor{}

type MySQLExecutor struct {
	conf *MySQLConf
	db   *sqlx.DB
}

func (e *MySQLExecutor) GetDb() *sqlx.DB {
	return e.db
}

func NewMySQLExecutor(ctx context.Context, conf *MySQLConf) (*MySQLExecutor, error) {
	if conf.Port == 0 {
		conf.Port = 3306
	}

	params := map[string]string{}
	// Merge custom params from configuration
	for k, v := range conf.Params {
		params[k] = v
	}

	config := mysql.NewConfig()
	config.User = conf.User
	config.Net = "tcp"
	config.Passwd = conf.Password
	config.Addr = fmt.Sprintf("%s:%d", conf.Host, conf.Port)
	config.DBName = conf.Database
	config.Params = params
	config.ParseTime = true

	// Handle AllowInsecure flag
	if conf.AllowInsecure {
		config.TLSConfig = "false"
	}

	db, err := sqlx.Open("mysql", config.FormatDSN())
	if err != nil {
		return nil, err
	}
	err = db.PingContext(ctx)
	if err != nil {
		return nil, exec.NewAuthError(err)
	}

	return &MySQLExecutor{conf: conf, db: db}, nil
}

func (e *MySQLExecutor) QueryRows(ctx context.Context, sql string, args ...interface{}) (*sqlx.Rows, error) {
	return e.db.QueryxContext(ctx, sql, args...)
}

func (e *MySQLExecutor) Close() error {
	return e.db.Close()
}

type Mock struct {
	MySQLExecutor
	queryRowsReturns func() (*sqlx.Rows, error)
}

func (bq *Mock) returnOnQueryRows(fn func() (*sqlx.Rows, error)) {
	bq.queryRowsReturns = fn
}

func (bq *Mock) QueryRows(ctx context.Context, q string, args ...interface{}) (*sqlx.Rows, error) {
	if bq.queryRowsReturns == nil {
		return nil, fmt.Errorf("no return defined in query rows mock")
	}
	return bq.queryRowsReturns()
}
