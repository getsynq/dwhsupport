package trino

import (
	"context"
	"fmt"
	"net/url"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	_ "github.com/trinodb/trino-go-client/trino"
)

type TrinoConf struct {
	Host      string
	Port      int
	Plaintext bool
	User      string
	Password  string
	Source    string // optional, e.g. "synq"
}

type Executor interface {
	stdsql.StdSqlExecutor
}

var _ stdsql.StdSqlExecutor = &TrinoExecutor{}

type TrinoExecutor struct {
	conf *TrinoConf
	db   *sqlx.DB
}

func (e *TrinoExecutor) GetDb() *sqlx.DB {
	return e.db
}

func NewTrinoExecutor(ctx context.Context, conf *TrinoConf) (*TrinoExecutor, error) {
	host := conf.Host
	if conf.Port > 0 {
		host = fmt.Sprintf("%s:%d", conf.Host, conf.Port)
	}

	dsn := &url.URL{
		Scheme: lo.Ternary(conf.Plaintext, "http", "https"),
		Host:   host,
		User:   url.UserPassword(conf.User, conf.Password),
	}
	if conf.Source != "" {
		dsn.Query().Set("source", conf.Source)
	}

	db, err := sqlx.Open("trino", dsn.String())
	if err != nil {
		return nil, err
	}
	err = db.PingContext(ctx)
	if err != nil {
		return nil, exec.NewAuthError(err)
	}
	return &TrinoExecutor{conf: conf, db: db}, nil
}

func (e *TrinoExecutor) QueryRows(ctx context.Context, sql string, args ...interface{}) (*sqlx.Rows, error) {
	return e.db.QueryxContext(ctx, sql, args...)
}

func (e *TrinoExecutor) Exec(ctx context.Context, q string) error {
	_, err := e.db.Exec(q)
	return err
}

func (e *TrinoExecutor) Close() error {
	return e.db.Close()
}
