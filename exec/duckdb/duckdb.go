package duckdb

import (
	"context"
	"fmt"
	"net/url"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/jmoiron/sqlx"
)

type DuckDBConf struct {
	MotherduckAccount string
	MotherduckToken   string
}

type Executor interface {
	QueryRows(ctx context.Context, q string, args ...interface{}) (*sqlx.Rows, error)
}

var _ stdsql.StdSqlExecutor = &DuckDBExecutor{}

type DuckDBExecutor struct {
	conf *DuckDBConf
	db   *sqlx.DB
}

func (e *DuckDBExecutor) GetDb() *sqlx.DB {
	return e.db
}

func NewDuckDBExecutor(ctx context.Context, conf *DuckDBConf) (*DuckDBExecutor, error) {

	dsn := fmt.Sprintf("md:%s?motherduck_token=%s", conf.MotherduckAccount, url.QueryEscape(conf.MotherduckToken))

	db, err := sqlx.Open("duckdb", dsn)

	if err != nil {
		return nil, err
	}
	err = db.PingContext(ctx)
	if err != nil {
		return nil, exec.NewAuthError(err)
	}

	return &DuckDBExecutor{conf: conf, db: db}, nil
}

func (e *DuckDBExecutor) QueryRows(ctx context.Context, sql string, args ...interface{}) (*sqlx.Rows, error) {
	return e.db.QueryxContext(ctx, sql, args...)
}

func (e *DuckDBExecutor) Exec(ctx context.Context, q string) error {
	if _, err := e.db.Exec(q); err != nil {
		return err
	}

	return nil
}

func (e *DuckDBExecutor) Close() error {
	return e.db.Close()
}
