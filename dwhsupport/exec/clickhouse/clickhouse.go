package clickhouse

import (
	"context"
	"crypto/tls"
	_ "embed"
	"time"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/jmoiron/sqlx"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type ClickhouseConf struct {
	Host            string
	Username        string
	Password        string
	DefaultDatabase string
	NoSsl           bool
}

var _ stdsql.StdSqlExecutor = &ClickhouseExecutor{}

type ClickhouseExecutor struct {
	conf *ClickhouseConf
	db   *sqlx.DB
}

func (e *ClickhouseExecutor) GetDb() *sqlx.DB {
	return e.db
}

func (e *ClickhouseExecutor) Close() error {
	return e.db.Close()
}

func NewClickhouseExecutor(ctx context.Context, conf *ClickhouseConf) (*ClickhouseExecutor, error) {

	clickhouseOptions := &clickhouse.Options{
		Protocol:    clickhouse.Native,
		DialTimeout: 30 * time.Second,
		Addr:        []string{conf.Host},
		Auth: clickhouse.Auth{
			Username: conf.Username,
			Password: conf.Password,
		},

		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},

		ConnOpenStrategy: clickhouse.ConnOpenRoundRobin,
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
			"max_query_size":     10000000,
		},
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "synq-clickhouse-client", Version: "1.0"},
			},
		},
	}

	if !conf.NoSsl {
		clickhouseOptions.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	db := clickhouse.OpenDB(clickhouseOptions)
	err := db.PingContext(ctx)
	if err != nil {
		return nil, exec.NewAuthError(err)
	}

	return &ClickhouseExecutor{db: sqlx.NewDb(db, "clickhouse"), conf: conf}, nil
}

func (e *ClickhouseExecutor) QueryRows(ctx context.Context, q string, args ...interface{}) (*sqlx.Rows, error) {
	rows, err := e.db.QueryxContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (e *ClickhouseExecutor) Exec(ctx context.Context, q string) error {
	_, err := e.db.ExecContext(ctx, q)
	return err
}

func (e *ClickhouseExecutor) QueryRow(ctx context.Context, sql string, args ...interface{}) *sqlx.Row {
	return e.db.QueryRowxContext(ctx, sql, args...)
}
