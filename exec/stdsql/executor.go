package stdsql

import (
	"context"

	"github.com/jmoiron/sqlx"
)

type StdSqlExecutor interface {
	GetDb() *sqlx.DB
	QueryRows(ctx context.Context, q string, args ...interface{}) (*sqlx.Rows, error)
	Select(ctx context.Context, dest any, query string, args ...any) error
	Exec(ctx context.Context, query string, args ...any) error
}
