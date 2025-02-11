package stdsql

import (
	"context"

	"github.com/jmoiron/sqlx"
)

type StdSqlExecutor interface {
	GetDb() *sqlx.DB
	QueryRows(ctx context.Context, q string, args ...interface{}) (*sqlx.Rows, error)
}
