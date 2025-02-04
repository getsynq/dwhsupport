package postgres

import (
	"github.com/getsynq/dwhsupport/exec/querier"
	"github.com/getsynq/dwhsupport/exec/stdsql"
)

func NewQuerier[T any](conn *PostgresExecutor) querier.Querier[T] {
	return stdsql.NewQuerier[T](conn.db)
}
