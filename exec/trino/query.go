package trino

import (
	"github.com/getsynq/dwhsupport/exec/querier"
	"github.com/getsynq/dwhsupport/exec/stdsql"
)

func NewQuerier[T any](conn *TrinoExecutor) querier.Querier[T] {
	return stdsql.NewQuerier[T](conn.db)
}
