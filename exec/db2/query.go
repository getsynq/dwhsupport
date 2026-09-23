package db2

import (
	"github.com/getsynq/dwhsupport/exec/querier"
	"github.com/getsynq/dwhsupport/exec/stdsql"
)

func NewQuerier[T any](conn *Db2Executor) querier.Querier[T] {
	return stdsql.NewQuerier[T](conn.db)
}
