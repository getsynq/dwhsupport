package stdsql

import (
	"context"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/querier"
	"github.com/jmoiron/sqlx"
)

var _ querier.Querier[interface{}] = (*StdSqlQuerier[interface{}])(nil)

func NewQuerier[T any](conn *sqlx.DB) *StdSqlQuerier[T] {
	return &StdSqlQuerier[T]{conn: conn}
}

type StdSqlQuerier[T any] struct {
	conn *sqlx.DB
}

func (s StdSqlQuerier[T]) QueryMany(ctx context.Context, sql string, opts ...exec.QueryManyOpt[T]) ([]*T, error) {
	return QueryMany(ctx, s.conn, sql, opts...)
}

func (s StdSqlQuerier[T]) QueryAndProcessMany(ctx context.Context, sql string, handler func(ctx context.Context, batch []*T) error, opts ...exec.QueryManyOpt[T]) error {
	return QueryAndProcessMany(ctx, s.conn, sql, handler, opts...)
}

func (s StdSqlQuerier[T]) QueryMaps(ctx context.Context, sql string) ([]exec.QueryMapResult, error) {
	return QueryMaps(ctx, s.conn, sql)
}

func (s StdSqlQuerier[T]) Exec(ctx context.Context, sql string) error {
	return Exec(ctx, s.conn, sql)
}

func (s StdSqlQuerier[T]) Close() error {
	return s.conn.Close()
}
