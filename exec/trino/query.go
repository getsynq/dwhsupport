package trino

import (
	"context"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/querier"
	"github.com/getsynq/dwhsupport/exec/stdsql"
)

type Querier[T any] struct {
	std *stdsql.StdSqlQuerier[T]
}

var _ querier.Querier[any] = &Querier[any]{}

func NewQuerier[T any](conn *TrinoExecutor) querier.Querier[T] {
	return &Querier[T]{std: stdsql.NewQuerier[T](conn.db)}
}

func (q *Querier[T]) Close() error {
	return q.std.Close()
}

func (q *Querier[T]) QueryMany(ctx context.Context, sql string, opts ...exec.QueryManyOpt[T]) ([]*T, error) {
	return q.std.QueryMany(ctx, trimRightSemicolons(sql), opts...)
}

func (q *Querier[T]) QueryAndProcessMany(
	ctx context.Context,
	sql string,
	handler func(ctx context.Context, batch []*T) error,
	opts ...exec.QueryManyOpt[T],
) error {
	return q.std.QueryAndProcessMany(ctx, trimRightSemicolons(sql), handler, opts...)
}

func (q *Querier[T]) QueryMaps(ctx context.Context, sql string) ([]exec.QueryMapResult, error) {
	return q.std.QueryMaps(ctx, trimRightSemicolons(sql))
}

func (q *Querier[T]) Exec(ctx context.Context, sql string) error {
	return q.std.Exec(ctx, trimRightSemicolons(sql))
}
