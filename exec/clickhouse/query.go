package clickhouse

import (
	"context"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/querier"
	"github.com/getsynq/dwhsupport/exec/stdsql"
)

func NewQuerier[T any](conn *ClickhouseExecutor) querier.Querier[T] {
	return &clickhouseQuerier[T]{
		inner: stdsql.NewQuerier[T](conn.db),
	}
}

// clickhouseQuerier wraps StdSqlQuerier to enrich context with ClickHouse-specific
// stats callbacks before delegating to the standard querier.
type clickhouseQuerier[T any] struct {
	inner *stdsql.StdSqlQuerier[T]
}

func (q *clickhouseQuerier[T]) QueryMany(ctx context.Context, sql string, opts ...exec.QueryManyOpt[T]) ([]*T, error) {
	return q.inner.QueryMany(EnrichClickhouseContext(ctx), sql, opts...)
}

func (q *clickhouseQuerier[T]) QueryAndProcessMany(
	ctx context.Context,
	sql string,
	handler func(ctx context.Context, batch []*T) error,
	opts ...exec.QueryManyOpt[T],
) error {
	return q.inner.QueryAndProcessMany(EnrichClickhouseContext(ctx), sql, handler, opts...)
}

func (q *clickhouseQuerier[T]) QueryMaps(ctx context.Context, sql string) ([]exec.QueryMapResult, error) {
	return q.inner.QueryMaps(EnrichClickhouseContext(ctx), sql)
}

func (q *clickhouseQuerier[T]) Exec(ctx context.Context, sql string) error {
	return q.inner.Exec(ctx, sql)
}

func (q *clickhouseQuerier[T]) Close() error {
	return q.inner.Close()
}
