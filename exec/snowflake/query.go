package snowflake

import (
	"context"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/querier"
	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/snowflakedb/gosnowflake"
)

func NewQuerier[T any](conn *SnowflakeExecutor) querier.Querier[T] {
	return &snowflakeQuerier[T]{
		inner: stdsql.NewQuerier[T](conn.db),
		exec:  conn,
	}
}

// snowflakeQuerier wraps StdSqlQuerier to enrich context with Snowflake-specific
// query ID channel and optional stats fetching.
type snowflakeQuerier[T any] struct {
	inner *stdsql.StdSqlQuerier[T]
	exec  *SnowflakeExecutor
}

func (q *snowflakeQuerier[T]) enrichCtx(ctx context.Context) context.Context {
	ctx = EnrichSnowflakeContext(ctx, q.exec.db.DB)
	if qc := querycontext.GetQueryContext(ctx); qc != nil {
		if tag := qc.FormatAsJSON(); tag != "" {
			ctx = gosnowflake.WithQueryTag(ctx, tag)
		}
	}
	return ctx
}

func (q *snowflakeQuerier[T]) QueryMany(ctx context.Context, sql string, opts ...exec.QueryManyOpt[T]) ([]*T, error) {
	return q.inner.QueryMany(q.enrichCtx(ctx), sql, opts...)
}

func (q *snowflakeQuerier[T]) QueryAndProcessMany(
	ctx context.Context,
	sql string,
	handler func(ctx context.Context, batch []*T) error,
	opts ...exec.QueryManyOpt[T],
) error {
	return q.inner.QueryAndProcessMany(q.enrichCtx(ctx), sql, handler, opts...)
}

func (q *snowflakeQuerier[T]) QueryMaps(ctx context.Context, sql string) ([]exec.QueryMapResult, error) {
	return q.inner.QueryMaps(q.enrichCtx(ctx), sql)
}

func (q *snowflakeQuerier[T]) Exec(ctx context.Context, sql string) error {
	return q.inner.Exec(ctx, sql)
}

func (q *snowflakeQuerier[T]) Close() error {
	return q.inner.Close()
}
