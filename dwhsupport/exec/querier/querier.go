package querier

import (
	"context"

	"github.com/getsynq/dwhsupport/exec"
)

type Querier[T any] interface {
	QueryMany(ctx context.Context, sql string, opts ...exec.QueryManyOpt[T]) ([]*T, error)
	QueryAndProcessMany(ctx context.Context, sql string, handler func(ctx context.Context, batch []*T) error, opts ...exec.QueryManyOpt[T]) error
	QueryMaps(ctx context.Context, sql string) ([]exec.QueryMapResult, error)
	Exec(ctx context.Context, sql string) error
	Close() error
}
