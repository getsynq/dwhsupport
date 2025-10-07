package bigquery

import (
	"context"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/logging"
	"google.golang.org/api/iterator"
)

func QueryMany[T any](ctx context.Context, conn Executor, sql string, opts ...dwhexec.QueryManyOpt[T]) ([]*T, error) {
	q := dwhexec.NewQueryMany[T](sql)
	for _, opt := range opts {
		opt(q)
	}

	rows, err := conn.queryRows(ctx, q.Sql, q.Args...)
	if err != nil {
		return nil, err
	}
	counter := 0
	results := make([]*T, 0)
	for {
		counter++

		var result T
		err = rows.Next(&result)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		results = append(results, &result)
	}

	return results, nil
}

func QueryAndProcessMany[T any](
	ctx context.Context,
	conn Executor,
	sql string,
	handler func(ctx context.Context, rows []*T) error,
	opts ...dwhexec.QueryManyOpt[T],
) error {
	q := dwhexec.NewQueryMany[T](sql)
	for _, opt := range opts {
		opt(q)
	}

	rows, err := conn.queryRows(ctx, q.Sql, q.Args...)
	if err != nil {
		return err
	}
	counter := 0
	results := make([]*T, 0, 1000)
	for {
		counter++

		var result T
		err = rows.Next(&result)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		results = append(results, &result)

		if len(results) >= 1000 {
			// Process the page of rows here
			err = handler(ctx, results)
			if err != nil {
				return err
			}
			// clear the slice
			results = results[:0]
		}
	}

	// Process the last page of rows here
	err = handler(ctx, results)
	if err != nil {
		return err
	}

	logging.GetLogger(ctx).Infof("bigquery query logs processed: %d", counter)

	return nil
}
