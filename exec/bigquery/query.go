package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/exec"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
)

func NewQuerier[T any](conn *BigQueryExecutor) *Querier[T] {
	return &Querier[T]{conn: conn}
}

type Querier[T any] struct {
	conn *BigQueryExecutor
}

func (q *Querier[T]) Close() error {
	return q.conn.Close()
}

func (q *Querier[T]) QueryMaps(ctx context.Context, sql string) ([]exec.QueryMapResult, error) {
	it, err := q.conn.QueryRowsIterator(ctx, sql)
	if err != nil {
		return nil, err
	}

	results := make([]exec.QueryMapResult, 0)
	for {
		var row map[string]bigquery.Value
		result := make(exec.QueryMapResult)

		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		for k, v := range row {
			result[k] = v
		}

		results = append(results, result)
	}

	return results, nil
}

func (q *Querier[T]) Exec(ctx context.Context, sql string) error {
	return q.conn.Exec(ctx, sql)
}

func (q *Querier[T]) QueryMany(
	ctx context.Context,
	sql string,
	opts ...exec.QueryManyOpt[T],
) ([]*T, error) {
	qq := exec.NewQueryMany[T](sql)
	for _, opt := range opts {
		opt(qq)
	}

	rows, err := q.conn.QueryRowsIterator(ctx, qq.Sql, qq.Args...)
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

func (q *Querier[T]) QueryAndProcessMany(
	ctx context.Context,
	sql string,
	handler func(ctx context.Context, batch []*T) error,
	opts ...exec.QueryManyOpt[T],
) error {
	qq := exec.NewQueryMany[T](sql)
	for _, opt := range opts {
		opt(qq)
	}

	rows, err := q.conn.QueryRowsIterator(ctx, qq.Sql, qq.Args...)
	if err != nil {
		return err
	}
	results := make([]*T, 0)
	for {
		var result T
		err = rows.Next(&result)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return err
		}

		results = append(results, &result)
		if len(results) >= qq.ProcessBatchSize {
			err = handler(ctx, results)
			if err != nil {
				return err
			}
			results = nil
		}
	}
	if len(results) > 0 {
		err = handler(ctx, results)
		if err != nil {
			return err
		}
	}

	return nil
}
