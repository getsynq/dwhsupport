package stdsql

import (
	"context"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/jmoiron/sqlx"
)

func QueryAndProcessMany[T any](ctx context.Context, conn *sqlx.DB, sql string, handler func(ctx context.Context, rows []*T) error, opts ...exec.QueryManyOpt[T]) error {

	queryMany := exec.NewQueryMany[T](sql)
	for _, opt := range opts {
		opt(queryMany)
	}

	rows, err := conn.QueryxContext(ctx, queryMany.Sql, queryMany.Args...)
	if err != nil {
		return err
	}

	counter := 0
	results := make([]*T, 0)
	for rows.Next() {
		counter++
		err = rows.Err()
		if err != nil {
			return err
		}

		var result T
		processed := &result
		if err := rows.StructScan(&result); err != nil {
			return err
		}

		for _, processor := range queryMany.Postprocessors {
			if processed != nil {
				processed, err = processor(processed)
				if err != nil {
					return err
				}
			}
		}

		if processed != nil {
			results = append(results, processed)
		}

		if len(results) >= 1000 {
			// Process the page of rows here
			err = handler(ctx, results)

			if err != nil {
				return err
			}
			// clear the slice
			results = nil
		}
	}

	err = rows.Err()
	if err != nil {
		return err
	}

	// Deal with the rest of the rows
	if len(results) > 0 {
		err = handler(ctx, results)
		if err != nil {
			return err
		}
	}

	return nil
}

func QueryMany[T any](ctx context.Context, conn *sqlx.DB, sql string, opts ...exec.QueryManyOpt[T]) ([]*T, error) {
	queryMany := exec.NewQueryMany[T](sql)
	for _, opt := range opts {
		opt(queryMany)
	}
	rows, err := conn.QueryxContext(ctx, queryMany.Sql, queryMany.Args...)
	if err != nil {
		return nil, err
	}

	results := make([]*T, 0)
	for rows.Next() {
		result := *new(T)
		processed := &result

		if err := rows.StructScan(&result); err != nil {
			return nil, err
		}

		for _, processor := range queryMany.Postprocessors {
			if processed != nil {
				processed, err = processor(processed)
				if err != nil {
					return nil, err
				}
			}
		}

		if processed != nil {
			results = append(results, processed)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func QueryMaps(ctx context.Context, conn *sqlx.DB, sql string) ([]exec.QueryMapResult, error) {
	rows, err := conn.QueryxContext(ctx, sql)
	if err != nil {
		return nil, err
	}

	results := make([]exec.QueryMapResult, 0)
	for rows.Next() {
		result := make(exec.QueryMapResult)

		if err := rows.MapScan(result); err != nil {
			return nil, err
		}

		results = append(results, result)
	}

	return results, nil
}

func Exec(ctx context.Context, db *sqlx.DB, sql string) error {
	_, err := db.ExecContext(ctx, sql)
	return err
}
