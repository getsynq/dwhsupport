package stdsql

import (
	"context"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/rowscan"
	"github.com/jmoiron/sqlx"
)

// rowScan plans the column-to-field mapping the first time a row arrives, and
// reuses it for the rest of the result set.
//
// The plan comes from rowscan rather than sqlx's StructScan because StructScan
// is all-or-nothing in one direction: a single result column no `db` tag claims
// fails every row with "missing destination name X in *T". Two things routinely
// produce such a column and neither is ours to control. An account configured to
// fold quoted identifiers to upper case returns every alias in a case the query
// did not ask for — and every alias our SQL asks for is quoted — so nothing
// matches at all. And a warehouse-managed view gains a column on a vendor
// release, which would turn a read into a total outage rather than a missing
// field. See rowscan for what the scan does with each.
//
// Planning is deferred to the first row so that a result set that yields none
// stays the non-error it has always been, whatever type the caller named:
// a querier is prepared with whatever type opens the connection, including a
// scalar one that never scans a struct.
type rowScan[T any] struct {
	scanner *rowscan.Scanner[T]
}

func (r *rowScan[T]) scan(ctx context.Context, rows *sqlx.Rows, dest *T) error {
	if r.scanner == nil {
		scanner, err := rowscan.New[T](rows)
		if err != nil {
			return err
		}
		scanner.LogColumnDrift(ctx, "")
		r.scanner = scanner
	}
	return r.scanner.Scan(rows, dest)
}

func QueryAndProcessMany[T any](
	ctx context.Context,
	conn *sqlx.DB,
	sql string,
	handler func(ctx context.Context, rows []*T) error,
	opts ...exec.QueryManyOpt[T],
) error {

	queryMany := exec.NewQueryMany[T](sql)
	for _, opt := range opts {
		opt(queryMany)
	}
	queryMany.Sql = querycontext.AppendSQLComment(ctx, queryMany.Sql)

	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()
	var rowCount int64

	rows, err := conn.QueryxContext(ctx, queryMany.Sql, queryMany.Args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	var scan rowScan[T]
	results := make([]*T, 0, queryMany.ProcessBatchSize)
	for rows.Next() {
		rowCount++
		err = rows.Err()
		if err != nil {
			collector.SetRowsProduced(rowCount)
			return err
		}

		var result T
		processed := &result
		if err := scan.scan(ctx, rows, &result); err != nil {
			collector.SetRowsProduced(rowCount)
			return err
		}

		for _, processor := range queryMany.Postprocessors {
			if processed != nil {
				processed, err = processor(processed)
				if err != nil {
					collector.SetRowsProduced(rowCount)
					return err
				}
			}
		}

		if processed != nil {
			results = append(results, processed)
		}

		if len(results) >= queryMany.ProcessBatchSize {
			err = handler(ctx, results)
			if err != nil {
				collector.SetRowsProduced(rowCount)
				return err
			}
			results = results[:0]
		}
	}

	err = rows.Err()
	if err != nil {
		collector.SetRowsProduced(rowCount)
		return err
	}

	// Deal with the rest of the rows
	if len(results) > 0 {
		err = handler(ctx, results)
		if err != nil {
			collector.SetRowsProduced(rowCount)
			return err
		}
	}

	collector.SetRowsProduced(rowCount)
	return nil
}

func QueryMany[T any](ctx context.Context, conn *sqlx.DB, sql string, opts ...exec.QueryManyOpt[T]) ([]*T, error) {
	queryMany := exec.NewQueryMany[T](sql)
	for _, opt := range opts {
		opt(queryMany)
	}
	queryMany.Sql = querycontext.AppendSQLComment(ctx, queryMany.Sql)

	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()
	var rowCount int64

	rows, err := conn.QueryxContext(ctx, queryMany.Sql, queryMany.Args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var scan rowScan[T]
	results := make([]*T, 0)
	for rows.Next() {
		rowCount++
		result := *new(T)
		processed := &result

		if err := scan.scan(ctx, rows, &result); err != nil {
			collector.SetRowsProduced(rowCount)
			return nil, err
		}

		for _, processor := range queryMany.Postprocessors {
			if processed != nil {
				processed, err = processor(processed)
				if err != nil {
					collector.SetRowsProduced(rowCount)
					return nil, err
				}
			}
		}

		if processed != nil {
			results = append(results, processed)
		}
	}

	if err := rows.Err(); err != nil {
		collector.SetRowsProduced(rowCount)
		return nil, err
	}

	collector.SetRowsProduced(rowCount)
	return results, nil
}

func QueryMaps(ctx context.Context, conn *sqlx.DB, sql string) ([]exec.QueryMapResult, error) {
	sql = querycontext.AppendSQLComment(ctx, sql)

	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()
	var rowCount int64

	rows, err := conn.QueryxContext(ctx, sql)
	if err != nil {
		return nil, err
	}

	results := make([]exec.QueryMapResult, 0)
	for rows.Next() {
		rowCount++
		result := make(exec.QueryMapResult)

		if err := rows.MapScan(result); err != nil {
			collector.SetRowsProduced(rowCount)
			return nil, err
		}

		results = append(results, result)
	}

	if err := rows.Err(); err != nil {
		collector.SetRowsProduced(rowCount)
		return nil, err
	}

	collector.SetRowsProduced(rowCount)
	return results, nil
}

func Exec(ctx context.Context, db *sqlx.DB, sql string) error {
	sql = querycontext.AppendSQLComment(ctx, sql)
	_, err := db.ExecContext(ctx, sql)
	return err
}
