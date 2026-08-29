package scrapper

import (
	"context"

	"github.com/getsynq/dwhsupport/rowscan"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// ScanAll reads an open result set into one entry per row.
//
// Every catalog query in this repository ends in the same loop, and having one
// implementation of it is what lets the way a warehouse's columns are matched to
// a struct be decided once rather than per query. It goes through RowScanner, so
// the result set decides the shape: a column no field claims is discarded rather
// than fatal, a field no column feeds stays at its zero value, and the match is
// case-insensitive. A caller that cannot work without a given column plans the
// scan itself and says so with RequireColumns.
//
// source names the object read ("MY_DB.information_schema.tables"). It is what a
// scan failure is reported against and what column drift is logged against — the
// caller's own wrapping says which step it belonged to.
func ScanAll[T any](ctx context.Context, rows *sqlx.Rows, source string) ([]*T, error) {
	scanner, err := rowscan.New[T](rows)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to plan the scan of %s", source)
	}
	scanner.LogColumnDrift(ctx, source)

	var results []*T
	for rows.Next() {
		var row T
		if err := scanner.Scan(rows, &row); err != nil {
			return nil, errors.Wrapf(err, "failed to scan a row of %s", source)
		}
		results = append(results, &row)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrapf(err, "failed to read %s", source)
	}
	return results, nil
}
