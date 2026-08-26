package scrapper

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// ScanAll reads an open result set into one entry per row.
//
// Every catalog query in this repository ends in the same loop, and having one
// implementation of it is what lets the way a warehouse's columns are matched to
// a struct be decided once rather than per query. source names the object read
// ("MY_DB.information_schema.tables"), and is what a scan failure is reported
// against — the caller's own wrapping says which step it belonged to.
func ScanAll[T any](ctx context.Context, rows *sqlx.Rows, source string) ([]*T, error) {
	var results []*T
	for rows.Next() {
		var row T
		if err := rows.StructScan(&row); err != nil {
			return nil, errors.Wrapf(err, "failed to scan a row of %s", source)
		}
		results = append(results, &row)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrapf(err, "failed to read %s", source)
	}
	return results, nil
}
