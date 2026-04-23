package stdsql

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/jmoiron/sqlx"
)

// RunRawQuery executes sqlQuery and returns an iterator over typed rows. The
// iterator must be Close()'d by the caller; it also auto-closes on io.EOF.
//
// Unlike QueryCustomMetrics this path preserves every column: segment* names
// are not sidelined, and string columns come back as StringValue rather than
// being re-parsed and collapsed to IgnoredValue. Unknown driver types fall
// back to StringValue(fmt.Sprint(v)) so nothing is silently dropped.
func RunRawQuery(ctx context.Context, db RowQuerier, sqlQuery string) (scrapper.RawQueryRowIterator, error) {
	collector, ctx := querystats.Start(ctx)

	sqlRows, err := db.QueryRows(ctx, sqlQuery)
	if err != nil {
		collector.Finish()
		return nil, err
	}

	columnTypes, err := sqlRows.ColumnTypes()
	if err != nil {
		_ = sqlRows.Close()
		collector.Finish()
		return nil, err
	}

	columns := make([]*scrapper.QueryShapeColumn, len(columnTypes))
	columnNames := make([]string, len(columnTypes))
	for i, ct := range columnTypes {
		columns[i] = &scrapper.QueryShapeColumn{
			Name:       ct.Name(),
			NativeType: ct.DatabaseTypeName(),
			Position:   int32(i + 1),
		}
		columnNames[i] = ct.Name()
	}

	return &rawRowsIterator{
		rows:        sqlRows,
		columns:     columns,
		columnNames: columnNames,
		collector:   collector,
	}, nil
}

type rawRowsIterator struct {
	rows        *sqlx.Rows
	columns     []*scrapper.QueryShapeColumn
	columnNames []string
	collector   *querystats.Collector

	mu       sync.Mutex
	closed   bool
	rowCount int64
}

func (it *rawRowsIterator) Columns() []*scrapper.QueryShapeColumn {
	return it.columns
}

func (it *rawRowsIterator) Next(ctx context.Context) ([]*scrapper.ColumnValue, error) {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return nil, io.EOF
	}

	if err := ctx.Err(); err != nil {
		it.closeLocked()
		return nil, err
	}

	if !it.rows.Next() {
		if err := it.rows.Err(); err != nil {
			it.closeLocked()
			return nil, err
		}
		it.closeLocked()
		return nil, io.EOF
	}
	it.rowCount++

	scanners := make([]any, len(it.columnNames))
	for i := range it.columnNames {
		scanners[i] = new(any)
	}
	if err := it.rows.Scan(scanners...); err != nil {
		it.closeLocked()
		return nil, err
	}

	values := make([]*scrapper.ColumnValue, len(it.columnNames))
	for i, name := range it.columnNames {
		raw := *(scanners[i].(*any))
		cv := &scrapper.ColumnValue{Name: name, IsNull: raw == nil}
		if raw != nil {
			cv.Value = convertToRawValue(raw)
		}
		values[i] = cv
	}
	return values, nil
}

func (it *rawRowsIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.closeLocked()
}

func (it *rawRowsIterator) closeLocked() error {
	if it.closed {
		return nil
	}
	it.closed = true
	err := it.rows.Close()
	it.collector.SetRowsProduced(it.rowCount)
	it.collector.Finish()
	return err
}

// convertToRawValue mirrors convertToScrapperValue but preserves text
// (string/[]byte → StringValue) and renders unknown driver types via
// fmt.Sprint so RunRawQuery never drops a value.
func convertToRawValue(v any) scrapper.Value {
	switch val := v.(type) {
	case string:
		return scrapper.StringValue(sanitizeRawString(val))
	case []byte:
		return scrapper.StringValue(sanitizeRawString(string(val)))
	}
	converted := convertToScrapperValue(v)
	if _, ignored := converted.(scrapper.IgnoredValue); ignored {
		return scrapper.StringValue(sanitizeRawString(fmt.Sprint(v)))
	}
	return converted
}

func sanitizeRawString(s string) string {
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "")
	}
	return s
}
