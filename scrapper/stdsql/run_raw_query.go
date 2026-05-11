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
	"github.com/google/uuid"
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
// (string/[]byte → StringValue), renders UUIDs as their canonical string form,
// and falls back to Stringer / fmt.Sprint for unknown driver types so
// RunRawQuery never drops a value.
func convertToRawValue(v any) scrapper.Value {
	switch val := v.(type) {
	case string:
		return scrapper.StringValue(sanitizeRawString(val))
	case [16]byte:
		// Native pgx (and google/uuid) surface UUIDs as a raw 16-byte array.
		// Format as canonical xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx.
		return scrapper.StringValue(uuid.UUID(val).String())
	case []byte:
		if id, ok := tryUUIDFromBytes(val); ok {
			return scrapper.StringValue(id)
		}
		return scrapper.StringValue(sanitizeRawString(string(val)))
	}
	converted := convertToScrapperValue(v)
	if _, ignored := converted.(scrapper.IgnoredValue); ignored {
		if s, ok := v.(fmt.Stringer); ok {
			return scrapper.StringValue(sanitizeRawString(s.String()))
		}
		return scrapper.StringValue(sanitizeRawString(fmt.Sprint(v)))
	}
	return converted
}

// tryUUIDFromBytes recognises the two byte shapes a database driver might
// return for a UUID column: the 16-byte binary form, and the 36-byte canonical
// text form. Anything else is left to the generic []byte path.
func tryUUIDFromBytes(b []byte) (string, bool) {
	switch len(b) {
	case 16:
		var arr [16]byte
		copy(arr[:], b)
		return uuid.UUID(arr).String(), true
	case 36:
		if id, err := uuid.ParseBytes(b); err == nil {
			return id.String(), true
		}
	}
	return "", false
}

func sanitizeRawString(s string) string {
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "")
	}
	return s
}
