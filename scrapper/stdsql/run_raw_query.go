package stdsql

import (
	"context"
	"fmt"
	"io"
	"reflect"
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
			cv.Value = convertToRawValue(raw, it.columns[i].NativeType)
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
// normalises complex/nested cells (arrays, structs, maps, JSON/variant columns)
// to a single JsonValue, and falls back to Stringer / fmt.Sprint for unknown
// scalar driver types so RunRawQuery never drops a value.
//
// nativeType is the column's DatabaseTypeName(); it disambiguates the two cases
// Go types alone cannot: a native nested type (ClickHouse Array/Map/Tuple, whose
// driver values may include []uint8 integer arrays) versus a JSON-string type
// (Snowflake ARRAY/OBJECT/VARIANT), versus a plain text/blob column.
func convertToRawValue(v any, nativeType string) scrapper.Value {
	// Native nested types (ClickHouse Array/Map/Tuple/Nested): the driver returns
	// nested Go slices/maps, and Array(UInt8) arrives as []uint8 — render bytes as
	// an integer array, not a string.
	if isNativeNestedType(nativeType) {
		if jv, ok := scrapper.NewJsonValueFromGo(v, true); ok {
			return jv
		}
	}

	switch val := v.(type) {
	case string:
		// Semi-structured string columns (Snowflake ARRAY/OBJECT/VARIANT) carry
		// JSON text; preserve their structure as JsonValue.
		if isJSONTextType(nativeType) {
			if jv, ok := scrapper.NewJsonValueFromJSONText(val); ok {
				return jv
			}
		}
		return scrapper.StringValue(sanitizeRawString(val))
	case [16]byte:
		// Native pgx (and google/uuid) surface UUIDs as a raw 16-byte array.
		// Format as canonical xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx.
		return scrapper.StringValue(uuid.UUID(val).String())
	case []byte:
		if id, ok := tryUUIDFromBytes(val); ok {
			return scrapper.StringValue(id)
		}
		// Redshift SUPER surfaces as a JSON []byte with an empty DatabaseTypeName;
		// other engines expose JSON via a named type. Parse it as JSON only when
		// the type signals JSON (or is unnamed) and the bytes actually parse, so
		// genuine text/blob columns are left untouched.
		if isJSONTextType(nativeType) || nativeType == "" {
			if looksLikeJSON(val) {
				if jv, ok := scrapper.NewJsonValueFromJSONText(string(val)); ok {
					return jv
				}
			}
		}
		return scrapper.StringValue(sanitizeRawString(string(val)))
	}

	// Generic containers from any dialect (pgx arrays, DuckDB LIST/STRUCT/MAP,
	// Trino array/map/row, ...) — anything the driver hands back as a Go
	// slice/array/map that is not a []byte blob is a complex cell.
	switch reflect.ValueOf(v).Kind() {
	case reflect.Slice, reflect.Array, reflect.Map:
		if jv, ok := scrapper.NewJsonValueFromGo(v, false); ok {
			return jv
		}
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

// isNativeNestedType reports whether a column's DatabaseTypeName denotes a type
// whose driver value is a native nested Go structure (slice/map) rather than a
// scalar or a JSON string. This is the ClickHouse family — Array(...), Map(...),
// Tuple(...), Nested(...) — including LowCardinality/Nullable-wrapped forms.
func isNativeNestedType(nativeType string) bool {
	t := nativeType
	for {
		switch {
		case strings.HasPrefix(t, "Array("),
			strings.HasPrefix(t, "Map("),
			strings.HasPrefix(t, "Tuple("),
			strings.HasPrefix(t, "Nested("):
			return true
		case strings.HasPrefix(t, "LowCardinality("):
			t = t[len("LowCardinality("):]
		case strings.HasPrefix(t, "Nullable("):
			t = t[len("Nullable("):]
		default:
			return false
		}
	}
}

// isJSONTextType reports whether a column's DatabaseTypeName denotes a
// semi-structured type the driver surfaces as JSON text (Snowflake
// ARRAY/OBJECT/VARIANT, Redshift SUPER, generic JSON/JSONB).
func isJSONTextType(nativeType string) bool {
	switch strings.ToUpper(nativeType) {
	case "ARRAY", "OBJECT", "VARIANT", "SUPER", "JSON", "JSONB":
		return true
	}
	return false
}

// looksLikeJSON reports whether b begins (ignoring leading whitespace) with a
// JSON array or object delimiter, so we only attempt to parse plausible JSON.
func looksLikeJSON(b []byte) bool {
	for _, c := range b {
		switch c {
		case ' ', '\t', '\r', '\n':
			continue
		case '[', '{':
			return true
		default:
			return false
		}
	}
	return false
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
