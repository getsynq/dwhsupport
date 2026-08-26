package rowscan

import (
	"context"
	"reflect"
	"strings"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Scanner scans a result set into a struct by `db` tag while tolerating a
// result set whose columns do not match the struct exactly.
//
// sqlx's StructScan is all-or-nothing in one direction: a single result column
// that no `db` tag claims fails every row with "missing destination name X in
// *T". Two things routinely produce such a column, and both are outside our
// control. A warehouse-managed view gains one on a vendor release, which turns
// an ingest into a total outage rather than a missing field. And an account
// configured to fold quoted identifiers to upper case returns every alias in a
// case the query did not ask for, so nothing matches at all.
//
// Pinning an explicit column list in the query instead only moves the problem:
// the query then fails outright the day a column is withdrawn, or against a
// narrowed view exposing a subset.
//
// So neither the struct nor the query decides the shape — the result set does.
// A column with no field is discarded and reported by UnknownColumns; a `db` tag
// with no column is left at its zero value and reported by MissingColumns.
// Callers that cannot work without a given column say so with RequireColumns
// rather than leaving it to the scan to fail somewhere downstream.
//
// Only fields carrying an explicit `db` tag participate; fields the scrapper
// fills in itself stay untouched. Matching is case-insensitive.
type Scanner[T any] struct {
	// fieldIndex holds, per result column, the index path of the field it
	// lands in, or nil for a column to discard.
	fieldIndex [][]int
	unknown    []string
	missing    []string
	// targets is scratch reused across rows, one entry per result column.
	targets []any
	// sinks holds the throwaway destinations for discarded columns, so a
	// discarded column costs one allocation per query rather than per row.
	sinks []any
}

type dbField struct {
	column string
	index  []int
}

// New plans how the columns of an open result set map onto T.
func New[T any](rows *sqlx.Rows) (*Scanner[T], error) {
	structType := reflect.TypeFor[T]()
	if structType.Kind() != reflect.Struct {
		return nil, errors.Errorf("row scanner target must be a struct, got %s", structType)
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "could not read result set columns")
	}

	fields, err := collectDbFields(structType, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "could not map %s onto the result set", structType)
	}

	indexByColumn := make(map[string][]int, len(fields))
	for _, field := range fields {
		indexByColumn[normalizeColumn(field.column)] = field.index
	}

	scanner := &Scanner[T]{
		fieldIndex: make([][]int, len(columns)),
		targets:    make([]any, len(columns)),
		sinks:      make([]any, len(columns)),
	}

	claimed := make(map[string]bool, len(columns))
	for i, column := range columns {
		normalized := normalizeColumn(column)
		index, found := indexByColumn[normalized]
		if !found {
			scanner.unknown = append(scanner.unknown, column)
			continue
		}
		if claimed[normalized] {
			// A duplicated column would otherwise have its later copy
			// silently overwrite the earlier one.
			scanner.unknown = append(scanner.unknown, column)
			continue
		}
		claimed[normalized] = true
		scanner.fieldIndex[i] = index
	}

	for _, field := range fields {
		if !claimed[normalizeColumn(field.column)] {
			scanner.missing = append(scanner.missing, field.column)
		}
	}

	return scanner, nil
}

// UnknownColumns lists result columns the struct has no field for. They are
// discarded rather than fatal, and are the expected shape of a vendor adding a
// column to a view we read.
func (s *Scanner[T]) UnknownColumns() []string {
	return s.unknown
}

// MissingColumns lists `db` tags the result set does not carry. Those fields stay
// at their zero value.
func (s *Scanner[T]) MissingColumns() []string {
	return s.missing
}

// RequireColumns fails when a column the caller cannot do without is absent from
// the result set, so an unusable read is rejected up front instead of producing
// rows with holes in them.
func (s *Scanner[T]) RequireColumns(columns ...string) error {
	var absent []string
	for _, column := range columns {
		for _, missing := range s.missing {
			if normalizeColumn(missing) == normalizeColumn(column) {
				absent = append(absent, column)
				break
			}
		}
	}
	if len(absent) > 0 {
		return errors.Errorf("result set is missing required column(s) %s", strings.Join(absent, ", "))
	}
	return nil
}

// LogColumnDrift reports the difference between the result set and the struct, so
// a vendor adding or withdrawing a column surfaces in logs on the run it first
// happens rather than the next time someone reads the code. source names the
// object read, for example "SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY".
func (s *Scanner[T]) LogColumnDrift(ctx context.Context, source string) {
	if len(s.unknown) == 0 && len(s.missing) == 0 {
		return
	}
	logging.GetLogger(ctx).WithFields(
		logrus.Fields{
			"source":          source,
			"target":          reflect.TypeFor[T]().String(),
			"unknown_columns": s.unknown,
			"missing_columns": s.missing,
		},
	).Warn("result set columns differ from the expected shape, scanning what matched")
}

// Scan reads the current row into dest.
func (s *Scanner[T]) Scan(rows *sqlx.Rows, dest *T) error {
	value := reflect.ValueOf(dest).Elem()
	for i, index := range s.fieldIndex {
		if index == nil {
			if s.sinks[i] == nil {
				s.sinks[i] = new(any)
			}
			s.targets[i] = s.sinks[i]
			continue
		}
		s.targets[i] = value.FieldByIndex(index).Addr().Interface()
	}
	return rows.Scan(s.targets...)
}

// collectDbFields walks a struct's `db`-tagged fields in declaration order,
// descending into embedded structs the way sqlx does.
func collectDbFields(structType reflect.Type, prefix []int) ([]dbField, error) {
	var fields []dbField
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		index := append(append([]int{}, prefix...), i)

		tag := field.Tag.Get("db")
		if tag == "-" {
			continue
		}
		if tag == "" {
			if field.Anonymous && field.Type.Kind() == reflect.Struct {
				embedded, err := collectDbFields(field.Type, index)
				if err != nil {
					return nil, err
				}
				fields = append(fields, embedded...)
			}
			continue
		}
		if !field.IsExported() {
			return nil, errors.Errorf("field %s is unexported and cannot receive column %s", field.Name, tag)
		}

		for _, existing := range fields {
			if normalizeColumn(existing.column) == normalizeColumn(tag) {
				return nil, errors.Errorf("column %s is claimed by more than one field", tag)
			}
		}
		fields = append(fields, dbField{column: tag, index: index})
	}
	return fields, nil
}

func normalizeColumn(column string) string {
	return strings.ToUpper(strings.TrimSpace(column))
}
