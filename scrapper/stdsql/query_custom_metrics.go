package stdsql

import (
	"context"
	"math/big"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/scrapper"
)

// QueryCustomMetrics runs a metrics query. dialect is Scrapper.DialectType(),
// which decides what each column's native type means; a time is decoded by
// its column's kind (see scrapper.NormalizeTimeOfKind).
func QueryCustomMetrics(ctx context.Context, db RowQuerier, dialect string, sqlQuery string, args ...any) ([]*scrapper.CustomMetricsRow, error) {
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()
	var rowCount int64

	sqlRows, err := db.QueryRows(ctx, sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()

	columnTypes, err := sqlRows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	columns := make([]string, len(columnTypes))
	kinds := make([]scrapper.ValueKind, len(columnTypes))
	for i, ct := range columnTypes {
		columns[i] = ct.Name()
		kinds[i] = scrapper.NativeValueKind(dialect, ct.DatabaseTypeName())
	}

	result := make([]*scrapper.CustomMetricsRow, 0)

	for sqlRows.Next() {
		rowCount++
		if err := sqlRows.Err(); err != nil {
			collector.SetRowsProduced(rowCount)
			return nil, err
		}

		// Create scanners for each column using interface{} to handle various driver types
		scanners := make([]any, len(columns))
		for i := range columns {
			scanners[i] = new(any)
		}

		if err := sqlRows.Scan(scanners...); err != nil {
			collector.SetRowsProduced(rowCount)
			return nil, err
		}

		row := &scrapper.CustomMetricsRow{
			ColumnValues: make([]*scrapper.ColumnValue, 0, len(columns)),
		}

		// Process each column
		for i, colName := range columns {
			rawValue := *(scanners[i].(*any))
			isNull := rawValue == nil

			if strings.HasPrefix(strings.ToLower(colName), "segment") {
				if !isNull {
					strValue := valueToString(rawValue)
					// Clean invalid UTF-8 characters
					if !utf8.ValidString(strValue) {
						strValue = strings.ToValidUTF8(strValue, "")
					}
					row.Segments = append(row.Segments, &scrapper.SegmentValue{
						Name:  colName,
						Value: strValue,
					})
				}
				continue
			}

			colValue := &scrapper.ColumnValue{
				Name:   colName,
				IsNull: isNull,
			}

			if !isNull {
				colValue.Value = convertToScrapperValue(rawValue, kinds[i])
			}

			row.ColumnValues = append(row.ColumnValues, colValue)
		}

		result = append(result, row)
	}

	if err := sqlRows.Err(); err != nil {
		collector.SetRowsProduced(rowCount)
		return nil, err
	}

	collector.SetRowsProduced(rowCount)
	return result, nil
}

// valueToString converts various database driver types to string
func valueToString(v any) string {
	switch val := v.(type) {
	case []byte:
		return string(val)
	case string:
		return val
	case *big.Int:
		return val.String()
	default:
		return ""
	}
}

// Interfaces for decimal/numeric types from various database drivers

// float64er is for types with Float64() float64 (e.g., duckdb.Decimal)
type float64er interface {
	Float64() float64
}

// float64WithExact is for types with Float64() (float64, bool) (e.g., shopspring/decimal used by ClickHouse)
type float64WithExact interface {
	Float64() (float64, bool)
}

// inexactFloat64er is for types with InexactFloat64() float64 (e.g., shopspring/decimal)
type inexactFloat64er interface {
	InexactFloat64() float64
}

// bigInter is for types that can return a *big.Int (e.g., shopspring/decimal.BigInt())
type bigInter interface {
	BigInt() *big.Int
}

// convertToScrapperValue converts database driver values to scrapper value
// types. kind is the column's, and decides how a time is normalised.
func convertToScrapperValue(v any, kind scrapper.ValueKind) scrapper.Value {
	switch val := v.(type) {
	case int64:
		return scrapper.IntValue(val)
	case int32:
		return scrapper.IntValue(int64(val))
	case int16:
		return scrapper.IntValue(int64(val))
	case int8:
		return scrapper.IntValue(int64(val))
	case int:
		return scrapper.IntValue(int64(val))
	case uint64:
		// Handle potential overflow when converting uint64 to int64
		if val <= 9223372036854775807 {
			return scrapper.IntValue(int64(val))
		}
		return scrapper.DoubleValue(float64(val))
	case uint32:
		return scrapper.IntValue(int64(val))
	case uint16:
		return scrapper.IntValue(int64(val))
	case uint8:
		return scrapper.IntValue(int64(val))
	case float64:
		return scrapper.DoubleValue(val)
	case float32:
		return scrapper.DoubleValue(float64(val))
	case bool:
		if val {
			return scrapper.IntValue(1)
		}
		return scrapper.IntValue(0)
	case time.Time:
		return scrapper.TimeValue(scrapper.NormalizeTimeOfKind(val, kind))
	case *big.Rat:
		f, _ := val.Float64()
		return scrapper.DoubleValue(f)
	case *big.Int:
		// Handle *big.Int (e.g., DuckDB hugeint)
		// If it fits in int64, use IntValue; otherwise use BigIntValue to preserve precision
		if val.IsInt64() {
			return scrapper.IntValue(val.Int64())
		}
		return scrapper.NewBigIntValue(val)
	case big.Int:
		// Handle big.Int value (e.g., ClickHouse Int128/Int256)
		// If it fits in int64, use IntValue; otherwise use BigIntValue to preserve precision
		if val.IsInt64() {
			return scrapper.IntValue(val.Int64())
		}
		return scrapper.NewBigIntValue(&val)
	case []byte:
		return parseByteValue(val)
	case string:
		return parseStringValue(val)
	default:
		// Try various decimal/numeric type interfaces
		return convertDecimalType(v)
	}
}

// convertDecimalType handles various decimal/numeric types from different database drivers
func convertDecimalType(v any) scrapper.Value {
	// Try Float64() float64 (duckdb.Decimal)
	if f, ok := v.(float64er); ok {
		return scrapper.DoubleValue(f.Float64())
	}

	// Try Float64() (float64, bool) (shopspring/decimal used by ClickHouse)
	if f, ok := v.(float64WithExact); ok {
		val, _ := f.Float64()
		return scrapper.DoubleValue(val)
	}

	// Try InexactFloat64() float64 (shopspring/decimal)
	if f, ok := v.(inexactFloat64er); ok {
		return scrapper.DoubleValue(f.InexactFloat64())
	}

	// Try BigInt() *big.Int for integer decimals (shopspring/decimal)
	if b, ok := v.(bigInter); ok {
		bigVal := b.BigInt()
		if bigVal.IsInt64() {
			return scrapper.IntValue(bigVal.Int64())
		}
		return scrapper.NewBigIntValue(bigVal)
	}

	return scrapper.IgnoredValue{}
}

// parseByteValue parses a byte slice value (e.g., from sql.RawBytes)
func parseByteValue(val []byte) scrapper.Value {
	return parseStringValue(string(val))
}

// parseStringValue reads a cell the driver handed back as text; see
// scrapper.MetricValueFromText.
func parseStringValue(strVal string) scrapper.Value {
	return scrapper.MetricValueFromText(strVal)
}
