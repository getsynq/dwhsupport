package stdsql

import (
	"context"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/jmoiron/sqlx"
)

func QueryCustomMetrics(ctx context.Context, db *sqlx.DB, sqlQuery string, args ...any) ([]*scrapper.CustomMetricsRow, error) {
	sqlRows, err := db.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()

	columnTypes, err := sqlRows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	columns := make([]string, len(columnTypes))
	for i, ct := range columnTypes {
		columns[i] = ct.Name()
	}

	result := make([]*scrapper.CustomMetricsRow, 0)

	for sqlRows.Next() {
		if err := sqlRows.Err(); err != nil {
			return nil, err
		}

		// Create scanners for each column using interface{} to handle various driver types
		scanners := make([]any, len(columns))
		for i := range columns {
			scanners[i] = new(any)
		}

		if err := sqlRows.Scan(scanners...); err != nil {
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
				colValue.Value = convertToScrapperValue(rawValue)
			}

			row.ColumnValues = append(row.ColumnValues, colValue)
		}

		result = append(result, row)
	}

	if err := sqlRows.Err(); err != nil {
		return nil, err
	}

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

// convertToScrapperValue converts database driver values to scrapper value types
func convertToScrapperValue(v any) scrapper.Value {
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
		return scrapper.TimeValue(val)
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

// parseStringValue parses a string value and determines its type
func parseStringValue(strVal string) scrapper.Value {
	// Try parsing as different types
	if v, err := strconv.ParseInt(strVal, 10, 64); err == nil {
		return scrapper.IntValue(v)
	}
	if v, err := strconv.ParseFloat(strVal, 64); err == nil {
		return scrapper.DoubleValue(v)
	}
	if v, err := strconv.ParseBool(strVal); err == nil {
		if v {
			return scrapper.IntValue(1)
		}
		return scrapper.IntValue(0)
	}
	if t, err := time.Parse(time.RFC3339Nano, strVal); err == nil {
		return scrapper.TimeValue(t)
	}
	if t, err := time.Parse("2006-01-02 15:04:05.999999999", strVal); err == nil {
		return scrapper.TimeValue(t)
	}
	// Unsupported type
	return scrapper.IgnoredValue{}
}
