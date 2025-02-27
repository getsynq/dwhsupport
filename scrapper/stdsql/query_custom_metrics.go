package stdsql

import (
	"context"
	"database/sql"
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

		// Create scanners for each column
		scanners := make([]any, len(columns))
		for i := range columns {
			scanners[i] = new(sql.RawBytes)
		}

		if err := sqlRows.Scan(scanners...); err != nil {
			return nil, err
		}

		row := &scrapper.CustomMetricsRow{
			ColumnValues: make([]*scrapper.ColumnValue, 0, len(columns)),
		}

		// Process each column
		for i, colName := range columns {
			rawValue := scanners[i].(*sql.RawBytes)
			isNull := *rawValue == nil

			if strings.ToLower(colName) == "segment" {
				if !isNull {
					strValue := string(*rawValue)
					// Clean invalid UTF-8 characters
					if !utf8.ValidString(strValue) {
						strValue = strings.ToValidUTF8(strValue, "")
					}
					row.Segment = &strValue
				}
				continue
			}

			colValue := &scrapper.ColumnValue{
				Name:   colName,
				IsNull: isNull,
			}

			if !isNull {
				strVal := string(*rawValue)
				// Try parsing as different types
				if v, err := strconv.ParseInt(strVal, 10, 64); err == nil {
					colValue.Value = scrapper.IntValue(v)
				} else if v, err := strconv.ParseFloat(strVal, 64); err == nil {
					colValue.Value = scrapper.DoubleValue(v)
				} else if v, err := strconv.ParseBool(strVal); err == nil {
					colValue.Value = scrapper.IntValue(0)
					if v {
						colValue.Value = scrapper.IntValue(1)
					}
				} else if t, err := time.Parse(time.RFC3339Nano, strVal); err == nil {
					// Convert timestamp to Unix timestamp in seconds as int64
					colValue.Value = scrapper.TimeValue(t)
				} else if t, err := time.Parse("2006-01-02 15:04:05.999999999", strVal); err == nil {
					// Try common SQL timestamp format
					colValue.Value = scrapper.TimeValue(t)
				} else {
					// Unsupported type, skip this column
					colValue.Value = scrapper.IgnoredValue{}
				}
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
