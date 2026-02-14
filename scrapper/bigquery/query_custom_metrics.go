package bigquery

import (
	"context"
	"strings"
	"time"
	"unicode/utf8"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	execbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/scrapper"
	"google.golang.org/api/iterator"
)

func (e *BigQueryScrapper) QueryCustomMetrics(ctx context.Context, sql string, args ...any) ([]*scrapper.CustomMetricsRow, error) {
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()
	var rowCount int64

	// Create a query using the BigQuery executor
	query := e.executor.GetBigQueryClient().Query(sql)

	// Run the query
	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}

	// Get the results
	it, err := job.Read(ctx)
	if err != nil {
		return nil, err
	}

	// Collect BigQuery job statistics
	execbigquery.CollectBigQueryStats(ctx, job)

	// Get the schema to extract column names
	schema := it.Schema
	columns := make([]string, len(schema))
	for i, field := range schema {
		columns[i] = field.Name
	}

	result := make([]*scrapper.CustomMetricsRow, 0)

	// Iterate through the results
	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			collector.SetRowsProduced(rowCount)
			return nil, err
		}
		rowCount++

		customRow := &scrapper.CustomMetricsRow{
			ColumnValues: make([]*scrapper.ColumnValue, 0, len(columns)),
		}

		// Process each column
		for _, colName := range columns {
			value, exists := row[colName]
			isNull := !exists || value == nil

			// Handle segment column specially
			if strings.HasPrefix(strings.ToLower(colName), "segment") {
				if !isNull {
					strValue, ok := value.(string)
					if ok {
						// Clean invalid UTF-8 characters
						if !utf8.ValidString(strValue) {
							strValue = strings.ToValidUTF8(strValue, "")
						}
						customRow.Segments = append(customRow.Segments, &scrapper.SegmentValue{
							Name:  colName,
							Value: strValue,
						})
					}
				}
				continue
			}

			colValue := &scrapper.ColumnValue{
				Name:   colName,
				IsNull: isNull,
			}

			if !isNull {
				// Handle different BigQuery value types
				switch v := value.(type) {
				case int64:
					colValue.Value = scrapper.IntValue(v)
				case float64:
					colValue.Value = scrapper.DoubleValue(v)
				case bool:
					if v {
						colValue.Value = scrapper.IntValue(1)
					} else {
						colValue.Value = scrapper.IntValue(0)
					}
				case civil.DateTime:
					colValue.Value = scrapper.TimeValue(v.In(time.UTC))
				case civil.Date:
					colValue.Value = scrapper.TimeValue(v.In(time.UTC))
				case time.Time:
					colValue.Value = scrapper.TimeValue(v)
				case string:
					// Try to parse string as other types
					// For BigQuery, we'll just treat strings as ignored values
					// as they're typically already properly typed
					colValue.Value = scrapper.IgnoredValue{}
				default:
					// Unsupported type
					colValue.Value = scrapper.IgnoredValue{}
				}
			}

			customRow.ColumnValues = append(customRow.ColumnValues, colValue)
		}

		result = append(result, customRow)
	}

	collector.SetRowsProduced(rowCount)
	return result, nil
}
