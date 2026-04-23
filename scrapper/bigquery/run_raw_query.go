package bigquery

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	execbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/scrapper"
	"google.golang.org/api/iterator"
)

func (e *BigQueryScrapper) RunRawQuery(ctx context.Context, sql string) (scrapper.RawQueryRowIterator, error) {
	collector, ctx := querystats.Start(ctx)

	query := e.executor.GetBigQueryClient().Query(sql)
	job, err := query.Run(ctx)
	if err != nil {
		collector.Finish()
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		collector.Finish()
		return nil, err
	}
	execbigquery.CollectBigQueryStats(ctx, job)

	schema := it.Schema
	columns := make([]*scrapper.QueryShapeColumn, len(schema))
	columnNames := make([]string, len(schema))
	for i, field := range schema {
		columns[i] = &scrapper.QueryShapeColumn{
			Name:       field.Name,
			NativeType: string(field.Type),
			Position:   int32(i + 1),
		}
		columnNames[i] = field.Name
	}

	return &bqRawRowsIterator{
		it:          it,
		columns:     columns,
		columnNames: columnNames,
		collector:   collector,
	}, nil
}

type bqRawRowsIterator struct {
	it          *bigquery.RowIterator
	columns     []*scrapper.QueryShapeColumn
	columnNames []string
	collector   *querystats.Collector

	mu       sync.Mutex
	closed   bool
	rowCount int64
}

func (it *bqRawRowsIterator) Columns() []*scrapper.QueryShapeColumn {
	return it.columns
}

func (it *bqRawRowsIterator) Next(ctx context.Context) ([]*scrapper.ColumnValue, error) {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return nil, io.EOF
	}

	if err := ctx.Err(); err != nil {
		it.closeLocked()
		return nil, err
	}

	var row map[string]bigquery.Value
	err := it.it.Next(&row)
	if err == iterator.Done {
		it.closeLocked()
		return nil, io.EOF
	}
	if err != nil {
		it.closeLocked()
		return nil, err
	}
	it.rowCount++

	values := make([]*scrapper.ColumnValue, len(it.columnNames))
	for i, name := range it.columnNames {
		raw, exists := row[name]
		cv := &scrapper.ColumnValue{Name: name, IsNull: !exists || raw == nil}
		if !cv.IsNull {
			cv.Value = bqValueToScrapperValue(raw)
		}
		values[i] = cv
	}
	return values, nil
}

func (it *bqRawRowsIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	it.closeLocked()
	return nil
}

func (it *bqRawRowsIterator) closeLocked() {
	if it.closed {
		return
	}
	it.closed = true
	it.collector.SetRowsProduced(it.rowCount)
	it.collector.Finish()
}

func bqValueToScrapperValue(v bigquery.Value) scrapper.Value {
	switch val := v.(type) {
	case int64:
		return scrapper.IntValue(val)
	case float64:
		return scrapper.DoubleValue(val)
	case bool:
		if val {
			return scrapper.IntValue(1)
		}
		return scrapper.IntValue(0)
	case time.Time:
		return scrapper.TimeValue(val)
	case civil.DateTime:
		return scrapper.TimeValue(val.In(time.UTC))
	case civil.Date:
		return scrapper.TimeValue(val.In(time.UTC))
	case civil.Time:
		return scrapper.StringValue(val.String())
	case string:
		return scrapper.StringValue(sanitizeUTF8(val))
	case []byte:
		return scrapper.StringValue(sanitizeUTF8(string(val)))
	default:
		return scrapper.StringValue(sanitizeUTF8(fmt.Sprint(v)))
	}
}

func sanitizeUTF8(s string) string {
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "")
	}
	return s
}
