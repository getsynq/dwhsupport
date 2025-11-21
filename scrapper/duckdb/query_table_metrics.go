package duckdb

import (
	"context"
	_ "embed"
	"time"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecduckdb "github.com/getsynq/dwhsupport/exec/duckdb"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_table_metrics.sql
var queryTableMetricsSql string

func (e *DuckDBScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	return dwhexecduckdb.NewQuerier[scrapper.TableMetricsRow](e.executor).QueryMany(ctx, queryTableMetricsSql,
		dwhexec.WithPostProcessors(func(row *scrapper.TableMetricsRow) (*scrapper.TableMetricsRow, error) {
			row.Instance = e.conf.MotherduckAccount
			return row, nil
		}))
}
