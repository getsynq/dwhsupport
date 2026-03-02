package duckdb

import (
	"context"
	_ "embed"
	"time"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecduckdb "github.com/getsynq/dwhsupport/exec/duckdb"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_table_metrics.sql
var queryTableMetricsSql string

func (e *DuckDBScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	sql := scope.AppendScopeConditions(ctx, queryTableMetricsSql, "t.database_name", "t.schema_name", "t.table_name")
	return dwhexecduckdb.NewQuerier[scrapper.TableMetricsRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors(func(row *scrapper.TableMetricsRow) (*scrapper.TableMetricsRow, error) {
			row.Instance = e.conf.MotherduckAccount
			return row, nil
		}))
}
