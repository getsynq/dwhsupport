package redshift

import (
	"context"
	_ "embed"
	"time"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_table_metrics.sql
var queryTableMetricsSql string

//go:embed query_table_metrics_estimated_freshness.sql
var queryTabkeMetricsEstimatedFreshnessSql string

func (e *RedshiftScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {

	sqlToRun := queryTableMetricsSql
	if e.conf.FreshnessFromQueryLogs {
		sqlToRun = queryTabkeMetricsEstimatedFreshnessSql
	}
	sqlToRun = scope.AppendScopeConditions(ctx, sqlToRun, "", `"schema"`, `"table"`)
	return stdsql.QueryMany[scrapper.TableMetricsRow](ctx, e.executor.GetDb(), sqlToRun,
		dwhexec.WithArgs[scrapper.TableMetricsRow](e.conf.Database),
		dwhexec.WithPostProcessors(func(row *scrapper.TableMetricsRow) (*scrapper.TableMetricsRow, error) {
			if row.Schema == "pg_automv" {
				return nil, nil
			}
			row.Database = e.conf.Database
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
