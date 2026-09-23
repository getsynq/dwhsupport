package db2

import (
	"context"
	_ "embed"
	"time"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecdb2 "github.com/getsynq/dwhsupport/exec/db2"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_table_metrics.sql
var queryTableMetricsSql string

func (e *Db2Scrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	sql := scope.AppendScopeConditions(ctx, queryTableMetricsSql, "", "t.TABSCHEMA", "t.TABNAME")
	return dwhexecdb2.NewQuerier[scrapper.TableMetricsRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors(func(row *scrapper.TableMetricsRow) (*scrapper.TableMetricsRow, error) {
			row.Instance = e.conf.Hostname
			return row, nil
		}),
	)
}
