package mysql

import (
	"context"
	_ "embed"
	"time"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecmysql "github.com/getsynq/dwhsupport/exec/mysql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_metrics.sql
var tableMetricsSql string

func (e *MySQLScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	sql := scope.AppendScopeConditions(ctx, tableMetricsSql, "", "TABLE_SCHEMA", "TABLE_NAME")
	return dwhexecmysql.NewQuerier[scrapper.TableMetricsRow](
		e.executor,
	).QueryMany(ctx, sql, dwhexec.WithPostProcessors(func(row *scrapper.TableMetricsRow) (*scrapper.TableMetricsRow, error) {
		row.Database = e.conf.Host
		return row, nil
	}))
}
