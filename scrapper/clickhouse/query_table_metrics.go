package clickhouse

import (
	"context"
	_ "embed"
	"time"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_table_metrics.sql
var queryTableMetricsSql string

func (e *ClickhouseScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	sql := scope.AppendScopeConditions(ctx, e.systemTablesSql(queryTableMetricsSql), "", "tbls.database", "tbls.name")
	return dwhexecclickhouse.NewQuerier[scrapper.TableMetricsRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors[scrapper.TableMetricsRow](func(row *scrapper.TableMetricsRow) (*scrapper.TableMetricsRow, error) {
			row.Instance, row.Database = e.rowIdentity()
			return row, nil
		}),
	)
}
