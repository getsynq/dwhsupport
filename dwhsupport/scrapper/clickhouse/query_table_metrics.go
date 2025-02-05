package clickhouse

import (
	"context"
	_ "embed"
	"time"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_table_metrics.sql
var queryTableMetricsSql string

func (e *ClickhouseScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	return dwhexecclickhouse.NewQuerier[scrapper.TableMetricsRow](e.executor).QueryMany(ctx, queryTableMetricsSql,
		dwhexec.WithPostProcessors[scrapper.TableMetricsRow](func(row *scrapper.TableMetricsRow) (*scrapper.TableMetricsRow, error) {
			row.Database = e.conf.Host
			if len(e.conf.DatabaseName) > 0 {
				row.Database = e.conf.DatabaseName
			}
			return row, nil
		}),
	)
}
