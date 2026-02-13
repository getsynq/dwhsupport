package mssql

import (
	"context"
	_ "embed"
	"time"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecmssql "github.com/getsynq/dwhsupport/exec/mssql"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_table_metrics.sql
var queryTableMetricsSql string

func (e *MSSQLScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	return dwhexecmssql.NewQuerier[scrapper.TableMetricsRow](e.executor).QueryMany(ctx, queryTableMetricsSql,
		dwhexec.WithPostProcessors(func(row *scrapper.TableMetricsRow) (*scrapper.TableMetricsRow, error) {
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
