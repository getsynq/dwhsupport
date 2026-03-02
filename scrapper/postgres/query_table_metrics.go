package postgres

import (
	"context"
	_ "embed"
	"time"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecpostgres "github.com/getsynq/dwhsupport/exec/postgres"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	_ "github.com/lib/pq"
)

//go:embed query_table_metrics.sql
var queryTableMetricsSql string

func (e *PostgresScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	sql := scope.AppendSchemaScopeConditions(ctx, queryTableMetricsSql, "", "sch.nspname")
	return dwhexecpostgres.NewQuerier[scrapper.TableMetricsRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors(func(row *scrapper.TableMetricsRow) (*scrapper.TableMetricsRow, error) {
			row.Database = e.conf.Database
			row.Instance = e.conf.Host
			return row, nil
		}))
}
