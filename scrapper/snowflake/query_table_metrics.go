package snowflake

import (
	"context"
	"fmt"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/pkg/errors"
)

var tableMetricsSql = `
	select
		table_catalog as "database",
		table_schema as "schema",
		table_name as "table",
		row_count as "row_count",
		bytes as "size_bytes",
		last_altered as "updated_at"
	from
		%s.information_schema.tables
	where
		row_count is not null and table_schema not in ('INFORMATION_SCHEMA')
		/* SYNQ_SCOPE_FILTER */
	`

func (e *SnowflakeScrapper) QueryTableMetrics(origCtx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	if e.conf.TableMetricsFromAccountUsage {
		return e.queryTableMetricsFromAccountUsage(origCtx)
	}
	databasesToQuery, err := e.getDatabasesWithPrivileges(origCtx)
	if err != nil {
		return nil, err
	}

	return readDatabases(
		origCtx, databasesToQuery, 8, scrapper.OmittedMetrics,
		func(groupCtx context.Context, database string) ([]*scrapper.TableMetricsRow, error) {
			var tmpResults []*scrapper.TableMetricsRow

			query := scope.AppendScopeConditions(groupCtx, fmt.Sprintf(tableMetricsSql, databaseIdent(database)), "", "table_schema", "table_name")
			rows, err := e.executor.QueryRows(groupCtx, query)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to query metrics for database %s", database)
			}
			defer rows.Close()

			metricsRows, err := scrapper.ScanAll[scrapper.TableMetricsRow](
				groupCtx, rows, fmt.Sprintf("%s.information_schema.tables", database),
			)
			if err != nil {
				return nil, err
			}
			for _, result := range metricsRows {
				result.Instance = e.conf.Account
				if result.UpdatedAt != nil {
					normalized := result.UpdatedAt.UTC()
					result.UpdatedAt = &normalized
				}
				tmpResults = append(tmpResults, result)
			}

			return tmpResults, nil
		},
	)
}
