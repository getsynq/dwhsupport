package snowflake

import (
	"context"
	"fmt"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
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
		row_count is not null
	`

func (e *SnowflakeScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	var results []*scrapper.TableMetricsRow

	allDatabases, err := e.GetExistingDbs(ctx)
	if err != nil {
		return nil, err
	}

	existingDbs := map[string]bool{}
	for _, database := range allDatabases {
		existingDbs[database.Name] = true
	}
	for _, database := range e.conf.Databases {
		if !existingDbs[database] {
			continue
		}

		rows, err := e.executor.GetDb().QueryxContext(ctx, fmt.Sprintf(tableMetricsSql, database))
		if err != nil {
			return nil, err
		}

		for rows.Next() {
			result := scrapper.TableMetricsRow{}

			if err := rows.StructScan(&result); err != nil {
				return nil, err
			}
			result.Instance = e.conf.Account
			if result.UpdatedAt != nil {
				normalized := result.UpdatedAt.UTC()
				result.UpdatedAt = &normalized
			}
			results = append(results, &result)
		}
	}

	return results, nil
}
