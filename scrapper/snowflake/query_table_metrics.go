package snowflake

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
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
	`

func (e *SnowflakeScrapper) QueryTableMetrics(origCtx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	var finalResults []*scrapper.TableMetricsRow
	var m sync.Mutex

	allDatabases, err := e.GetExistingDbs(origCtx)
	if err != nil {
		return nil, err
	}

	existingDbs := map[string]bool{}
	for _, database := range allDatabases {
		existingDbs[database.Name] = true
	}

	g, ctx := errgroup.WithContext(origCtx)
	g.SetLimit(8)

	for _, database := range e.conf.Databases {
		database := database
		if !existingDbs[database] {
			continue
		}

		g.Go(
			func() error {

				var tmpResults []*scrapper.TableMetricsRow

				rows, err := e.executor.GetDb().QueryxContext(ctx, fmt.Sprintf(tableMetricsSql, database))
				if err != nil {
					return errors.Wrapf(err, "failed to query metrics for database %s", database)
				}
				defer rows.Close()

				for rows.Next() {
					result := scrapper.TableMetricsRow{}

					if err := rows.StructScan(&result); err != nil {
						return errors.Wrapf(err, "failed to scan metrics row for database %s", database)
					}
					result.Instance = e.conf.Account
					if result.UpdatedAt != nil {
						normalized := result.UpdatedAt.UTC()
						result.UpdatedAt = &normalized
					}
					tmpResults = append(tmpResults, &result)
				}

				m.Lock()
				defer m.Unlock()
				finalResults = append(finalResults, tmpResults...)
				return nil
			},
		)
	}

	err = g.Wait()
	if err != nil {
		return nil, err
	}

	return finalResults, nil
}
