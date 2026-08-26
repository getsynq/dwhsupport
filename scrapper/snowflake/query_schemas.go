package snowflake

import (
	"context"
	"fmt"
	"sync"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

var schemasQuery = `
	select
		s.catalog_name as "database",
		s.schema_name as "schema",
		NVL2(s.comment, s.comment, '') as "description",
		s.schema_owner as "schema_owner"
	from
		%s.information_schema.schemata as s
	where
		UPPER(s.schema_name) NOT IN ('INFORMATION_SCHEMA')
		/* SYNQ_SCOPE_FILTER */
	`

func (e *SnowflakeScrapper) QuerySchemas(origCtx context.Context) ([]*scrapper.SchemaRow, error) {
	var finalResults []*scrapper.SchemaRow
	var m sync.Mutex

	databasesToQuery, err := e.GetDatabasesToQuery(origCtx)
	if err != nil {
		return nil, err
	}

	g, groupCtx := errgroup.WithContext(origCtx)
	g.SetLimit(8)

	for _, database := range databasesToQuery {
		select {
		case <-groupCtx.Done():
			return nil, groupCtx.Err()
		default:
		}

		g.Go(func() error {
			query := scope.AppendSchemaScopeConditions(origCtx, fmt.Sprintf(schemasQuery, database), "s.catalog_name", "s.schema_name")
			rows, err := e.executor.QueryRows(groupCtx, query)
			if err != nil {
				if isSharedDatabaseUnavailableError(err) {
					logging.GetLogger(groupCtx).WithField("database", database).WithError(err).
						Warn("Shared database is no longer available, skipping")
					return nil
				}
				return errors.Wrapf(err, "failed to query schemas for database %s", database)
			}
			defer rows.Close()

			tmpResults, err := scrapper.ScanAll[scrapper.SchemaRow](
				groupCtx, rows, fmt.Sprintf("%s.information_schema.schemata", database),
			)
			if err != nil {
				return err
			}
			for _, result := range tmpResults {
				result.Instance = e.conf.Account
				if result.Description != nil && *result.Description == "" {
					result.Description = nil
				}
			}

			m.Lock()
			finalResults = append(finalResults, tmpResults...)
			m.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return finalResults, nil
}
