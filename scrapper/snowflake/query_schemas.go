package snowflake

import (
	"context"
	"fmt"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/pkg/errors"
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
	if e.conf.CatalogFromAccountUsage {
		return e.querySchemasFromAccountUsage(origCtx)
	}
	databasesToQuery, err := e.GetDatabasesToQuery(origCtx)
	if err != nil {
		return nil, err
	}

	return readDatabases(
		origCtx, databasesToQuery, 8, scrapper.OmittedObjects,
		func(groupCtx context.Context, database string) ([]*scrapper.SchemaRow, error) {
			query := scope.AppendSchemaScopeConditions(groupCtx, fmt.Sprintf(schemasQuery, database), "s.catalog_name", "s.schema_name")
			rows, err := e.executor.QueryRows(groupCtx, query)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to query schemas for database %s", database)
			}
			defer rows.Close()

			tmpResults, err := scrapper.ScanAll[scrapper.SchemaRow](
				groupCtx, rows, fmt.Sprintf("%s.information_schema.schemata", database),
			)
			if err != nil {
				return nil, err
			}
			for _, result := range tmpResults {
				result.Instance = e.conf.Account
				if result.Description != nil && *result.Description == "" {
					result.Description = nil
				}
			}
			return tmpResults, nil
		},
	)
}
