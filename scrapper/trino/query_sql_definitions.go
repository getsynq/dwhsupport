package trino

import (
	"context"
	_ "embed"
	"fmt"
	"strings"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"golang.org/x/sync/errgroup"
)

//go:embed query_sql_definitions.sql
var querySqlDefinitionsSQL string

func (e *TrinoScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	basic, err := e.getBasicSqlDefinitions(ctx)
	if err != nil {
		return nil, err
	}

	if len(basic) > 0 && (e.conf.UseShowCreateTable || e.conf.UseShowCreateView) {
		pool, ctx := errgroup.WithContext(ctx)
		pool.SetLimit(8)
		for i, sqlDef := range basic {
			if i > 0 && i%100 == 0 {
				logging.GetLogger(ctx).Infof("fetched SQL definitions for %d/%d", i, len(basic))
			}

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}

			if (sqlDef.IsView && e.conf.UseShowCreateView) || (!sqlDef.IsView && e.conf.UseShowCreateTable) {
				pool.Go(func() error {
					sql, err := e.showCreate(ctx, sqlDef.Database, sqlDef.Schema, sqlDef.Table, sqlDef.IsView, sqlDef.IsMaterializedView)
					if err != nil {
						logging.GetLogger(ctx).WithField("table_fqn", sqlDef.TableFqn()).WithError(err).Warnf("error getting sql definition")
						return nil
					}
					sqlDef.Sql = sql

					return nil
				})
			}
		}

		err = pool.Wait()
		if err != nil {
			return nil, err
		}
	}

	return basic, nil
}

func (e *TrinoScrapper) showCreate(
	ctx context.Context,
	database string,
	schema string,
	table string,
	isView bool,
	isMaterializedView bool,
) (string, error) {
	entity := "TABLE"
	if isMaterializedView {
		entity = "MATERIALIZED VIEW"
	} else if isView {
		entity = "VIEW"
	}
	query := fmt.Sprintf("SHOW CREATE %s %s.%s.%s", entity, database, schema, table)
	var res []string
	err := e.executor.GetDb().SelectContext(ctx, &res, query)
	if err != nil {
		if strings.Contains(err.Error(), "is a materialized view, not a table") {
			return e.showCreate(ctx, database, schema, table, isView, true)
		}
		return "", err
	}
	if len(res) > 0 {
		return res[0], nil
	}
	return "", nil
}

func (e *TrinoScrapper) getBasicSqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	query := querySqlDefinitionsSQL
	db := e.executor.GetDb()
	var out []*scrapper.SqlDefinitionRow

	availableCatalogs, err := e.allAvailableCatalogs.Get()
	if err != nil {
		return nil, err
	}

	for _, catalog := range availableCatalogs {
		if !catalog.IsAccepted {
			continue
		}
		catalogQuery := strings.Replace(query, "{{catalog}}", catalog.CatalogName, -1)

		// Conditionally add materialized views JOIN based on feature flag
		if e.conf.FetchMaterializedViews {
			catalogQuery = strings.Replace(
				catalogQuery,
				"{{materialized_views_join}}",
				"LEFT JOIN system.metadata.materialized_views mv ON t.database = mv.catalog_name AND t.schema = mv.schema_name AND t.table_name = mv.name",
				-1,
			)
			catalogQuery = strings.Replace(catalogQuery, "{{is_materialized_view_expression}}",
				"(mv.name IS NOT NULL)", -1)
			catalogQuery = strings.Replace(catalogQuery, "{{sql_expression}}",
				"coalesce(mv.definition, v.view_definition, '')", -1)
		} else {
			catalogQuery = strings.Replace(catalogQuery, "{{materialized_views_join}}", "", -1)
			catalogQuery = strings.Replace(catalogQuery, "{{is_materialized_view_expression}}", "false", -1)
			catalogQuery = strings.Replace(catalogQuery, "{{sql_expression}}",
				"coalesce(v.view_definition, '')", -1)
		}

		res, err := stdsql.QueryMany(ctx, db, catalogQuery,
			dwhexec.WithPostProcessors(func(row *scrapper.SqlDefinitionRow) (*scrapper.SqlDefinitionRow, error) {
				row.Instance = e.conf.Host
				return row, nil
			}),
		)
		if err != nil {
			return nil, err
		}
		out = append(out, res...)
	}
	return out, nil
}
