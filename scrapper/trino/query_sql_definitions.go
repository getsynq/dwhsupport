package trino

import (
	"context"
	_ "embed"
	"fmt"
	"strings"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/xxjwxc/gowp/workpool"
)

//go:embed query_sql_definitions.sql
var querySqlDefinitionsSQL string

func (e *TrinoScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	basic, err := e.getBasicSqlDefinitions(ctx)
	if err != nil {
		return nil, err
	}

	if len(basic) > 0 && (e.conf.UseShowCreateTable || e.conf.UseShowCreateView) {
		pool := workpool.New(8)
		for _, sqlDef := range basic {
			currentSqlDef := sqlDef // Create a local copy of the loop variable
			if (currentSqlDef.IsView && e.conf.UseShowCreateView) || (!currentSqlDef.IsView && e.conf.UseShowCreateTable) {
				pool.Do(func() error {
					sql, err := e.showCreate(ctx, currentSqlDef.Database, currentSqlDef.Schema, currentSqlDef.Table, currentSqlDef.IsView, currentSqlDef.IsMaterializedView)
					if err != nil {
						return err
					}
					currentSqlDef.Sql = sql

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
	if len(res) > 0 {
		return res[0], nil
	}
	return "", err
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
