package databricks

import (
	"context"
	"fmt"

	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/xxjwxc/gowp/workpool"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *DatabricksScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	log := logging.GetLogger(ctx)
	var sqlDefs []*scrapper.SqlDefinitionRow

	tablesFound := 0

	catalogs, err := e.client.Catalogs.ListAll(ctx, servicecatalog.ListCatalogsRequest{})
	if err != nil {
		return nil, err
	}
	for _, catalogInfo := range catalogs {
		if e.isIgnoredCatalog(catalogInfo) {
			continue
		}
		if e.blocklist.IsBlocked(catalogInfo.FullName) {
			log.Infof("catalog %s excluded by blocklist", catalogInfo.FullName)
			continue
		}

		schemas, err := e.client.Schemas.ListAll(ctx, servicecatalog.ListSchemasRequest{CatalogName: catalogInfo.Name})
		if err != nil {
			return nil, err
		}
		for _, schemaInfo := range schemas {
			if schemaInfo.Name == "information_schema" {
				continue
			}
			if e.blocklist.IsBlocked(schemaInfo.FullName) {
				log.Infof("schema %s excluded by blocklist", schemaInfo.FullName)
				continue
			}

			tables, err := e.client.Tables.ListAll(ctx, servicecatalog.ListTablesRequest{CatalogName: catalogInfo.Name, SchemaName: schemaInfo.Name, OmitColumns: true, OmitProperties: true})
			if err != nil {
				return nil, err
			}

			tablesFound += len(tables)
			log.Infof("Found %d tables in catalog '%s' schema '%s', %d total", len(tables), catalogInfo.Name, schemaInfo.Name, tablesFound)

			for _, tableInfo := range tables {
				if e.blocklist.IsBlocked(tableInfo.FullName) {
					log.Infof("table %s excluded by blocklist", tableInfo.FullName)
					continue
				}
				sqlDefs = append(sqlDefs, &scrapper.SqlDefinitionRow{
					Instance: e.conf.WorkspaceUrl,
					Database: tableInfo.CatalogName,
					Schema:   tableInfo.SchemaName,
					Table:    tableInfo.Name,
					Sql:      tableInfo.ViewDefinition,
					IsView:   tableInfo.TableType == servicecatalog.TableTypeMaterializedView || tableInfo.TableType == servicecatalog.TableTypeView,
				})
			}
		}
	}

	if e.conf.UseShowCreateTable {
		log.Infof("Found %d tables in total, fetching SqlDefinitions for them using SHOW CREATE TABLE", tablesFound)

		pool := workpool.New(32)

		sqlClient, err := e.lazyExecutor.Get()
		if err != nil {
			return nil, err
		}

		for _, sqlDef := range sqlDefs {
			sqlDef := sqlDef
			pool.Do(func() error {
				if !sqlDef.IsView {

					if sql, err := e.showCreateTable(ctx, sqlClient, sqlDef.Database, sqlDef.Schema, sqlDef.Table); err == nil {
						sqlDef.Sql = sql
					}
				}
				return nil
			})
		}
		err = pool.Wait()
		if err != nil {
			return nil, err
		}
	}

	return sqlDefs, nil
}

func (e *DatabricksScrapper) showCreateTable(ctx context.Context, sqlClient *dwhexecdatabricks.DatabricksExecutor, catalog string, schema string, table string) (string, error) {
	var res []string
	sql := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`.`%s`", catalog, schema, table)
	var err = sqlClient.GetDb().SelectContext(ctx, &res, sql)
	if len(res) > 0 {
		return res[0], err
	}
	return "", err
}
