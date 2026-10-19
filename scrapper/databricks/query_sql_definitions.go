package databricks

import (
	"context"
	"strings"
	"sync"

	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
)

func (e *DatabricksScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	log := logging.GetLogger(ctx)
	var sqlDefs []*scrapper.SqlDefinitionRow
	tablesFound := 0

	walk := e.walkUnityCatalog(ctx, scrapper.OmittedDefinitions)
	err := walk.forEachSchema(ctx, servicecatalog.ListTablesRequest{OmitColumns: true, OmitProperties: true},
		func(schemaInfo servicecatalog.SchemaInfo, tables []servicecatalog.TableInfo) error {
			tablesFound += len(tables)
			log.Infof("Found %d tables in catalog '%s' schema '%s', %d total", len(tables), schemaInfo.CatalogName, schemaInfo.Name, tablesFound)

			for _, tableInfo := range tables {
				sqlDefs = append(sqlDefs, &scrapper.SqlDefinitionRow{
					Instance: e.conf.WorkspaceUrl,
					Database: tableInfo.CatalogName,
					Schema:   tableInfo.SchemaName,
					Table:    tableInfo.Name,
					Sql:      tableInfo.ViewDefinition,
					IsView:   tableInfo.TableType == servicecatalog.TableTypeMaterializedView || tableInfo.TableType == servicecatalog.TableTypeView,
				})
			}
			return nil
		})
	if err != nil {
		return nil, err
	}

	if e.conf.UseShowCreateTable {
		log.Infof("Found %d tables in total, fetching SqlDefinitions for them using SHOW CREATE TABLE", tablesFound)

		var m sync.Mutex
		g, groupCtx := errgroup.WithContext(ctx)
		g.SetLimit(32)

		sqlClient, err := e.lazyExecutor.Get()
		if err != nil {
			return nil, err
		}

		omitted := map[*scrapper.SqlDefinitionRow]bool{}
		for _, sqlDef := range sqlDefs {
			select {
			case <-groupCtx.Done():
				return nil, groupCtx.Err()
			default:
			}

			g.Go(func() error {
				if sqlDef.IsView {
					return nil
				}
				sql, err := e.showCreateTable(groupCtx, sqlClient, sqlDef.Database, sqlDef.Schema, sqlDef.Table)
				if err == nil {
					m.Lock()
					sqlDef.Sql = sql
					m.Unlock()
					return nil
				}
				// A caller that did not ask for incomplete results gets the table
				// without a definition, as it always did.
				if !walk.acceptIncomplete || isTableWithoutCreateStatement(err) {
					log.WithError(err).Debugf("No CREATE statement for %s.%s.%s", sqlDef.Database, sqlDef.Schema, sqlDef.Table)
					return nil
				}
				omission := scrapper.Omission{
					Database: sqlDef.Database, Schema: sqlDef.Schema, Table: sqlDef.Table, Part: scrapper.OmittedDefinitions, Err: err,
				}
				if err := walk.leaveOut(groupCtx, omission); err != nil {
					return err
				}
				m.Lock()
				omitted[sqlDef] = true
				m.Unlock()
				return nil
			})
		}
		err = g.Wait()
		if err != nil {
			return nil, err
		}
		sqlDefs = lo.Reject(sqlDefs, func(sqlDef *scrapper.SqlDefinitionRow, _ int) bool { return omitted[sqlDef] })
	}

	return sqlDefs, walk.result()
}

// isTableWithoutCreateStatement reports a SHOW CREATE TABLE that failed for a reason
// no later run would fix: Databricks cannot show a CREATE for the table by design
// (UNSUPPORTED_SHOW_CREATE_TABLE, such as a Hive SerDe table), or the table was dropped
// since it was listed. Those tables keep an empty definition and are not reported.
func isTableWithoutCreateStatement(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "[UNSUPPORTED_SHOW_CREATE_TABLE") || strings.Contains(msg, "[TABLE_OR_VIEW_NOT_FOUND]")
}

func (e *DatabricksScrapper) showCreateTable(
	ctx context.Context,
	sqlClient *dwhexecdatabricks.DatabricksExecutor,
	catalog string,
	schema string,
	table string,
) (string, error) {
	sql, err := showCreateTableStatement(catalog, schema, table)
	if err != nil {
		return "", err
	}
	var res []string
	err = sqlClient.Select(ctx, &res, sql)
	if len(res) > 0 {
		return res[0], err
	}
	return "", err
}

func showCreateTableStatement(catalog, schema, table string) (string, error) {
	name, err := quotedName(catalog, schema, table)
	if err != nil {
		return "", err
	}
	return "SHOW CREATE TABLE " + name, nil
}
