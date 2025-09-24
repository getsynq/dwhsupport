package snowflake

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	"github.com/xxjwxc/gowp/workpool"
	"golang.org/x/sync/errgroup"
)

var sqlDefinitionsQuery = `
SELECT table_catalog as "database",
table_schema as "schema",
table_name as "table",
true as "is_view",
NVL2(view_definition,view_definition,'') as "sql"

FROM %[1]s.information_schema.views
where UPPER(table_schema) NOT IN ('INFORMATION_SCHEMA', 'SYSADMIN')
UNION ALL
SELECT table_catalog as "database",
table_schema as "schema",
table_name as "table",
false as "is_view",
'' as "sql"

FROM %[1]s.information_schema.tables
where UPPER(table_schema) NOT IN ('INFORMATION_SCHEMA', 'SYSADMIN')
AND table_type !='VIEW' 
AND table_type !='MATERIALIZED VIEW'
`

func (e *SnowflakeScrapper) QuerySqlDefinitions(origCtx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	var finalResults []*scrapper.SqlDefinitionRow
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

				var tmpResults []*scrapper.SqlDefinitionRow

				rows, err := e.executor.GetDb().QueryxContext(ctx, fmt.Sprintf(sqlDefinitionsQuery, database))
				if err != nil {
					return errors.Wrapf(err, "failed to query sql definitions for database %s", database)
				}
				defer rows.Close()

				for rows.Next() {
					result := scrapper.SqlDefinitionRow{}
					if err := rows.StructScan(&result); err != nil {
						return errors.Wrapf(err, "failed to scan sql definition row for database %s", database)
					}
					result.Instance = e.conf.Account
					tmpResults = append(tmpResults, &result)
				}

				streamRows, err := e.showStreamsInDatabase(ctx, database)
				if err == nil {
					for _, streamRow := range streamRows {

						tmpResults = append(
							tmpResults, &scrapper.SqlDefinitionRow{
								Instance:           e.conf.Account,
								Database:           streamRow.DatabaseName,
								Schema:             streamRow.SchemaName,
								Table:              streamRow.Name,
								IsView:             false,
								IsMaterializedView: false,
								Sql:                fmt.Sprintf("SELECT * FROM %s", streamRow.TableName),
							},
						)
					}
				} else {
					logging.GetLogger(origCtx).WithField("database", database).WithError(err).Error("SHOW STREAMS IN DATABASE failed")
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

	ignoreDbDdls := map[string]bool{}
	for _, db := range allDatabases {
		ignoreDbDdls[db.Name] = e.conf.NoGetDll || db.Kind == "IMPORTED DATABASE"
	}

	if len(finalResults) > 0 {
		pool := workpool.New(8)
		for _, sqlDef := range finalResults {
			sqlDef := sqlDef
			if sqlDef.Sql != "" {
				continue
			}
			if ignoreDbDdls[sqlDef.Database] {
				continue
			}
			pool.Do(
				func() error {
					if sqlDef.IsView {
						if sql, err := e.getDdl(origCtx, "VIEW", sqlDef.Database, sqlDef.Schema, sqlDef.Table); err == nil {
							sqlDef.Sql = sql
						}
					} else {
						if sql, err := e.getDdl(origCtx, "TABLE", sqlDef.Database, sqlDef.Schema, sqlDef.Table); err == nil {
							sqlDef.Sql = sql
						}
					}
					return nil
				},
			)
		}

		err = pool.Wait()
		if err != nil {
			return nil, err
		}
	}

	return finalResults, nil
}

func (e *SnowflakeScrapper) getDdl(ctx context.Context, kind string, database string, schema string, table string) (string, error) {
	var res []string
	var err = e.executor.GetDb().SelectContext(ctx, &res, fmt.Sprintf("SELECT GET_DDL('%s', '%s.%s.%s', TRUE)", kind, database, schema, table))
	if len(res) > 0 {
		return fixDdl(res[0]), nil
	}
	return "", err
}

var ddlReplacer = strings.NewReplacer(
	"#UNKNOWN_POLICY", "UNKNOWN_POLICY",
	"#unknown_policy", "unknown_policy",
	"#UNKNOWN_TAG", "UNKNOWN_TAG",
	"#unknown_tag", "unknown_tag",
)

func fixDdl(s string) string {
	return ddlReplacer.Replace(s)
}
