package snowflake

import (
	"context"
	"fmt"
	"strings"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/xxjwxc/gowp/workpool"
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

func (e *SnowflakeScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	var results []*scrapper.SqlDefinitionRow

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

		rows, err := e.executor.GetDb().QueryxContext(ctx, fmt.Sprintf(sqlDefinitionsQuery, database))
		if err != nil {
			return nil, err
		}

		for rows.Next() {
			result := scrapper.SqlDefinitionRow{}
			if err := rows.StructScan(&result); err != nil {
				return nil, err
			}
			result.Instance = e.conf.Account
			results = append(results, &result)
		}
		streamRows, err := e.showStreamsInDatabase(ctx, database)
		if err == nil {
			for _, streamRow := range streamRows {

				results = append(results, &scrapper.SqlDefinitionRow{
					Instance:           e.conf.Account,
					Database:           streamRow.DatabaseName,
					Schema:             streamRow.SchemaName,
					Table:              streamRow.Name,
					IsView:             false,
					IsMaterializedView: false,
					Sql:                fmt.Sprintf("SELECT * FROM %s", streamRow.TableName),
				})

			}
		} else {
			logging.GetLogger(ctx).WithField("database", database).WithError(err).Error("SHOW STREAMS IN DATABASE failed")
		}
	}

	ignoreDbDdls := map[string]bool{}
	for _, db := range allDatabases {
		ignoreDbDdls[db.Name] = e.conf.NoGetDll || db.Kind == "IMPORTED DATABASE"
	}

	if len(results) > 0 {
		pool := workpool.New(8)
		for _, sqlDef := range results {
			sqlDef := sqlDef
			if sqlDef.Sql != "" {
				continue
			}
			if ignoreDbDdls[sqlDef.Database] {
				continue
			}
			pool.Do(func() error {
				if sqlDef.IsView {
					if sql, err := e.getDdl(ctx, "VIEW", sqlDef.Database, sqlDef.Schema, sqlDef.Table); err == nil {
						sqlDef.Sql = sql
					}
				} else {
					if sql, err := e.getDdl(ctx, "TABLE", sqlDef.Database, sqlDef.Schema, sqlDef.Table); err == nil {
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

	return results, nil
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
