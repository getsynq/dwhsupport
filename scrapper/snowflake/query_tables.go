package snowflake

import (
	"context"
	"fmt"
	"github.com/getsynq/dwhsupport/scrapper"
)

var tablesQuery = `
	select 
		t.table_catalog as "database",
        t.table_schema as "schema",
        t.table_name as "table",
        t.table_type as "table_type",
        NVL2(t.comment, t.comment, '') as "description"
	from  
		%s.information_schema.tables as t
	where 
		table_type='BASE TABLE'
	AND UPPER(t.table_schema) NOT IN ('INFORMATION_SCHEMA', 'SYSADMIN')
	`

func (e *SnowflakeScrapper) QueryTables(ctx context.Context) ([]*scrapper.TableRow, error) {
	var results []*scrapper.TableRow

	allDatabases, err := e.allAllowedDatabases(ctx)
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
		rows, err := e.executor.GetDb().QueryxContext(ctx, fmt.Sprintf(tablesQuery, database))
		if err != nil {
			return nil, err
		}

		for rows.Next() {
			result := scrapper.TableRow{}
			if err := rows.StructScan(&result); err != nil {
				return nil, err
			}
			result.Instance = e.conf.Account
			results = append(results, &result)
		}
	}

	return results, nil
}
