package mysql

import (
	"context"
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/require"
)

// Every base table gets its SHOW CREATE TABLE and every view its definition,
// on MySQL and on MariaDB.
func TestMySQLSqlDefinitionsOfTablesAndViews(t *testing.T) {
	scrappers := map[string]func(context.Context) (*MySQLScrapper, error){
		"MySQL":   newMySQLScrapperFromEnv,
		"MariaDB": newMariaDBScrapperFromEnv,
	}
	for name, newScrapper := range scrappers {
		t.Run(name, func(t *testing.T) {
			if os.Getenv("CI") != "" {
				t.Skip("Skipping " + name + " tests in CI")
			}
			ctx := t.Context()
			sc, err := newScrapper(ctx)
			if err != nil {
				t.Skipf("%s not reachable: %v", name, err)
			}
			defer sc.Close()

			tables, err := sc.QueryTables(ctx)
			require.NoError(t, err)
			defs, err := sc.QuerySqlDefinitions(ctx)
			require.NoError(t, err)
			byName := map[string]*scrapper.SqlDefinitionRow{}
			for _, def := range defs {
				byName[def.Schema+"."+def.Table] = def
			}

			var baseTables, views int
			for _, table := range tables {
				def := byName[table.Schema+"."+table.Table]
				require.NotNil(t, def, "no definition row for %s.%s", table.Schema, table.Table)
				require.Equal(t, table.IsView, def.IsView, "%s.%s", table.Schema, table.Table)
				if table.IsView {
					views++
					require.NotEmpty(t, def.Sql, "view %s.%s has no definition", table.Schema, table.Table)
					continue
				}
				baseTables++
				require.Contains(t, def.Sql, "CREATE TABLE", "table %s.%s has no definition", table.Schema, table.Table)
				require.NotRegexp(t, `AUTO_INCREMENT=\d`, def.Sql, "the counter changes with every insert")
			}
			require.NotZero(t, baseTables, "the seed has base tables")
			require.NotZero(t, views, "the seed has views")
		})
	}
}
