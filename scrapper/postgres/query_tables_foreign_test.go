package postgres

import (
	"os"
	"testing"

	dwhexecpostgres "github.com/getsynq/dwhsupport/exec/postgres"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestTableRowForeignOf(t *testing.T) {
	for _, tc := range []struct {
		name string
		row  tableRow
		want *scrapper.TableForeignOf
	}{
		{
			name: "not a foreign table",
			row:  tableRow{TableRow: scrapper.TableRow{Schema: "public", Table: "orders"}},
		},
		{
			name: "postgres_fdw with every option",
			row: tableRow{
				TableRow:        scrapper.TableRow{Schema: "imported", Table: "orders"},
				ForeignWrapper:  lo.ToPtr("postgres_fdw"),
				ForeignServer:   lo.ToPtr("billing"),
				ForeignHost:     lo.ToPtr("10.0.0.5"),
				ForeignPort:     lo.ToPtr("6432"),
				ForeignDatabase: lo.ToPtr("billing"),
				ForeignSchema:   lo.ToPtr("public"),
				ForeignTable:    lo.ToPtr("invoices"),
			},
			want: &scrapper.TableForeignOf{
				Wrapper: "postgres_fdw", Server: "billing", Host: "10.0.0.5", Port: "6432", Database: "billing", Schema: "public", Table: "invoices",
			},
		},
		{
			name: "postgres_fdw takes the foreign table's own schema and name when the options leave them out",
			row: tableRow{
				TableRow:        scrapper.TableRow{Schema: "imported", Table: "orders"},
				ForeignWrapper:  lo.ToPtr("postgres_fdw"),
				ForeignServer:   lo.ToPtr("billing"),
				ForeignHost:     lo.ToPtr("db.internal"),
				ForeignDatabase: lo.ToPtr("billing"),
			},
			want: &scrapper.TableForeignOf{
				Wrapper: "postgres_fdw", Server: "billing", Host: "db.internal", Database: "billing", Schema: "imported", Table: "orders",
			},
		},
		{
			name: "postgres_fdw over a server with no connection options",
			row: tableRow{
				TableRow:       scrapper.TableRow{Schema: "imported", Table: "orders"},
				ForeignWrapper: lo.ToPtr("postgres_fdw"),
				ForeignServer:  lo.ToPtr("loopback"),
			},
			want: &scrapper.TableForeignOf{Wrapper: "postgres_fdw", Server: "loopback", Schema: "imported", Table: "orders"},
		},
		{
			name: "another wrapper names only its wrapper and server",
			row: tableRow{
				TableRow:        scrapper.TableRow{Schema: "imported", Table: "orders"},
				ForeignWrapper:  lo.ToPtr("mysql_fdw"),
				ForeignServer:   lo.ToPtr("shop"),
				ForeignHost:     lo.ToPtr("mysql.internal"),
				ForeignDatabase: lo.ToPtr("shop"),
				ForeignTable:    lo.ToPtr("orders"),
			},
			want: &scrapper.TableForeignOf{Wrapper: "mysql_fdw", Server: "shop"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.row.foreignOf())
		})
	}
}

// A foreign table names the remote table it reads, read from a real
// postgres_fdw server. The test creates the extension, a server, its own
// schemas and a role mapping, so it needs a superuser, and skips without one.
func TestPostgresQueryTablesForeignTables(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Postgres tests in CI")
	}
	ctx := t.Context()
	sc, err := newPostgresScrapperFromEnv(ctx)
	if err != nil {
		t.Skipf("Postgres not reachable: %v", err)
	}
	defer sc.Close()

	const schema = "dwhsupport_foreign_tables_test"
	const server = "dwhsupport_foreign_tables_server"
	cleanup := func() {
		_ = sc.executor.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		_ = sc.executor.Exec(ctx, "DROP SERVER IF EXISTS "+server+" CASCADE")
	}
	cleanup()
	t.Cleanup(cleanup)
	for _, stmt := range []string{
		"CREATE EXTENSION IF NOT EXISTS postgres_fdw",
		"CREATE SERVER " + server + " FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'billing.db.internal', port '6432', dbname 'billing')",
		"CREATE SCHEMA " + schema,
		"CREATE TABLE " + schema + ".local_orders (id int)",
		"CREATE FOREIGN TABLE " + schema + ".invoices (id int) SERVER " + server,
		"CREATE FOREIGN TABLE " + schema + ".renamed (id int) SERVER " + server + " OPTIONS (schema_name 'finance', table_name 'Invoices 2024')",
	} {
		if err := sc.executor.Exec(ctx, stmt); err != nil {
			if dwhexecpostgres.IsPermissionError(err) {
				t.Skipf("the test role cannot create its fixture: %v", err)
			}
			require.NoError(t, err, stmt)
		}
	}

	tables, err := sc.QueryTables(ctx)
	require.NoError(t, err)
	byName := lo.KeyBy(
		lo.Filter(tables, func(r *scrapper.TableRow, _ int) bool { return r.Schema == schema }),
		func(r *scrapper.TableRow) string { return r.Table },
	)
	require.ElementsMatch(t, []string{"local_orders", "invoices", "renamed"}, lo.Keys(byName))

	require.Nil(t, byName["local_orders"].ForeignOf)
	require.Equal(t, "BASE TABLE", byName["invoices"].TableType, "a foreign table is still listed as a table")
	require.Equal(t, &scrapper.TableForeignOf{
		Wrapper: "postgres_fdw", Server: server, Host: "billing.db.internal", Port: "6432", Database: "billing", Schema: schema, Table: "invoices",
	}, byName["invoices"].ForeignOf)
	require.Equal(t, &scrapper.TableForeignOf{
		Wrapper:  "postgres_fdw",
		Server:   server,
		Host:     "billing.db.internal",
		Port:     "6432",
		Database: "billing",
		Schema:   "finance",
		Table:    "Invoices 2024",
	}, byName["renamed"].ForeignOf)
}
