package redshift

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	dwhexecredshift "github.com/getsynq/dwhsupport/exec/redshift"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRedshift is a scrapper over sqlmock, whose expectations match a
// statement that contains their text.
func fakeRedshift(t *testing.T, useShowCreateTable bool) (*RedshiftScrapper, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(func(expected, actual string) error {
		if !strings.Contains(actual, expected) {
			return errors.Errorf("%q does not contain %q", actual, expected)
		}
		return nil
	})))
	require.NoError(t, err)
	mock.MatchExpectationsInOrder(false)
	t.Cleanup(func() {
		assert.NoError(t, mock.ExpectationsWereMet())
		_ = db.Close()
	})
	conf := &RedshiftScrapperConf{
		RedshiftConf:       dwhexecredshift.RedshiftConf{Host: "cluster", Database: "dev"},
		UseShowCreateTable: useShowCreateTable,
	}
	return &RedshiftScrapper{conf: conf, executor: dwhexecredshift.NewRedshiftExecutorFromDb(&conf.RedshiftConf, sqlx.NewDb(db, "sqlmock"))}, mock
}

func expectViews(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("pg_get_viewdef").WillReturnRows(sqlmock.NewRows([]string{"database", "schema", "table", "sql"}).
		AddRow("dev", "public", "v", "SELECT 1"))
}

func expectTables(mock sqlmock.Sqlmock, names ...string) {
	rows := sqlmock.NewRows([]string{"schema", "table"})
	for _, name := range names {
		rows.AddRow("public", name)
	}
	mock.ExpectQuery("c.relkind = 'r'").WillReturnRows(rows)
}

func expectShowTable(mock sqlmock.Sqlmock, name string, err error) {
	q := mock.ExpectQuery(`SHOW TABLE "public"."` + name + `"`)
	if err != nil {
		q.WillReturnError(err)
		return
	}
	q.WillReturnRows(sqlmock.NewRows([]string{"ddl"}).AddRow("CREATE TABLE public." + name + " (id bigint);"))
}

func definitionsByName(rows []*scrapper.SqlDefinitionRow) map[string]*scrapper.SqlDefinitionRow {
	byName := map[string]*scrapper.SqlDefinitionRow{}
	for _, row := range rows {
		byName[row.Table] = row
	}
	return byName
}

func TestRedshiftSqlDefinitionsWithoutShowTableReadOnlyViews(t *testing.T) {
	sc, mock := fakeRedshift(t, false)
	expectViews(mock)

	rows, err := sc.QuerySqlDefinitions(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "v", rows[0].Table)
}

func TestRedshiftSqlDefinitionsReadEachTableWithShowTable(t *testing.T) {
	sc, mock := fakeRedshift(t, true)
	expectViews(mock)
	expectTables(mock, "orders", "customers")
	expectShowTable(mock, "orders", nil)
	expectShowTable(mock, "customers", nil)

	rows, err := sc.QuerySqlDefinitions(context.Background())
	require.NoError(t, err)
	byName := definitionsByName(rows)
	require.Len(t, byName, 3)
	assert.Equal(t, "SELECT 1", byName["v"].Sql)
	for _, name := range []string{"orders", "customers"} {
		row := byName[name]
		assert.Equal(t, "CREATE TABLE public."+name+" (id bigint);", row.Sql)
		assert.True(t, row.IsTable)
		assert.False(t, row.IsView)
		assert.Equal(t, "dev", row.Database)
		assert.Equal(t, "cluster", row.Instance)
	}
}

func TestRedshiftSqlDefinitionsWithoutTables(t *testing.T) {
	sc, mock := fakeRedshift(t, true)
	expectViews(mock)
	expectTables(mock)

	rows, err := sc.QuerySqlDefinitions(context.Background())
	require.NoError(t, err)
	assert.Len(t, rows, 1)
}

// A table dropped between the listing and its SHOW TABLE is left out quietly,
// one SHOW TABLE refuses is an omission for a caller that accepts incomplete
// results, and the views and other tables are returned either way.
func TestRedshiftSqlDefinitionsOfTablesShowTableFailsFor(t *testing.T) {
	refused := &pq.Error{Code: "42501", Message: "permission denied for relation secret"}
	setup := func(t *testing.T) *RedshiftScrapper {
		sc, mock := fakeRedshift(t, true)
		expectViews(mock)
		expectTables(mock, "dropped", "renamed", "secret", "kept")
		expectShowTable(mock, "dropped", errors.New("pq: Could not find relation 107794"))
		expectShowTable(mock, "renamed", &pq.Error{Code: "42P01", Message: `relation "public.renamed" does not exist`})
		expectShowTable(mock, "secret", refused)
		expectShowTable(mock, "kept", nil)
		return sc
	}

	t.Run("caller accepts incomplete results", func(t *testing.T) {
		rows, err := setup(t).QuerySqlDefinitions(scrapper.WithIncompleteResults(context.Background()))
		incomplete, ok := scrapper.AsIncomplete(err)
		require.True(t, ok, "%v", err)
		assert.Equal(t, []scrapper.Omission{{
			Database: "dev", Schema: "public", Table: "secret", Part: scrapper.OmittedDefinitions, Err: refused,
		}}, incomplete.Omissions)
		assert.ElementsMatch(t, []string{"v", "kept"}, keysOf(definitionsByName(rows)))
	})

	t.Run("caller does not", func(t *testing.T) {
		rows, err := setup(t).QuerySqlDefinitions(context.Background())
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"v", "kept"}, keysOf(definitionsByName(rows)))
	})
}

func TestRedshiftSqlDefinitionsFailWhenTheTablesCannotBeListed(t *testing.T) {
	sc, mock := fakeRedshift(t, true)
	expectViews(mock)
	mock.ExpectQuery("c.relkind = 'r'").WillReturnError(errors.New("pq: connection reset"))

	_, err := sc.QuerySqlDefinitions(scrapper.WithIncompleteResults(context.Background()))
	require.Error(t, err)
	_, incomplete := scrapper.AsIncomplete(err)
	assert.False(t, incomplete)
}

func TestShowTableStatementQuotesEachPart(t *testing.T) {
	for _, tc := range []struct{ schema, table, want string }{
		{"public", "orders", `SHOW TABLE "public"."orders"`},
		{"my schema", "order", `SHOW TABLE "my schema"."order"`},
		{"s", `we"ird.name`, `SHOW TABLE "s"."we""ird.name"`},
	} {
		got, err := showTableStatement(tc.schema, tc.table)
		require.NoError(t, err)
		assert.Equal(t, tc.want, got)
	}
}

func keysOf[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
