package oracle

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	dwhexecoracle "github.com/getsynq/dwhsupport/exec/oracle"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeOracle is a scrapper over sqlmock, whose expectations match a statement
// that contains their text.
func fakeOracle(t *testing.T) (*OracleScrapper, sqlmock.Sqlmock) {
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
	conf := &OracleScrapperConf{OracleConf: dwhexecoracle.OracleConf{Host: "db-host", ServiceName: "FREEPDB1"}}
	return &OracleScrapper{conf: conf, executor: dwhexecoracle.NewOracleExecutorFromDb(&conf.OracleConf, sqlx.NewDb(db, "sqlmock"))}, mock
}

func expectOracleViews(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("ALL_VIEWS").WillReturnRows(
		sqlmock.NewRows([]string{"schema", "table", "is_view", "is_materialized_view", "sql"}).
			AddRow("APP", "ACTIVE", 1, 0, "SELECT 1 FROM DUAL"))
}

// expectOracleTables lists the tables as "OWNER.TABLE".
func expectOracleTables(mock sqlmock.Sqlmock, names ...string) {
	rows := sqlmock.NewRows([]string{"schema", "table"})
	for _, name := range names {
		owner, table, _ := strings.Cut(name, ".")
		rows.AddRow(owner, table)
	}
	mock.ExpectQuery("FROM\n    ALL_TABLES t").WillReturnRows(rows)
}

func expectOracleSession(mock sqlmock.Sqlmock, user string, catalogRole bool) {
	role := 0
	if catalogRole {
		role = 1
	}
	mock.ExpectQuery("SESSION_ROLES").WillReturnRows(sqlmock.NewRows([]string{"user", "catalog_role"}).AddRow(user, role))
}

func expectGetDdl(mock sqlmock.Sqlmock, name string, err error) {
	owner, table, _ := strings.Cut(name, ".")
	q := mock.ExpectQuery("DBMS_METADATA.GET_DDL('TABLE', p_table, p_owner)").WithArgs(owner, table)
	if err != nil {
		q.WillReturnError(err)
		return
	}
	q.WillReturnRows(sqlmock.NewRows([]string{"sql"}).AddRow("\n  CREATE TABLE \"" + owner + "\".\"" + table + "\" (\"ID\" NUMBER)\n"))
}

func TestOracleSqlDefinitionsReadEveryTableWithTheCatalogRole(t *testing.T) {
	sc, mock := fakeOracle(t)
	expectOracleViews(mock)
	expectOracleTables(mock, "APP.ORDERS", "SYNQ.OWN")
	expectOracleSession(mock, "SYNQ", true)
	expectGetDdl(mock, "APP.ORDERS", nil)
	expectGetDdl(mock, "SYNQ.OWN", nil)

	rows, err := sc.QuerySqlDefinitions(scrapper.WithIncompleteResults(context.Background()))
	require.NoError(t, err)
	byName := oracleDefinitionsByName(rows)
	require.Len(t, byName, 3)
	assert.True(t, byName["APP.ACTIVE"].IsView)
	for _, name := range []string{"APP.ORDERS", "SYNQ.OWN"} {
		row := byName[name]
		owner, table, _ := strings.Cut(name, ".")
		assert.Equal(t, `CREATE TABLE "`+owner+`"."`+table+`" ("ID" NUMBER)`, row.Sql, "the DDL is trimmed")
		assert.True(t, row.IsTable)
		assert.False(t, row.IsView)
		assert.Equal(t, "FREEPDB1", row.Database)
		assert.Equal(t, "db-host", row.Instance)
	}
}

// Without SELECT_CATALOG_ROLE GET_DDL shows only the user's own tables, so the
// other schemas are not asked for at all and are each one omission.
func TestOracleSqlDefinitionsWithoutTheCatalogRole(t *testing.T) {
	setup := func(t *testing.T) *OracleScrapper {
		sc, mock := fakeOracle(t)
		expectOracleViews(mock)
		expectOracleTables(mock, "APP.ORDERS", "APP.ITEMS", "HR.PEOPLE", "SYNQ.OWN")
		expectOracleSession(mock, "SYNQ", false)
		expectGetDdl(mock, "SYNQ.OWN", nil)
		return sc
	}

	t.Run("caller accepts incomplete results", func(t *testing.T) {
		rows, err := setup(t).QuerySqlDefinitions(scrapper.WithIncompleteResults(context.Background()))
		incomplete, ok := scrapper.AsIncomplete(err)
		require.True(t, ok, "%v", err)
		assert.Equal(t, []scrapper.Omission{
			{Database: "FREEPDB1", Schema: "APP", Part: scrapper.OmittedDefinitions, Err: errNoCatalogRole},
			{Database: "FREEPDB1", Schema: "HR", Part: scrapper.OmittedDefinitions, Err: errNoCatalogRole},
		}, incomplete.Omissions)
		assert.ElementsMatch(t, []string{"APP.ACTIVE", "SYNQ.OWN"}, oracleKeys(oracleDefinitionsByName(rows)))
	})

	t.Run("caller does not", func(t *testing.T) {
		rows, err := setup(t).QuerySqlDefinitions(context.Background())
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"APP.ACTIVE", "SYNQ.OWN"}, oracleKeys(oracleDefinitionsByName(rows)))
	})
}

// A table dropped after the listing answers ORA-31603 and is left out quietly;
// any other failure is an omission of that table.
func TestOracleSqlDefinitionsOfTablesGetDdlFailsFor(t *testing.T) {
	failed := errors.New("ORA-03113: end-of-file on communication channel")
	sc, mock := fakeOracle(t)
	expectOracleViews(mock)
	expectOracleTables(mock, "APP.DROPPED", "APP.BROKEN", "APP.KEPT")
	expectOracleSession(mock, "SYNQ", true)
	expectGetDdl(mock, "APP.DROPPED", errors.New(`ORA-31603: object "DROPPED" of type TABLE not found in schema "APP"`))
	expectGetDdl(mock, "APP.BROKEN", failed)
	expectGetDdl(mock, "APP.KEPT", nil)

	rows, err := sc.QuerySqlDefinitions(scrapper.WithIncompleteResults(context.Background()))
	incomplete, ok := scrapper.AsIncomplete(err)
	require.True(t, ok, "%v", err)
	assert.Equal(t, []scrapper.Omission{
		{Database: "FREEPDB1", Schema: "APP", Table: "BROKEN", Part: scrapper.OmittedDefinitions, Err: failed},
	}, incomplete.Omissions)
	assert.ElementsMatch(t, []string{"APP.ACTIVE", "APP.KEPT"}, oracleKeys(oracleDefinitionsByName(rows)))
}

// With no table in scope, the session is not read.
func TestOracleSqlDefinitionsWithoutTables(t *testing.T) {
	sc, mock := fakeOracle(t)
	expectOracleViews(mock)
	expectOracleTables(mock)

	rows, err := sc.QuerySqlDefinitions(context.Background())
	require.NoError(t, err)
	assert.Len(t, rows, 1)
}

func TestOracleSqlDefinitionsFailWhenTheSessionCannotBeRead(t *testing.T) {
	sc, mock := fakeOracle(t)
	expectOracleViews(mock)
	expectOracleTables(mock, "APP.ORDERS")
	mock.ExpectQuery("SESSION_ROLES").WillReturnError(errors.New("ORA-03113: end-of-file on communication channel"))

	_, err := sc.QuerySqlDefinitions(scrapper.WithIncompleteResults(context.Background()))
	require.Error(t, err)
	_, incomplete := scrapper.AsIncomplete(err)
	assert.False(t, incomplete)
}
