package athena

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// Athena runs a DDL statement on its Hive engine, whose parser rejects a block
// comment after the statement ("missing EOF at '/'") but takes a line comment.
// Everything else runs on the Trino engine and keeps the block comment, which
// is where a query log reader looks for it.
func TestExecutorMarksAStatementTheWayItsEngineAccepts(t *testing.T) {
	ctx := querycontext.WithQueryContext(context.Background(), querycontext.QueryContext{"source": "synq"})
	cases := []struct {
		name string
		sql  string
		sent string
	}{
		{
			name: "SHOW CREATE TABLE",
			sql:  "SHOW CREATE TABLE `my_db`.`elb_logs`",
			sent: "SHOW CREATE TABLE `my_db`.`elb_logs`\n-- {\"source\":\"synq\"}",
		},
		{
			name: "DESCRIBE, lower case and indented",
			sql:  "\n  describe `my_db`.`elb_logs`;",
			sent: "\n  describe `my_db`.`elb_logs`\n-- {\"source\":\"synq\"}",
		},
		{
			name: "SHOW CREATE VIEW",
			sql:  `SHOW CREATE VIEW "my_db"."my_view"`,
			sent: "SHOW CREATE VIEW \"my_db\".\"my_view\"\n-- {\"source\":\"synq\"}",
		},
		{
			name: "a query",
			sql:  "SELECT 1",
			sent: `SELECT 1 /* {"source":"synq"} */`,
		},
		{
			name: "a query that starts with a comment",
			sql:  "-- SHOW CREATE TABLE\nSELECT 1",
			sent: "-- SHOW CREATE TABLE\nSELECT 1 /* {\"source\":\"synq\"} */",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer db.Close()
			e := &AthenaExecutor{conf: &AthenaConf{}, db: sqlx.NewDb(db, "awsathena")}

			mock.ExpectQuery(c.sent).WillReturnRows(sqlmock.NewRows([]string{"createtab_stmt"}).AddRow("CREATE EXTERNAL TABLE x"))
			var rows []string
			require.NoError(t, e.Select(ctx, &rows, c.sql))

			mock.ExpectQuery(c.sent).WillReturnRows(sqlmock.NewRows([]string{"createtab_stmt"}))
			r, err := e.QueryRows(ctx, c.sql)
			require.NoError(t, err)
			require.NoError(t, r.Close())

			mock.ExpectExec(c.sent).WillReturnResult(sqlmock.NewResult(0, 0))
			require.NoError(t, e.Exec(ctx, c.sql))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// A query context carrying a line break cannot end the line comment early:
// the JSON escapes it.
func TestExecutorLineCommentStaysOnOneLine(t *testing.T) {
	ctx := querycontext.WithQueryContext(context.Background(), querycontext.QueryContext{"note": "a\nDROP TABLE x"})
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()
	e := &AthenaExecutor{conf: &AthenaConf{}, db: sqlx.NewDb(db, "awsathena")}

	mock.ExpectQuery("SHOW CREATE TABLE t\n-- {\"note\":\"a\\nDROP TABLE x\"}").WillReturnRows(sqlmock.NewRows([]string{"createtab_stmt"}))
	var rows []string
	require.NoError(t, e.Select(ctx, &rows, "SHOW CREATE TABLE t"))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLeadingKeyword(t *testing.T) {
	cases := map[string]string{
		"SHOW CREATE TABLE t":                       "SHOW",
		"  \n\tdescribe t":                          "DESCRIBE",
		"/* SHOW */ SELECT 1":                       "SELECT",
		"-- a\n-- b\n/* c */\n MSCK REPAIR TABLE t": "MSCK",
		"(SELECT 1)":                                "",
		"-- only a comment":                         "",
		"/* unterminated":                           "",
		"":                                          "",
	}
	for sql, keyword := range cases {
		require.Equal(t, keyword, leadingKeyword(sql), "%q", sql)
	}
}
