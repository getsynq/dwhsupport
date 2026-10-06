package stdsql_test

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func pingDb(t *testing.T) (*sqlx.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true), sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return sqlx.NewDb(db, "sqlmock"), mock
}

func TestPingWithoutQueryContextIsTheDriversPing(t *testing.T) {
	db, mock := pingDb(t)
	mock.ExpectPing()

	require.NoError(t, stdsql.Ping(context.Background(), db))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPingWithQueryContextRunsACommentedSelect(t *testing.T) {
	db, mock := pingDb(t)
	ctx := querycontext.WithQueryContext(context.Background(), querycontext.QueryContext{"source": "synq"})
	mock.ExpectQuery(`SELECT 1 /* {"source":"synq"} */`).WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))

	require.NoError(t, stdsql.Ping(ctx, db))
	require.NoError(t, mock.ExpectationsWereMet())
}

// An empty query context has no comment to carry, so the check is a plain SELECT 1.
func TestPingWithEmptyQueryContextRunsAPlainSelect(t *testing.T) {
	db, mock := pingDb(t)
	ctx := querycontext.WithQueryContext(context.Background(), querycontext.QueryContext{})
	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))

	require.NoError(t, stdsql.Ping(ctx, db))
	require.NoError(t, mock.ExpectationsWereMet())
}

// The warehouse's own error comes back, not a driver's generic bad connection.
func TestPingWithQueryContextReturnsTheQueryError(t *testing.T) {
	db, mock := pingDb(t)
	ctx := querycontext.WithQueryContext(context.Background(), querycontext.QueryContext{"source": "synq"})
	refused := errors.New("access denied")
	mock.ExpectQuery(`SELECT 1 /* {"source":"synq"} */`).WillReturnError(refused)

	require.ErrorIs(t, stdsql.Ping(ctx, db), refused)
	require.NoError(t, mock.ExpectationsWereMet())
}
