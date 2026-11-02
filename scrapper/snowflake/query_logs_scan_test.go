package snowflake

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// QUERY_HISTORY has rows whose QUERY_TAG is NULL, not an empty string. One such row must not fail
// the whole window it is in.
func TestFetchedRowWithNullQueryTag(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	endTime := time.Date(2026, 10, 14, 10, 50, 0, 0, time.UTC)
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"QUERY_ID", "QUERY_TEXT", "QUERY_TAG", "EXECUTION_STATUS", "END_TIME"}).
			AddRow("q-null", "SELECT 1", nil, "SUCCESS", endTime).
			AddRow("q-tagged", "SELECT 2", "dbt", "SUCCESS", endTime),
	)
	rows, err := sqlx.NewDb(db, "sqlmock").QueryxContext(context.Background(), "SELECT")
	require.NoError(t, err)

	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)
	it := querylogs.NewSqlxRowsIterator[SnowflakeQueryLogSchema](rows, obfuscator, "snowflake",
		func(row *SnowflakeQueryLogSchema, obfuscator querylogs.QueryObfuscator, sqlDialect string) (*querylogs.QueryLog, error) {
			return convertSnowflakeRowToQueryLog(row, obfuscator, sqlDialect, "account")
		})
	defer it.Close()

	first, err := it.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, "q-null", first.QueryID)
	require.Empty(t, first.Metadata.GetFields()["query_tag"].GetStringValue())

	second, err := it.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, "dbt", second.Metadata.GetFields()["query_tag"].GetStringValue())
}
