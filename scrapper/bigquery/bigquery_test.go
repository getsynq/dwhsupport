package bigquery

import (
	"testing"
	"time"

	"github.com/samber/lo"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type BigQuerySuite struct {
	suite.Suite
}

func TestBigQuerySuite(t *testing.T) {
	suite.Run(t, new(BigQuerySuite))
}

func (s *BigQuerySuite) TestShardedTables() {

	tableIds := []string{
		"users_20240102",
		"users_20240101",
		"users_20240103",
		"actions20240102",
		"actions20240101",
		// date outside of valid shart range
		"actions20900101",
		"accounts",
		"transactions",
		"random_46428353",
		"sqlmesh_example__incremental_model__2565192088",
	}

	shardedTables := getShardedTables(
		tableIds,
		time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2054, 1, 3, 0, 0, 0, 0, time.UTC),
	)

	require.Contains(s.T(), shardedTables, "users_")
	require.Contains(s.T(), shardedTables["users_"].TableName, "users")
	require.Contains(s.T(), shardedTables["users_"].Shards, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	require.Contains(s.T(), shardedTables["users_"].Shards, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC))
	require.Contains(s.T(), shardedTables["users_"].Shards, time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC))
	require.Contains(s.T(), shardedTables["users_"].LatestShardId, "users_20240103")

	require.Contains(s.T(), shardedTables, "actions")

	require.NotContains(s.T(), shardedTables, "random_46428353")
	require.NotContains(s.T(), shardedTables, "accounts")
	require.NotContains(s.T(), shardedTables, "transactions")
	require.NotContains(s.T(), shardedTables, "sqlmesh_example__incremental_model__2565192088")
}

func (s *BigQuerySuite) TestShardedMetrics() {
	s.Run("sharded_table", func() {
		metrics := []*scrapper.TableMetricsRow{
			// changed sharded table to normal table
			metricRow("test", "test", "test", "users", 5, time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)),
			metricRow("test", "test", "test", "users_20240102", 20, time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)),
			metricRow("test", "test", "test", "users_20240101", 10, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
			metricRow("test", "test", "test", "users_20240103", 30, time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)),

			metricRow("test", "test", "test", "ebury20240102", 20, time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)),
			metricRow("test", "test", "test", "ebury20240103", 30, time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)),
			metricRow("test", "test", "test", "ebury", 5, time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)),
			metricRow("test", "test", "test", "ebury20240101", 10, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),

			// changed normal table to sharded table
			metricRow("test", "test", "test", "orgs_20240104", 30, time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)),
			metricRow("test", "test", "test", "orgs_20240102", 10, time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)),
			metricRow("test", "test", "test", "orgs", 5, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
			metricRow("test", "test", "test", "orgs_20240103", 20, time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)),

			metricRow("test", "test", "test", "aiven20240102", 10, time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)),
			metricRow("test", "test", "test", "aiven", 5, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
			metricRowWithBytes("test", "test", "test", "aiven20240104", 30, 128, time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)),
			metricRowWithBytes("test", "test", "test", "aiven20240103", 20, 512, time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)),

			// just normal table
			metricRow("test", "test", "test", "accounts", 100, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
		}

		collapsed, err := collapseShardedMetrics(metrics)
		require.NoError(s.T(), err)
		require.Len(s.T(), collapsed, 5)

		require.Contains(
			s.T(),
			collapsed,
			metricRow("test", "test", "test", "users", 5, time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)),
		)
		require.Contains(
			s.T(),
			collapsed,
			metricRow("test", "test", "test", "ebury", 5, time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)),
		)
		require.Contains(
			s.T(),
			collapsed,
			metricRow("test", "test", "test", "orgs", 60, time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)),
		)
		require.Contains(
			s.T(),
			collapsed,
			metricRowWithBytes("test", "test", "test", "aiven", 60, 128+512, time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)),
		)
		require.Contains(
			s.T(),
			collapsed,
			metricRow("test", "test", "test", "accounts", 100, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
		)
	})

	s.Run("tables_across_datasets", func() {
		metrics := []*scrapper.TableMetricsRow{
			metricRow("test", "test", "test", "users_20240101", 10, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
			metricRow("test", "test", "test", "users_20240102", 20, time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)),
			metricRow("test", "test", "other", "users_20240103", 30, time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)),
		}

		collapsed, err := collapseShardedMetrics(metrics)
		require.NoError(s.T(), err)
		require.Len(s.T(), collapsed, 2)

		require.Contains(
			s.T(),
			collapsed,
			metricRow("test", "test", "test", "users", 30, time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)),
		)
		require.Contains(
			s.T(),
			collapsed,
			metricRow("test", "test", "other", "users", 30, time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)),
		)
	})

	s.Run("aiven", func() {
		metrics := []*scrapper.TableMetricsRow{
			metricRow(
				"test",
				"bq-aiven-dw-prod",
				"marketing_dw",
				"assumed_account_users",
				10,
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			),
			metricRow(
				"test",
				"bq-aiven-dw-prod",
				"sandbox",
				"assumed_account_users",
				20,
				time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
			),
		}

		collapsed, err := collapseShardedMetrics(metrics)
		require.NoError(s.T(), err)
		require.Len(s.T(), collapsed, 2)

		require.Contains(
			s.T(),
			collapsed,
			metricRow(
				"test",
				"bq-aiven-dw-prod",
				"marketing_dw",
				"assumed_account_users",
				10,
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			),
		)
		require.Contains(
			s.T(),
			collapsed,
			metricRow(
				"test",
				"bq-aiven-dw-prod",
				"sandbox",
				"assumed_account_users",
				20,
				time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
			),
		)
	})
}

func metricRow(instance, database, schema, table string, rowCount int64, updatedAt time.Time) *scrapper.TableMetricsRow {
	return &scrapper.TableMetricsRow{
		Instance: instance,
		Database: database,
		Schema:   schema,
		Table:    table,

		RowCount:  lo.ToPtr(rowCount),
		UpdatedAt: lo.ToPtr(updatedAt),
	}
}

func metricRowWithBytes(
	instance, database, schema, table string,
	rowCount int64,
	sizeBytes int64,
	updatedAt time.Time,
) *scrapper.TableMetricsRow {
	return &scrapper.TableMetricsRow{
		Instance:  instance,
		Database:  database,
		Schema:    schema,
		Table:     table,
		RowCount:  lo.ToPtr(rowCount),
		UpdatedAt: lo.ToPtr(updatedAt),
		SizeBytes: lo.ToPtr(sizeBytes),
	}
}
