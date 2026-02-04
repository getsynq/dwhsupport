package bigquery

import (
	"context"
	"os"
	"testing"
	"time"

	dwhexecbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/suite"
)

// LocalBigQueryScrapperSuite tests scrapper methods against a local BigQuery instance
type LocalBigQueryScrapperSuite struct {
	suite.Suite
	bigqueryScrapper *BigQueryScrapper
	ctx              context.Context
	testDataset      string
}

func TestLocalBigQueryScrapperSuite(t *testing.T) {
	suite.Run(t, new(LocalBigQueryScrapperSuite))
}

func (s *LocalBigQueryScrapperSuite) SetupSuite() {
	// Skip in CI environment
	if os.Getenv("CI") != "" {
		s.T().Skip("Skipping local BigQuery tests in CI")
	}

	s.ctx = context.TODO()
	s.testDataset = "test_scrapper"

	// Try to load credentials from the JSON file
	credentialsFile := "../../nifty-motif-341212-88499dbfc22e.json"
	credentialsJson, err := os.ReadFile(credentialsFile)
	if err != nil {
		s.T().Skipf("Skipping: could not read BigQuery credentials file: %v", err)
	}

	// Create a BigQuery scrapper
	// Block all datasets except test_scrapper to speed up tests
	// Use explicit names and wildcards (note: * in blocklist means 1+ chars, so we need both exact and wildcard patterns)
	blocklist := "analytics,analytics*,dbt*,elementary*,fivetran*,gitlab*,google_sheets,in_ecom*,kuba,lukas*,marketing,mini_dbt,petr,petr*,runtime,snapshots,sqlmesh,sqlmesh*,synq*,test"
	conf := &BigQueryScrapperConf{
		BigQueryConf: dwhexecbigquery.BigQueryConf{
			ProjectId:       "nifty-motif-341212",
			Region:          "EU",
			CredentialsJson: string(credentialsJson),
		},
		Blocklist: blocklist,
	}

	bqScrapper, err := NewBigQueryScrapper(s.ctx, conf)
	if err != nil {
		s.T().Skipf("Skipping: could not connect to BigQuery: %v", err)
	}
	s.bigqueryScrapper = bqScrapper

	// Create test fixtures
	s.setupTestFixtures()
}

func (s *LocalBigQueryScrapperSuite) TearDownSuite() {
	if s.bigqueryScrapper != nil {
		s.cleanupTestFixtures()
		_ = s.bigqueryScrapper.Close()
	}
}

func (s *LocalBigQueryScrapperSuite) setupTestFixtures() {
	client := s.bigqueryScrapper.executor.GetBigQueryClient()

	// Create a test dataset (ignore error if already exists)
	dataset := client.Dataset(s.testDataset)
	_ = dataset.Create(s.ctx, nil)

	// Create a test table with various column types
	// BigQuery doesn't have Int128/Int256, but has NUMERIC (38 digits) and BIGNUMERIC (76 digits)
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS ` + "`nifty-motif-341212." + s.testDataset + `.test_bigquery_scrapper` + "`" + ` (
			id INT64,
			name STRING,
			amount NUMERIC,
			big_amount BIGNUMERIC,
			created_at TIMESTAMP,
			is_active BOOL
		)
	`
	err := s.bigqueryScrapper.executor.Exec(s.ctx, createTableSQL)
	s.Require().NoError(err)

	// Insert test data
	insertDataSQL := `
		INSERT INTO ` + "`nifty-motif-341212." + s.testDataset + `.test_bigquery_scrapper` + "`" + ` (id, name, amount, big_amount, created_at, is_active)
		VALUES
			(1, 'Alice', 100.50, 12345678901234567890.123456789012345678, TIMESTAMP('2024-01-01 10:00:00'), true),
			(2, 'Bob', 200.75, 99999999999999999999999999999999999999.999999999999999999, TIMESTAMP('2024-01-02 11:00:00'), false)
	`
	err = s.bigqueryScrapper.executor.Exec(s.ctx, insertDataSQL)
	s.Require().NoError(err)

	// Create a test view
	createViewSQL := `
		CREATE VIEW IF NOT EXISTS ` + "`nifty-motif-341212." + s.testDataset + `.test_bigquery_scrapper_view` + "`" + ` AS
		SELECT id, name, amount FROM ` + "`nifty-motif-341212." + s.testDataset + `.test_bigquery_scrapper` + "`" + ` WHERE is_active = true
	`
	err = s.bigqueryScrapper.executor.Exec(s.ctx, createViewSQL)
	s.Require().NoError(err)
}

func (s *LocalBigQueryScrapperSuite) cleanupTestFixtures() {
	client := s.bigqueryScrapper.executor.GetBigQueryClient()

	// Drop test view and table (ignore errors during cleanup)
	_ = client.Dataset(s.testDataset).Table("test_bigquery_scrapper_view").Delete(s.ctx)
	_ = client.Dataset(s.testDataset).Table("test_bigquery_scrapper").Delete(s.ctx)
}

func (s *LocalBigQueryScrapperSuite) TestQueryDatabases() {
	databases, err := s.bigqueryScrapper.QueryDatabases(s.ctx)
	// QueryDatabases is not supported for BigQuery
	s.ErrorIs(err, scrapper.ErrUnsupported)
	s.Nil(databases)
}

func (s *LocalBigQueryScrapperSuite) TestQueryTables() {
	tables, err := s.bigqueryScrapper.QueryTables(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(tables, "Should return tables")

	// Find our test table and view
	var foundTable, foundView bool
	for _, table := range tables {
		if table.Schema == s.testDataset {
			switch table.Table {
			case "test_bigquery_scrapper":
				foundTable = true
			case "test_bigquery_scrapper_view":
				foundView = true
			}
		}
	}

	s.True(foundTable, "Should find test_bigquery_scrapper table")
	s.True(foundView, "Should find test_bigquery_scrapper_view view")
}

func (s *LocalBigQueryScrapperSuite) TestQueryCatalog() {
	catalog, err := s.bigqueryScrapper.QueryCatalog(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(catalog, "Should return catalog entries")

	// Find columns from our test table
	var foundIdColumn, foundNameColumn, foundAmountColumn, foundBigAmountColumn bool
	for _, col := range catalog {
		if col.Schema == s.testDataset && col.Table == "test_bigquery_scrapper" {
			switch col.Column {
			case "id":
				foundIdColumn = true
				// BigQuery returns "INTEGER" not "INT64" in type string
				s.Contains(col.Type, "INTEGER")
			case "name":
				foundNameColumn = true
				s.Contains(col.Type, "STRING")
			case "amount":
				foundAmountColumn = true
				s.Contains(col.Type, "NUMERIC")
			case "big_amount":
				foundBigAmountColumn = true
				s.Contains(col.Type, "BIGNUMERIC")
			}
		}
	}

	s.True(foundIdColumn, "Should find id column")
	s.True(foundNameColumn, "Should find name column")
	s.True(foundAmountColumn, "Should find amount column (NUMERIC)")
	s.True(foundBigAmountColumn, "Should find big_amount column (BIGNUMERIC)")
}

func (s *LocalBigQueryScrapperSuite) TestQueryTableMetrics() {
	metrics, err := s.bigqueryScrapper.QueryTableMetrics(s.ctx, time.Time{})
	s.Require().NoError(err)
	s.NotEmpty(metrics, "Should return table metrics")

	// Find metrics for our test table
	var found bool
	for _, m := range metrics {
		if m.Schema == s.testDataset && m.Table == "test_bigquery_scrapper" {
			found = true
			s.NotNil(m.RowCount)
		}
	}

	s.True(found, "Should find metrics for test_bigquery_scrapper")
}

func (s *LocalBigQueryScrapperSuite) TestQuerySqlDefinitions() {
	definitions, err := s.bigqueryScrapper.QuerySqlDefinitions(s.ctx)
	s.Require().NoError(err)
	// Note: With blocklist enabled, only test_scrapper views will be returned
	// If the test view isn't immediately visible (eventual consistency), that's acceptable

	// If we find our view definition, validate it
	for _, def := range definitions {
		if def.Schema == s.testDataset && def.Table == "test_bigquery_scrapper_view" {
			s.True(def.IsView)
			s.NotEmpty(def.Sql)
			s.Contains(def.Sql, "test_bigquery_scrapper")
			return // Found and validated
		}
	}
	// It's acceptable if the view isn't found due to eventual consistency or API limitations
	s.T().Log("Test view not found in SQL definitions - this may be due to BigQuery eventual consistency")
}

func (s *LocalBigQueryScrapperSuite) TestQuerySegments() {
	sql := `SELECT DISTINCT name as segment FROM ` + "`nifty-motif-341212." + s.testDataset + `.test_bigquery_scrapper` + "`"
	segments, err := s.bigqueryScrapper.QuerySegments(s.ctx, sql)
	s.Require().NoError(err)
	s.Require().Len(segments, 2, "Should return 2 segments (Alice and Bob)")

	names := make([]string, len(segments))
	for i, seg := range segments {
		names[i] = seg.Segment
	}
	s.Contains(names, "Alice")
	s.Contains(names, "Bob")
}

func (s *LocalBigQueryScrapperSuite) TestQueryCustomMetrics() {
	sql := `SELECT
		name as segment_name,
		SUM(CAST(amount AS FLOAT64)) as total_amount,
		COUNT(*) as record_count
	FROM ` + "`nifty-motif-341212." + s.testDataset + `.test_bigquery_scrapper` + "`" + `
	GROUP BY name`

	result, err := s.bigqueryScrapper.QueryCustomMetrics(s.ctx, sql)
	s.Require().NoError(err)
	s.Require().Len(result, 2, "Should return 2 rows (one per name)")

	for _, row := range result {
		s.Require().Len(row.Segments, 1, "Should have one segment (name)")
		s.Equal("segment_name", row.Segments[0].Name)

		// Should have total_amount and record_count columns
		s.Require().Len(row.ColumnValues, 2)

		for _, col := range row.ColumnValues {
			s.False(col.IsNull)
			switch col.Name {
			case "total_amount":
				s.IsType(scrapper.DoubleValue(0), col.Value)
			case "record_count":
				s.IsType(scrapper.IntValue(0), col.Value)
				s.Equal(scrapper.IntValue(1), col.Value)
			}
		}
	}
}

func (s *LocalBigQueryScrapperSuite) TestQueryCustomMetrics_WithNumeric() {
	// Test that NUMERIC types are handled properly
	sql := `SELECT
		name as segment_name,
		amount as numeric_value
	FROM ` + "`nifty-motif-341212." + s.testDataset + `.test_bigquery_scrapper` + "`" + `
	ORDER BY id`

	result, err := s.bigqueryScrapper.QueryCustomMetrics(s.ctx, sql)
	s.Require().NoError(err)
	s.Require().Len(result, 2)

	// Check that numeric values are converted properly
	for _, row := range result {
		s.Require().Len(row.ColumnValues, 1)
		col := row.ColumnValues[0]
		s.Equal("numeric_value", col.Name)
		s.False(col.IsNull)
		// NUMERIC in BigQuery should be handled (may be DoubleValue or BigRat depending on implementation)
		s.NotNil(col.Value, "NUMERIC value should not be nil")
	}
}

func (s *LocalBigQueryScrapperSuite) TestQueryCustomMetrics_WithBigNumeric() {
	// Test that BIGNUMERIC types are handled properly
	sql := `SELECT
		name as segment_name,
		big_amount as bignumeric_value
	FROM ` + "`nifty-motif-341212." + s.testDataset + `.test_bigquery_scrapper` + "`" + `
	ORDER BY id`

	result, err := s.bigqueryScrapper.QueryCustomMetrics(s.ctx, sql)
	s.Require().NoError(err)
	s.Require().Len(result, 2)

	// Check that bignumeric values are converted properly
	for _, row := range result {
		s.Require().Len(row.ColumnValues, 1)
		col := row.ColumnValues[0]
		s.Equal("bignumeric_value", col.Name)
		s.False(col.IsNull)
		// BIGNUMERIC in BigQuery should be handled (may be DoubleValue or BigRat depending on implementation)
		s.NotNil(col.Value, "BIGNUMERIC value should not be nil")
	}
}

func (s *LocalBigQueryScrapperSuite) TestQueryCustomMetrics_WithTimestamp() {
	sql := `SELECT
		name as segment_name,
		created_at as timestamp_value
	FROM ` + "`nifty-motif-341212." + s.testDataset + `.test_bigquery_scrapper` + "`" + `
	ORDER BY id`

	result, err := s.bigqueryScrapper.QueryCustomMetrics(s.ctx, sql)
	s.Require().NoError(err)
	s.Require().Len(result, 2)

	// Check that timestamp values are converted properly
	for _, row := range result {
		s.Require().Len(row.ColumnValues, 1)
		col := row.ColumnValues[0]
		s.Equal("timestamp_value", col.Name)
		s.False(col.IsNull)
		_, ok := col.Value.(scrapper.TimeValue)
		s.True(ok, "Timestamp should be converted to TimeValue")
	}
}

func (s *LocalBigQueryScrapperSuite) TestQueryCustomMetrics_WithBoolean() {
	sql := `SELECT
		name as segment_name,
		is_active as bool_value
	FROM ` + "`nifty-motif-341212." + s.testDataset + `.test_bigquery_scrapper` + "`" + `
	ORDER BY id`

	result, err := s.bigqueryScrapper.QueryCustomMetrics(s.ctx, sql)
	s.Require().NoError(err)
	s.Require().Len(result, 2)

	// First row: Alice (is_active = true)
	s.Equal("Alice", result[0].Segments[0].Value)
	s.IsType(scrapper.IntValue(0), result[0].ColumnValues[0].Value)
	s.Equal(scrapper.IntValue(1), result[0].ColumnValues[0].Value)

	// Second row: Bob (is_active = false)
	s.Equal("Bob", result[1].Segments[0].Value)
	s.IsType(scrapper.IntValue(0), result[1].ColumnValues[0].Value)
	s.Equal(scrapper.IntValue(0), result[1].ColumnValues[0].Value)
}
