package clickhouse

import (
	"context"
	"os"
	"testing"
	"time"

	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	"github.com/getsynq/dwhsupport/scrapper"
	scrapperstdsql "github.com/getsynq/dwhsupport/scrapper/stdsql"
	"github.com/stretchr/testify/suite"
)

// LocalClickHouseScrapperSuite tests scrapper methods against a local ClickHouse instance
type LocalClickHouseScrapperSuite struct {
	suite.Suite
	clickhouseScrapper *ClickhouseScrapper
	ctx                context.Context
}

func TestLocalClickHouseScrapperSuite(t *testing.T) {
	suite.Run(t, new(LocalClickHouseScrapperSuite))
}

func (s *LocalClickHouseScrapperSuite) SetupSuite() {
	// Skip in CI environment
	if os.Getenv("CI") != "" {
		s.T().Skip("Skipping local ClickHouse tests in CI")
	}

	s.ctx = context.TODO()

	// Create a local ClickHouse scrapper
	conf := ClickhouseScrapperConf{
		ClickhouseConf: dwhexecclickhouse.ClickhouseConf{
			Hostname:        "127.0.0.1",
			Port:            9000,
			Username:        "",
			Password:        "getsynq10",
			DefaultDatabase: "kernel_runs",
			NoSsl:           true,
		},
		DatabaseName: "kernel_runs",
	}

	scrapper, err := NewClickhouseScrapper(s.ctx, conf)
	if err != nil {
		s.T().Skipf("Skipping: could not connect to local ClickHouse: %v", err)
	}
	s.clickhouseScrapper = scrapper

	// Create test fixtures
	s.setupTestFixtures()
}

func (s *LocalClickHouseScrapperSuite) TearDownSuite() {
	if s.clickhouseScrapper != nil {
		s.cleanupTestFixtures()
		s.clickhouseScrapper.Close()
	}
}

func (s *LocalClickHouseScrapperSuite) setupTestFixtures() {
	db := s.clickhouseScrapper.executor.GetDb()

	// Create a test table with various column types
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS test_clickhouse_scrapper (
			id UInt64,
			name String,
			amount Decimal(10,2),
			created_at DateTime,
			is_active UInt8,
			big_number Int128,
			huge_number Int256
		) ENGINE = MergeTree()
		ORDER BY id
	`)
	s.Require().NoError(err)

	// Insert test data
	_, err = db.Exec(`
		INSERT INTO test_clickhouse_scrapper (id, name, amount, created_at, is_active, big_number, huge_number)
		VALUES
			(1, 'Alice', 100.50, '2024-01-01 10:00:00', 1, 12345, 12345),
			(2, 'Bob', 200.75, '2024-01-02 11:00:00', 0, 170141183460469231731687303715884105727, 57896044618658097711785492504343953926634992332820282019728792003956564819967)
	`)
	s.Require().NoError(err)

	// Create a test view
	_, err = db.Exec(`
		CREATE VIEW IF NOT EXISTS test_clickhouse_scrapper_view AS
		SELECT id, name, amount FROM test_clickhouse_scrapper WHERE is_active = 1
	`)
	s.Require().NoError(err)
}

func (s *LocalClickHouseScrapperSuite) cleanupTestFixtures() {
	db := s.clickhouseScrapper.executor.GetDb()
	db.Exec(`DROP VIEW IF EXISTS test_clickhouse_scrapper_view`)
	db.Exec(`DROP TABLE IF EXISTS test_clickhouse_scrapper`)
}

func (s *LocalClickHouseScrapperSuite) TestQueryDatabases() {
	databases, err := s.clickhouseScrapper.QueryDatabases(s.ctx)
	// QueryDatabases is not supported for ClickHouse
	s.ErrorIs(err, scrapper.ErrUnsupported)
	s.Nil(databases)
}

func (s *LocalClickHouseScrapperSuite) TestQueryTables() {
	tables, err := s.clickhouseScrapper.QueryTables(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(tables, "Should return tables")

	// Find our test table
	var foundTable, foundView bool
	for _, table := range tables {
		if table.Database == "kernel_runs" {
			switch table.Table {
			case "test_clickhouse_scrapper":
				foundTable = true
			case "test_clickhouse_scrapper_view":
				foundView = true
			}
		}
	}

	s.True(foundTable, "Should find test_clickhouse_scrapper table")
	s.True(foundView, "Should find test_clickhouse_scrapper_view view")
}

func (s *LocalClickHouseScrapperSuite) TestQueryCatalog() {
	catalog, err := s.clickhouseScrapper.QueryCatalog(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(catalog, "Should return catalog entries")

	// Find columns from our test table
	var foundIdColumn, foundNameColumn, foundBigNumberColumn, foundHugeNumberColumn bool
	for _, col := range catalog {
		if col.Database == "kernel_runs" && col.Table == "test_clickhouse_scrapper" {
			switch col.Column {
			case "id":
				foundIdColumn = true
				s.Contains(col.Type, "UInt64")
			case "name":
				foundNameColumn = true
				s.Contains(col.Type, "String")
			case "big_number":
				foundBigNumberColumn = true
				s.Contains(col.Type, "Int128")
			case "huge_number":
				foundHugeNumberColumn = true
				s.Contains(col.Type, "Int256")
			}
		}
	}

	s.True(foundIdColumn, "Should find id column")
	s.True(foundNameColumn, "Should find name column")
	s.True(foundBigNumberColumn, "Should find big_number column (Int128)")
	s.True(foundHugeNumberColumn, "Should find huge_number column (Int256)")
}

func (s *LocalClickHouseScrapperSuite) TestQueryTableMetrics() {
	metrics, err := s.clickhouseScrapper.QueryTableMetrics(s.ctx, time.Time{})
	s.Require().NoError(err)
	s.NotEmpty(metrics, "Should return table metrics")

	// Find metrics for our test table
	var found bool
	for _, m := range metrics {
		if m.Database == "kernel_runs" && m.Table == "test_clickhouse_scrapper" {
			found = true
			s.NotNil(m.RowCount)
		}
	}

	s.True(found, "Should find metrics for test_clickhouse_scrapper")
}

func (s *LocalClickHouseScrapperSuite) TestQuerySqlDefinitions() {
	definitions, err := s.clickhouseScrapper.QuerySqlDefinitions(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(definitions, "Should return SQL definitions")

	// Find our view definition
	var found bool
	for _, def := range definitions {
		if def.Database == "kernel_runs" && def.Table == "test_clickhouse_scrapper_view" {
			found = true
			s.True(def.IsView)
			s.NotEmpty(def.Sql)
			s.Contains(def.Sql, "test_clickhouse_scrapper")
		}
	}

	s.True(found, "Should find SQL definition for test_clickhouse_scrapper_view")
}

func (s *LocalClickHouseScrapperSuite) TestQuerySegments() {
	sql := `SELECT DISTINCT name as segment FROM test_clickhouse_scrapper`
	segments, err := s.clickhouseScrapper.QuerySegments(s.ctx, sql)
	s.Require().NoError(err)
	s.Require().Len(segments, 2, "Should return 2 segments (Alice and Bob)")

	names := make([]string, len(segments))
	for i, seg := range segments {
		names[i] = seg.Segment
	}
	s.Contains(names, "Alice")
	s.Contains(names, "Bob")
}

func (s *LocalClickHouseScrapperSuite) TestQueryCustomMetrics() {
	sql := `SELECT
		name as segment_name,
		toFloat64(SUM(amount)) as total_amount,
		COUNT(*) as record_count
	FROM test_clickhouse_scrapper
	GROUP BY name`

	result, err := s.clickhouseScrapper.QueryCustomMetrics(s.ctx, sql)
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

func (s *LocalClickHouseScrapperSuite) TestQueryCustomMetrics_WithBigInt() {
	sql := `SELECT
		name as segment_name,
		big_number as big_value
	FROM test_clickhouse_scrapper
	ORDER BY id`

	result, err := s.clickhouseScrapper.QueryCustomMetrics(s.ctx, sql)
	s.Require().NoError(err)
	s.Require().Len(result, 2)

	// First row: Alice with small Int128 (fits in int64)
	s.Equal("Alice", result[0].Segments[0].Value)
	s.IsType(scrapper.IntValue(0), result[0].ColumnValues[0].Value)
	s.Equal(scrapper.IntValue(12345), result[0].ColumnValues[0].Value)

	// Second row: Bob with large Int128 (exceeds int64, becomes BigIntValue)
	s.Equal("Bob", result[1].Segments[0].Value)
	s.IsType((*scrapper.BigIntValue)(nil), result[1].ColumnValues[0].Value)
	bigVal := result[1].ColumnValues[0].Value.(*scrapper.BigIntValue)
	s.Equal("170141183460469231731687303715884105727", bigVal.String())
}

func (s *LocalClickHouseScrapperSuite) TestQueryCustomMetrics_WithInt256() {
	sql := `SELECT
		name as segment_name,
		huge_number as huge_value
	FROM test_clickhouse_scrapper
	ORDER BY id`

	result, err := s.clickhouseScrapper.QueryCustomMetrics(s.ctx, sql)
	s.Require().NoError(err)
	s.Require().Len(result, 2)

	// First row: Alice with small Int256 (fits in int64)
	s.Equal("Alice", result[0].Segments[0].Value)
	s.IsType(scrapper.IntValue(0), result[0].ColumnValues[0].Value)
	s.Equal(scrapper.IntValue(12345), result[0].ColumnValues[0].Value)

	// Second row: Bob with large Int256 (exceeds int64, becomes BigIntValue)
	s.Equal("Bob", result[1].Segments[0].Value)
	s.IsType((*scrapper.BigIntValue)(nil), result[1].ColumnValues[0].Value)
	bigVal := result[1].ColumnValues[0].Value.(*scrapper.BigIntValue)
	s.Equal("57896044618658097711785492504343953926634992332820282019728792003956564819967", bigVal.String())
}

func (s *LocalClickHouseScrapperSuite) TestQueryCustomMetrics_WithDecimal() {
	// Test that Decimal types are handled properly
	sql := `SELECT
		name as segment_name,
		amount as decimal_value
	FROM test_clickhouse_scrapper
	ORDER BY id`

	result, err := s.clickhouseScrapper.QueryCustomMetrics(s.ctx, sql)
	s.Require().NoError(err)
	s.Require().Len(result, 2)

	// Check that decimal values are converted to DoubleValue
	for _, row := range result {
		s.Require().Len(row.ColumnValues, 1)
		col := row.ColumnValues[0]
		s.Equal("decimal_value", col.Name)
		s.False(col.IsNull)
		// Decimal should be converted to DoubleValue via the Float64 interface
		s.IsType(scrapper.DoubleValue(0), col.Value, "Decimal should be converted to DoubleValue")
	}
}

func (s *LocalClickHouseScrapperSuite) TestQueryTableConstraints() {
	constraints, err := s.clickhouseScrapper.QueryTableConstraints(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(constraints, "Should return table constraints")

	// Find constraints for our test table (MergeTree ORDER BY id makes id both primary key and sorting key)
	var foundPrimaryKey, foundSortingKey bool
	for _, c := range constraints {
		if c.Schema == "kernel_runs" && c.Table == "test_clickhouse_scrapper" {
			s.Equal("kernel_runs", c.Database)
			if c.ConstraintType == scrapper.ConstraintTypePrimaryKey && c.ColumnName == "id" {
				foundPrimaryKey = true
			}
			if c.ConstraintType == scrapper.ConstraintTypeSortingKey && c.ColumnName == "id" {
				foundSortingKey = true
			}
		}
	}

	s.True(foundPrimaryKey, "Should find PRIMARY KEY constraint for test_clickhouse_scrapper.id")
	s.True(foundSortingKey, "Should find SORTING KEY constraint for test_clickhouse_scrapper.id")
}

// TestQueryCustomMetrics_DirectDB tests QueryCustomMetrics directly with the DB connection
func (s *LocalClickHouseScrapperSuite) TestQueryCustomMetrics_DirectDB() {
	db := s.clickhouseScrapper.executor.GetDb()

	sql := `SELECT
		name as segment_name,
		big_number as big_value,
		huge_number as huge_value
	FROM test_clickhouse_scrapper
	ORDER BY id`

	result, err := scrapperstdsql.QueryCustomMetrics(s.ctx, db, sql)
	s.Require().NoError(err)
	s.Require().Len(result, 2)

	// Verify both Int128 and Int256 are handled correctly
	// Second row has large values
	row := result[1]
	s.Equal("Bob", row.Segments[0].Value)

	// big_value (Int128)
	s.IsType((*scrapper.BigIntValue)(nil), row.ColumnValues[0].Value)

	// huge_value (Int256)
	s.IsType((*scrapper.BigIntValue)(nil), row.ColumnValues[1].Value)
}
