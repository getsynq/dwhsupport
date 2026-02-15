package duckdb

import (
	"context"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/getsynq/dwhsupport/scrapper"
	scrapperstdsql "github.com/getsynq/dwhsupport/scrapper/stdsql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/suite"
)

type DuckDBScrapperSuite struct {
	suite.Suite
}

func TestDuckDBScrapperSuite(t *testing.T) {
	suite.Run(t, new(DuckDBScrapperSuite))
}

func (s *DuckDBScrapperSuite) TestQueryCustomMetrics_HugeInt() {
	ctx := context.TODO()

	// Use in-memory DuckDB for testing hugeint support
	db, err := sqlx.Open("duckdb", "")
	s.Require().NoError(err)
	defer db.Close()

	// Test hugeint (128-bit integer) support
	// Max int64 is 9223372036854775807, so we test with a value larger than that
	sql := `SELECT
		'segment1' as segment_name,
		170141183460469231731687303715884105727::hugeint as huge_value,
		9223372036854775807::bigint as big_value,
		42::int as normal_value`

	result, err := scrapperstdsql.QueryCustomMetrics(ctx, db, sql)
	s.Require().NoError(err, "QueryCustomMetrics should handle hugeint type")
	s.Require().Len(result, 1)

	row := result[0]

	// Check that we got the column values
	s.Require().Len(row.ColumnValues, 3, "Should have 3 column values (segment excluded)")

	// Verify huge_value is converted to BigIntValue (since it exceeds int64 range)
	s.Equal("huge_value", row.ColumnValues[0].Name)
	s.False(row.ColumnValues[0].IsNull)
	s.IsType((*scrapper.BigIntValue)(nil), row.ColumnValues[0].Value)
	// Verify the value is preserved with full precision
	bigIntVal := row.ColumnValues[0].Value.(*scrapper.BigIntValue)
	s.Equal("170141183460469231731687303715884105727", bigIntVal.String())

	// Verify big_value is converted to IntValue (max int64)
	s.Equal("big_value", row.ColumnValues[1].Name)
	s.False(row.ColumnValues[1].IsNull)
	s.IsType(scrapper.IntValue(0), row.ColumnValues[1].Value)
	s.Equal(scrapper.IntValue(9223372036854775807), row.ColumnValues[1].Value)

	// Verify normal_value is converted to IntValue
	s.Equal("normal_value", row.ColumnValues[2].Name)
	s.False(row.ColumnValues[2].IsNull)
	s.IsType(scrapper.IntValue(0), row.ColumnValues[2].Value)
	s.Equal(scrapper.IntValue(42), row.ColumnValues[2].Value)

	// Verify segment was extracted
	s.Require().Len(row.Segments, 1)
	s.Equal("segment_name", row.Segments[0].Name)
	s.Equal("segment1", row.Segments[0].Value)
}

func (s *DuckDBScrapperSuite) TestQueryCustomMetrics_HugeIntWithinInt64Range() {
	ctx := context.TODO()

	// Use in-memory DuckDB for testing hugeint support
	db, err := sqlx.Open("duckdb", "")
	s.Require().NoError(err)
	defer db.Close()

	// Test hugeint that fits within int64 range - should be converted to IntValue
	sql := `SELECT 12345::hugeint as small_huge_value`

	result, err := scrapperstdsql.QueryCustomMetrics(ctx, db, sql)
	s.Require().NoError(err, "QueryCustomMetrics should handle hugeint that fits in int64")
	s.Require().Len(result, 1)

	row := result[0]
	s.Require().Len(row.ColumnValues, 1)

	// Hugeint within int64 range should be converted to IntValue
	s.Equal("small_huge_value", row.ColumnValues[0].Name)
	s.False(row.ColumnValues[0].IsNull)
	s.IsType(scrapper.IntValue(0), row.ColumnValues[0].Value)
	s.Equal(scrapper.IntValue(12345), row.ColumnValues[0].Value)
}

// LocalDuckDBScrapperSuite tests all scrapper methods against a local in-memory DuckDB
type LocalDuckDBScrapperSuite struct {
	suite.Suite
	duckdbScrapper *DuckDBScrapper
	ctx            context.Context
}

func TestLocalDuckDBScrapperSuite(t *testing.T) {
	suite.Run(t, new(LocalDuckDBScrapperSuite))
}

func (s *LocalDuckDBScrapperSuite) SetupSuite() {
	s.ctx = context.TODO()

	// Create a local in-memory DuckDB scrapper
	sc, err := NewLocalDuckDBScrapper(s.ctx, "", "test_instance")
	s.Require().NoError(err)
	s.duckdbScrapper = sc

	// Create test fixtures
	s.setupTestFixtures()
}

func (s *LocalDuckDBScrapperSuite) TearDownSuite() {
	if s.duckdbScrapper != nil {
		s.duckdbScrapper.Close()
	}
}

func (s *LocalDuckDBScrapperSuite) setupTestFixtures() {
	db := s.duckdbScrapper.executor.GetDb()

	// Create a test schema
	_, err := db.Exec(`CREATE SCHEMA IF NOT EXISTS test_schema`)
	s.Require().NoError(err)

	// Create a test table with various column types including comments
	_, err = db.Exec(`
		CREATE TABLE test_schema.test_table (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100),
			amount DECIMAL(10,2),
			created_at TIMESTAMP,
			is_active BOOLEAN,
			big_number HUGEINT
		)
	`)
	s.Require().NoError(err)

	// Add comments to table and columns
	_, err = db.Exec(`COMMENT ON TABLE test_schema.test_table IS 'A test table for unit testing'`)
	s.Require().NoError(err)

	_, err = db.Exec(`COMMENT ON COLUMN test_schema.test_table.id IS 'Primary identifier'`)
	s.Require().NoError(err)

	// Insert some test data
	_, err = db.Exec(`
		INSERT INTO test_schema.test_table (id, name, amount, created_at, is_active, big_number)
		VALUES
			(1, 'Alice', 100.50, '2024-01-01 10:00:00', true, 12345),
			(2, 'Bob', 200.75, '2024-01-02 11:00:00', false, 170141183460469231731687303715884105727)
	`)
	s.Require().NoError(err)

	// Create a test view
	_, err = db.Exec(`
		CREATE VIEW test_schema.test_view AS
		SELECT id, name, amount FROM test_schema.test_table WHERE is_active = true
	`)
	s.Require().NoError(err)

	// Add comment to view
	_, err = db.Exec(`COMMENT ON VIEW test_schema.test_view IS 'A view of active records'`)
	s.Require().NoError(err)

	// Create another table for testing multiple objects
	_, err = db.Exec(`
		CREATE TABLE test_schema.another_table (
			key VARCHAR PRIMARY KEY,
			value TEXT
		)
	`)
	s.Require().NoError(err)
}

func (s *LocalDuckDBScrapperSuite) TestQueryDatabases() {
	databases, err := s.duckdbScrapper.QueryDatabases(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(databases, "Should return at least one database")

	// Find the memory database (default for in-memory DuckDB)
	var found bool
	for _, db := range databases {
		s.Equal("test_instance", db.Instance)
		if db.Database == "memory" {
			found = true
			s.NotNil(db.DatabaseType)
			s.Equal("duckdb", *db.DatabaseType)
		}
	}
	s.True(found, "Should find 'memory' database")
}

func (s *LocalDuckDBScrapperSuite) TestQueryTables() {
	tables, err := s.duckdbScrapper.QueryTables(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(tables, "Should return tables")

	// Find our test table
	var foundTable, foundView, foundAnotherTable bool
	for _, table := range tables {
		s.Equal("test_instance", table.Instance)
		s.Equal("memory", table.Database)
		s.Equal("test_schema", table.Schema)

		switch table.Table {
		case "test_table":
			foundTable = true
			s.Equal("BASE TABLE", table.TableType)
			s.NotNil(table.Description)
			s.Equal("A test table for unit testing", *table.Description)
		case "test_view":
			foundView = true
			s.Equal("VIEW", table.TableType)
			s.NotNil(table.Description)
			s.Equal("A view of active records", *table.Description)
		case "another_table":
			foundAnotherTable = true
			s.Equal("BASE TABLE", table.TableType)
		}
	}

	s.True(foundTable, "Should find test_table")
	s.True(foundView, "Should find test_view")
	s.True(foundAnotherTable, "Should find another_table")
}

func (s *LocalDuckDBScrapperSuite) TestQueryCatalog() {
	catalog, err := s.duckdbScrapper.QueryCatalog(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(catalog, "Should return catalog entries")

	// Find columns from our test table
	var foundIdColumn, foundNameColumn, foundBigNumberColumn bool
	for _, col := range catalog {
		if col.Schema == "test_schema" && col.Table == "test_table" {
			s.Equal("test_instance", col.Instance)
			s.Equal("memory", col.Database)

			switch col.Column {
			case "id":
				foundIdColumn = true
				s.Contains(col.Type, "INTEGER")
				s.NotNil(col.Comment)
				s.Equal("Primary identifier", *col.Comment)
				s.False(col.IsView)
			case "name":
				foundNameColumn = true
				s.Contains(col.Type, "VARCHAR")
			case "big_number":
				foundBigNumberColumn = true
				s.Contains(col.Type, "HUGEINT")
			}
		}
	}

	s.True(foundIdColumn, "Should find id column")
	s.True(foundNameColumn, "Should find name column")
	s.True(foundBigNumberColumn, "Should find big_number column (hugeint)")
}

func (s *LocalDuckDBScrapperSuite) TestQueryTableMetrics() {
	metrics, err := s.duckdbScrapper.QueryTableMetrics(s.ctx, time.Time{})
	s.Require().NoError(err)
	s.NotEmpty(metrics, "Should return table metrics")

	// Find metrics for our test table
	var found bool
	for _, m := range metrics {
		if m.Schema == "test_schema" && m.Table == "test_table" {
			found = true
			s.Equal("test_instance", m.Instance)
			s.Equal("memory", m.Database)
			// Note: row_count from duckdb_tables() returns estimated_size
			s.NotNil(m.RowCount)
		}
	}

	s.True(found, "Should find metrics for test_table")
}

func (s *LocalDuckDBScrapperSuite) TestQuerySqlDefinitions() {
	definitions, err := s.duckdbScrapper.QuerySqlDefinitions(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(definitions, "Should return SQL definitions")

	// Find the view definition
	var found bool
	for _, def := range definitions {
		if def.Schema == "test_schema" && def.Table == "test_view" {
			found = true
			s.Equal("test_instance", def.Instance)
			s.Equal("memory", def.Database)
			s.True(def.IsView)
			s.NotEmpty(def.Sql)
			s.Contains(def.Sql, "test_table")
		}
	}

	s.True(found, "Should find SQL definition for test_view")
}

func (s *LocalDuckDBScrapperSuite) TestQuerySegments() {
	sql := `SELECT DISTINCT name as segment FROM test_schema.test_table`
	segments, err := s.duckdbScrapper.QuerySegments(s.ctx, sql)
	s.Require().NoError(err)
	s.Require().Len(segments, 2, "Should return 2 segments (Alice and Bob)")

	names := make([]string, len(segments))
	for i, seg := range segments {
		names[i] = seg.Segment
	}
	s.Contains(names, "Alice")
	s.Contains(names, "Bob")
}

func (s *LocalDuckDBScrapperSuite) TestQueryCustomMetrics() {
	sql := `SELECT
		name as segment_name,
		SUM(amount) as total_amount,
		COUNT(*) as record_count
	FROM test_schema.test_table
	GROUP BY name`

	result, err := s.duckdbScrapper.QueryCustomMetrics(s.ctx, sql)
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

func (s *LocalDuckDBScrapperSuite) TestQueryShape() {
	sql := `SELECT id, name, amount, created_at, is_active, big_number FROM test_schema.test_table`
	columns, err := s.duckdbScrapper.QueryShape(s.ctx, sql)
	s.Require().NoError(err)
	s.Require().Len(columns, 6)

	s.Equal("id", columns[0].Name)
	s.Equal(int32(1), columns[0].Position)
	s.Equal("INTEGER", columns[0].NativeType)

	s.Equal("name", columns[1].Name)
	s.Equal(int32(2), columns[1].Position)
	s.Equal("VARCHAR", columns[1].NativeType)

	s.Equal("amount", columns[2].Name)
	s.Equal(int32(3), columns[2].Position)
	s.Contains(columns[2].NativeType, "DECIMAL")

	s.Equal("created_at", columns[3].Name)
	s.Equal(int32(4), columns[3].Position)
	s.Equal("TIMESTAMP", columns[3].NativeType)

	s.Equal("is_active", columns[4].Name)
	s.Equal(int32(5), columns[4].Position)
	s.Equal("BOOLEAN", columns[4].NativeType)

	s.Equal("big_number", columns[5].Name)
	s.Equal(int32(6), columns[5].Position)
	s.Equal("HUGEINT", columns[5].NativeType)
}

func (s *LocalDuckDBScrapperSuite) TestQueryShape_StdSQL() {
	ctx := context.TODO()

	db, err := sqlx.Open("duckdb", "")
	s.Require().NoError(err)
	defer db.Close()

	sql := `SELECT 1::INTEGER as id, 'hello'::VARCHAR as name, 3.14::DOUBLE as value`
	columns, err := scrapperstdsql.QueryShape(ctx, db, sql)
	s.Require().NoError(err)
	s.Require().Len(columns, 3)

	s.Equal("id", columns[0].Name)
	s.Equal(int32(1), columns[0].Position)
	s.Equal("INTEGER", columns[0].NativeType)

	s.Equal("name", columns[1].Name)
	s.Equal(int32(2), columns[1].Position)
	s.Equal("VARCHAR", columns[1].NativeType)

	s.Equal("value", columns[2].Name)
	s.Equal(int32(3), columns[2].Position)
	s.Equal("DOUBLE", columns[2].NativeType)
}

func (s *LocalDuckDBScrapperSuite) TestQueryTableConstraints() {
	constraints, err := s.duckdbScrapper.QueryTableConstraints(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(constraints, "Should return table constraints")

	// Find primary key constraints from our test fixtures
	var foundTestTablePK, foundAnotherTablePK bool
	for _, c := range constraints {
		s.Equal("test_instance", c.Instance)
		if c.Schema == "test_schema" && c.Table == "test_table" && c.ConstraintType == scrapper.ConstraintTypePrimaryKey {
			foundTestTablePK = true
			s.Equal("id", c.ColumnName)
		}
		if c.Schema == "test_schema" && c.Table == "another_table" && c.ConstraintType == scrapper.ConstraintTypePrimaryKey {
			foundAnotherTablePK = true
			s.Equal("key", c.ColumnName)
		}
	}

	s.True(foundTestTablePK, "Should find PRIMARY KEY constraint for test_table.id")
	s.True(foundAnotherTablePK, "Should find PRIMARY KEY constraint for another_table.key")
}

func (s *LocalDuckDBScrapperSuite) TestQueryCustomMetrics_WithHugeInt() {
	sql := `SELECT
		name as segment_name,
		big_number as huge_value
	FROM test_schema.test_table
	ORDER BY id`

	result, err := s.duckdbScrapper.QueryCustomMetrics(s.ctx, sql)
	s.Require().NoError(err)
	s.Require().Len(result, 2)

	// First row: Alice with small hugeint (fits in int64)
	s.Equal("Alice", result[0].Segments[0].Value)
	s.IsType(scrapper.IntValue(0), result[0].ColumnValues[0].Value)
	s.Equal(scrapper.IntValue(12345), result[0].ColumnValues[0].Value)

	// Second row: Bob with large hugeint (exceeds int64, becomes BigIntValue)
	s.Equal("Bob", result[1].Segments[0].Value)
	s.IsType((*scrapper.BigIntValue)(nil), result[1].ColumnValues[0].Value)
	bigVal := result[1].ColumnValues[0].Value.(*scrapper.BigIntValue)
	s.Equal("170141183460469231731687303715884105727", bigVal.String())
}
