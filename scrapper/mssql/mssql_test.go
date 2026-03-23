package mssql

import (
	"context"
	_ "embed"
	"os"
	"strings"
	"testing"
	"time"

	dwhexecmssql "github.com/getsynq/dwhsupport/exec/mssql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/suite"
)

//go:embed testdata/init.sql
var initSQL string

// execBatches splits a SQL script on GO batch separators and executes each batch.
func execBatches(db *sqlx.DB, script string) error {
	for _, batch := range splitGoBatches(script) {
		batch = strings.TrimSpace(batch)
		if batch == "" {
			continue
		}
		if _, err := db.Exec(batch); err != nil {
			return err
		}
	}
	return nil
}

// splitGoBatches splits a T-SQL script on GO batch separators.
// GO must appear on its own line (case-insensitive).
func splitGoBatches(script string) []string {
	var batches []string
	var current strings.Builder
	for _, line := range strings.Split(script, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.EqualFold(trimmed, "GO") {
			batches = append(batches, current.String())
			current.Reset()
		} else {
			current.WriteString(line)
			current.WriteString("\n")
		}
	}
	if s := current.String(); strings.TrimSpace(s) != "" {
		batches = append(batches, s)
	}
	return batches
}

func newMSSQLConf() *dwhexecmssql.MSSQLConf {
	return &dwhexecmssql.MSSQLConf{
		User:      testenv.EnvOrDefault("MSSQL_USER", "sa"),
		Password:  testenv.EnvOrDefault("MSSQL_PASSWORD", "SynqTest1!"),
		Host:      testenv.EnvOrDefault("MSSQL_HOST", "127.0.0.1"),
		Port:      testenv.EnvOrDefaultInt("MSSQL_PORT", 1433),
		Database:  testenv.EnvOrDefault("MSSQL_DATABASE", "synq_test"),
		TrustCert: true,
	}
}

// setupDatabase connects to master, creates/resets the test database,
// runs init.sql to populate fixtures, then returns a scrapper connected to the test database.
func setupDatabase(t *testing.T, ctx context.Context) *MSSQLScrapper {
	t.Helper()

	conf := newMSSQLConf()

	// Connect to master to create the test database.
	masterConf := *conf
	masterConf.Database = "master"
	masterExec, err := dwhexecmssql.NewMSSQLExecutor(ctx, &masterConf)
	if err != nil {
		t.Skipf("Skipping: could not connect to local MSSQL: %v", err)
	}

	// Drop and recreate to ensure clean state.
	masterExec.GetDb().Exec("ALTER DATABASE [" + conf.Database + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
	masterExec.GetDb().Exec("DROP DATABASE IF EXISTS [" + conf.Database + "]")
	_, err = masterExec.GetDb().Exec("CREATE DATABASE [" + conf.Database + "]")
	masterExec.Close()
	if err != nil {
		t.Fatalf("Failed to create database %s: %v", conf.Database, err)
	}

	// Connect to the test database and run init SQL to create schemas, tables, and data.
	sc, err := NewMSSQLScrapper(ctx, conf)
	if err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
	}

	if err := execBatches(sc.executor.GetDb(), initSQL); err != nil {
		sc.Close()
		t.Fatalf("Failed to execute init SQL: %v", err)
	}

	return sc
}

// MSSQLScrapperSuite tests all scrapper methods against a local MSSQL instance.
// Start the database with: docker compose -f scrapper/mssql/docker-compose.yml up -d
type MSSQLScrapperSuite struct {
	suite.Suite
	scrapper     *MSSQLScrapper
	ctx          context.Context
	databaseName string
}

func TestMSSQLScrapperSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping local MSSQL tests in CI")
	}
	suite.Run(t, new(MSSQLScrapperSuite))
}

func (s *MSSQLScrapperSuite) SetupSuite() {
	s.ctx = context.Background()
	s.databaseName = testenv.EnvOrDefault("MSSQL_DATABASE", "synq_test")
	s.scrapper = setupDatabase(s.T(), s.ctx)
}

func (s *MSSQLScrapperSuite) TearDownSuite() {
	if s.scrapper != nil {
		s.scrapper.Close()
	}
}

func (s *MSSQLScrapperSuite) TestQueryDatabases() {
	databases, err := s.scrapper.QueryDatabases(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(databases, "Should return at least one database")

	var found bool
	for _, db := range databases {
		s.Equal("127.0.0.1", db.Instance)
		if db.Database == s.databaseName {
			found = true
		}
	}
	s.True(found, "Should find %s database", s.databaseName)
}

func (s *MSSQLScrapperSuite) TestQueryTables() {
	tables, err := s.scrapper.QueryTables(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(tables, "Should return tables")

	var foundProducts, foundOrderItems, foundActiveProducts, foundOrderSummary bool
	var foundCustomers, foundCustomerRegions bool
	for _, t := range tables {
		s.Equal("127.0.0.1", t.Instance)
		s.Equal(s.databaseName, t.Database)

		switch {
		case t.Schema == "schema_a" && t.Table == "products":
			foundProducts = true
			s.Equal("BASE TABLE", t.TableType)
			s.NotNil(t.Description)
			s.Equal("Product catalog with inventory tracking", *t.Description)
		case t.Schema == "schema_a" && t.Table == "order_items":
			foundOrderItems = true
			s.Equal("BASE TABLE", t.TableType)
			s.NotNil(t.Description)
			s.Equal("Line items within customer orders", *t.Description)
		case t.Schema == "schema_a" && t.Table == "active_products":
			foundActiveProducts = true
			s.Equal("VIEW", t.TableType)
		case t.Schema == "schema_a" && t.Table == "order_summary":
			foundOrderSummary = true
			s.Equal("VIEW", t.TableType)
		case t.Schema == "schema_b" && t.Table == "customers":
			foundCustomers = true
			s.Equal("BASE TABLE", t.TableType)
		case t.Schema == "schema_b" && t.Table == "customer_regions":
			foundCustomerRegions = true
			s.Equal("VIEW", t.TableType)
		}
	}

	s.True(foundProducts, "Should find schema_a.products")
	s.True(foundOrderItems, "Should find schema_a.order_items")
	s.True(foundActiveProducts, "Should find schema_a.active_products view")
	s.True(foundOrderSummary, "Should find schema_a.order_summary view")
	s.True(foundCustomers, "Should find schema_b.customers")
	s.True(foundCustomerRegions, "Should find schema_b.customer_regions view")
}

func (s *MSSQLScrapperSuite) TestQueryCatalog() {
	catalog, err := s.scrapper.QueryCatalog(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(catalog, "Should return catalog entries")

	var foundIdCol, foundNameCol, foundPriceCol, foundCreatedAtCol bool
	var foundIdComment, foundNameComment, foundTableComment bool
	for _, col := range catalog {
		s.Equal("127.0.0.1", col.Instance)
		s.Equal(s.databaseName, col.Database)

		if col.Schema == "schema_a" && col.Table == "products" {
			switch col.Column {
			case "id":
				foundIdCol = true
				s.Contains(col.Type, "int")
			case "name":
				foundNameCol = true
				s.Contains(col.Type, "nvarchar")
			case "price":
				foundPriceCol = true
				s.Contains(col.Type, "decimal")
			case "created_at":
				foundCreatedAtCol = true
				s.Contains(col.Type, "datetime2")
			}
			if col.Comment != nil {
				switch col.Column {
				case "id":
					foundIdComment = true
					s.Equal("Unique product identifier", *col.Comment)
				case "name":
					foundNameComment = true
					s.Equal("Product display name", *col.Comment)
				}
			}
			if col.TableComment != nil && *col.TableComment == "Product catalog with inventory tracking" {
				foundTableComment = true
			}
		}
	}

	s.True(foundIdCol, "Should find id column")
	s.True(foundNameCol, "Should find name column")
	s.True(foundPriceCol, "Should find price column")
	s.True(foundCreatedAtCol, "Should find created_at column")
	s.True(foundIdComment, "Should find comment on id column")
	s.True(foundNameComment, "Should find comment on name column")
	s.True(foundTableComment, "Should find table comment via extended properties")
}

func (s *MSSQLScrapperSuite) TestQueryTableMetrics() {
	metrics, err := s.scrapper.QueryTableMetrics(s.ctx, time.Time{})
	s.Require().NoError(err)
	s.NotEmpty(metrics, "Should return table metrics")

	var foundProducts, foundOrderItems bool
	for _, m := range metrics {
		s.Equal("127.0.0.1", m.Instance)
		s.Equal(s.databaseName, m.Database)

		if m.Schema == "schema_a" && m.Table == "products" {
			foundProducts = true
			s.NotNil(m.RowCount, "products should have row_count")
			s.Equal(int64(3), *m.RowCount)
		}
		if m.Schema == "schema_a" && m.Table == "order_items" {
			foundOrderItems = true
			s.NotNil(m.RowCount, "order_items should have row_count")
			s.Equal(int64(3), *m.RowCount)
		}
	}

	s.True(foundProducts, "Should find metrics for schema_a.products")
	s.True(foundOrderItems, "Should find metrics for schema_a.order_items")
}

func (s *MSSQLScrapperSuite) TestQuerySqlDefinitions() {
	definitions, err := s.scrapper.QuerySqlDefinitions(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(definitions, "Should return SQL definitions")

	var foundActiveProducts, foundOrderSummary bool
	for _, def := range definitions {
		s.Equal("127.0.0.1", def.Instance)
		s.Equal(s.databaseName, def.Database)

		if def.Schema == "schema_a" && def.Table == "active_products" {
			foundActiveProducts = true
			s.True(def.IsView)
			s.NotEmpty(def.Sql)
			s.Contains(def.Sql, "products")
			s.Contains(def.Sql, "is_active")
		}
		if def.Schema == "schema_a" && def.Table == "order_summary" {
			foundOrderSummary = true
			s.True(def.IsView)
			s.NotEmpty(def.Sql)
			s.Contains(def.Sql, "order_items")
		}
	}

	s.True(foundActiveProducts, "Should find SQL definition for active_products view")
	s.True(foundOrderSummary, "Should find SQL definition for order_summary view")
}

func (s *MSSQLScrapperSuite) TestQueryTableConstraints() {
	constraints, err := s.scrapper.QueryTableConstraints(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(constraints, "Should return table constraints")

	var foundProductsPK, foundOrderItemsCompositePK bool
	var foundProductsSkuUnique, foundProductsCategoryIdx bool
	var foundOrderItemsUniqueConstraint bool
	for _, c := range constraints {
		s.Equal("127.0.0.1", c.Instance)
		s.Equal(s.databaseName, c.Database)
		s.NotEmpty(c.ConstraintName)
		s.NotEmpty(c.ColumnName)

		switch {
		case c.Schema == "schema_a" && c.Table == "products" && c.ConstraintType == scrapper.ConstraintTypePrimaryKey && c.ColumnName == "id":
			foundProductsPK = true
		case c.Schema == "schema_a" && c.Table == "order_items" && c.ConstraintType == scrapper.ConstraintTypePrimaryKey:
			foundOrderItemsCompositePK = true
		case c.Schema == "schema_a" && c.Table == "products" && c.ConstraintType == scrapper.ConstraintTypeUniqueIndex && c.ColumnName == "sku":
			foundProductsSkuUnique = true
		case c.Schema == "schema_a" && c.Table == "products" && c.ConstraintType == scrapper.ConstraintTypeIndex && c.ColumnName == "category":
			foundProductsCategoryIdx = true
		case c.Schema == "schema_a" && c.Table == "order_items" && c.ConstraintType == scrapper.ConstraintTypeUniqueIndex:
			foundOrderItemsUniqueConstraint = true
		}
	}

	s.True(foundProductsPK, "Should find PRIMARY KEY for products.id")
	s.True(foundOrderItemsCompositePK, "Should find composite PRIMARY KEY for order_items")
	s.True(foundProductsSkuUnique, "Should find UNIQUE INDEX for products.sku")
	s.True(foundProductsCategoryIdx, "Should find INDEX for products.category")
	s.True(foundOrderItemsUniqueConstraint, "Should find UNIQUE constraint on order_items")
}

func (s *MSSQLScrapperSuite) TestQuerySegments() {
	sql := `SELECT DISTINCT category as segment FROM schema_a.products`
	segments, err := s.scrapper.QuerySegments(s.ctx, sql)
	s.Require().NoError(err)
	s.NotEmpty(segments)

	names := make([]string, len(segments))
	for i, seg := range segments {
		names[i] = seg.Segment
	}
	s.Contains(names, "Electronics")
	s.Contains(names, "Accessories")
}

func (s *MSSQLScrapperSuite) TestQueryCustomMetrics() {
	sql := `SELECT
		category as segment_name,
		CAST(SUM(price * quantity) AS FLOAT) as total_value,
		COUNT(*) as product_count
	FROM schema_a.products
	GROUP BY category`

	result, err := s.scrapper.QueryCustomMetrics(s.ctx, sql)
	s.Require().NoError(err)
	s.NotEmpty(result)

	for _, row := range result {
		s.Require().Len(row.Segments, 1)
		s.Equal("segment_name", row.Segments[0].Name)
		s.Require().Len(row.ColumnValues, 2)

		for _, col := range row.ColumnValues {
			s.False(col.IsNull)
		}
	}
}

func (s *MSSQLScrapperSuite) TestQueryShape() {
	sql := `SELECT id, name, price, created_at, is_active FROM schema_a.products`
	columns, err := s.scrapper.QueryShape(s.ctx, sql)
	s.Require().NoError(err)
	s.Require().Len(columns, 5)

	s.Equal("id", columns[0].Name)
	s.Equal(int32(1), columns[0].Position)

	s.Equal("name", columns[1].Name)
	s.Equal(int32(2), columns[1].Position)

	s.Equal("price", columns[2].Name)
	s.Equal(int32(3), columns[2].Position)

	s.Equal("created_at", columns[3].Name)
	s.Equal(int32(4), columns[3].Position)

	s.Equal("is_active", columns[4].Name)
	s.Equal(int32(5), columns[4].Position)
}

// MSSQLComplianceSuite runs the standard compliance test suite.
type MSSQLComplianceSuite struct {
	scrappertest.ComplianceSuite
}

func TestMSSQLComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping local MSSQL compliance tests in CI")
	}
	suite.Run(t, new(MSSQLComplianceSuite))
}

func (s *MSSQLComplianceSuite) SetupSuite() {
	s.Scrapper = setupDatabase(s.T(), s.Ctx())
}

func (s *MSSQLComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		s.Scrapper.Close()
	}
}

// MSSQLScopeComplianceSuite runs the scope filtering compliance checks.
type MSSQLScopeComplianceSuite struct {
	scrappertest.ScopeComplianceSuite
}

func TestMSSQLScopeComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping local MSSQL scope compliance tests in CI")
	}
	suite.Run(t, new(MSSQLScopeComplianceSuite))
}

func (s *MSSQLScopeComplianceSuite) SetupSuite() {
	s.Scrapper = setupDatabase(s.T(), s.Ctx())
}

func (s *MSSQLScopeComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		s.Scrapper.Close()
	}
}
