package mssql

import (
	"context"
	"os"
	"testing"
	"time"

	"io"

	dwhexecmssql "github.com/getsynq/dwhsupport/exec/mssql"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

func newMSSQLScrapperFromEnv(ctx context.Context) (*MSSQLScrapper, error) {
	conf := &MSSQLScrapperConf{
		MSSQLConf: dwhexecmssql.MSSQLConf{
			User:      testenv.EnvOrDefault("MSSQL_USER", "synq"),
			Password:  testenv.EnvOrDefault("MSSQL_PASSWORD", "SynqTest1!"),
			Host:      testenv.EnvOrDefault("MSSQL_HOST", "127.0.0.1"),
			Port:      testenv.EnvOrDefaultInt("MSSQL_PORT", 1433),
			Database:  testenv.EnvOrDefault("MSSQL_DATABASE", "synq_test"),
			TrustCert: true,
			Encrypt:   testenv.EnvOrDefault("MSSQL_ENCRYPT", "disable"),
		},
	}
	return NewMSSQLScrapper(ctx, conf)
}

// MSSQLScrapperSuite tests all scrapper methods against an MSSQL instance.
// Requires a pre-seeded database (e.g. via dwhtesting staging infra).
type MSSQLScrapperSuite struct {
	suite.Suite
	scrapper     *MSSQLScrapper
	ctx          context.Context
	databaseName string
}

func TestMSSQLScrapperSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MSSQL tests in CI")
	}
	suite.Run(t, new(MSSQLScrapperSuite))
}

func (s *MSSQLScrapperSuite) SetupSuite() {
	s.ctx = context.Background()
	s.databaseName = testenv.EnvOrDefault("MSSQL_DATABASE", "synq_test")
	sc, err := newMSSQLScrapperFromEnv(s.ctx)
	if err != nil {
		s.T().Skipf("Could not connect to MSSQL: %v", err)
	}
	s.scrapper = sc
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
		s.Equal(s.databaseName, m.Database)

		if m.Schema == "schema_a" && m.Table == "products" {
			foundProducts = true
			s.NotNil(m.RowCount, "products should have row_count")
			s.GreaterOrEqual(*m.RowCount, int64(3))
		}
		if m.Schema == "schema_a" && m.Table == "order_items" {
			foundOrderItems = true
			s.NotNil(m.RowCount, "order_items should have row_count")
			s.GreaterOrEqual(*m.RowCount, int64(3))
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

func (s *MSSQLScrapperSuite) TestFetchQueryLogs() {
	// Run a recognisable query through the scrapper's executor so Query Store captures it.
	s.scrapper.executor.GetDb().Exec("SELECT COUNT(*) FROM schema_a.products WHERE category = 'Electronics'")

	// Force a Query Store flush. Skip if MSSQL_SA_PASSWORD is not set (no admin access).
	saPassword := os.Getenv("MSSQL_SA_PASSWORD")
	if saPassword != "" {
		saDbConf := &dwhexecmssql.MSSQLConf{
			User:      "sa",
			Password:  saPassword,
			Host:      testenv.EnvOrDefault("MSSQL_HOST", "127.0.0.1"),
			Port:      testenv.EnvOrDefaultInt("MSSQL_PORT", 1433),
			Database:  s.databaseName,
			TrustCert: true,
			Encrypt:   testenv.EnvOrDefault("MSSQL_ENCRYPT", "disable"),
		}
		saExec, err := dwhexecmssql.NewMSSQLExecutor(s.ctx, saDbConf)
		if err == nil {
			saExec.GetDb().Exec("EXEC sp_query_store_flush_db")
			saExec.Close()
		}
	}

	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	s.Require().NoError(err)

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now().Add(1 * time.Hour)

	iter, err := s.scrapper.FetchQueryLogs(s.ctx, from, to, obfuscator)
	s.Require().NoError(err)
	defer iter.Close()

	var logs []*querylogs.QueryLog
	for {
		log, iterErr := iter.Next(s.ctx)
		if iterErr != nil {
			s.Require().ErrorIs(iterErr, io.EOF, "Iterator should end with EOF, got: %v", iterErr)
			break
		}
		logs = append(logs, log)
	}

	s.NotEmpty(logs, "Should return query logs from Query Store")

	for _, log := range logs {
		s.NotEmpty(log.SQL, "SQL should not be empty")
		s.NotEmpty(log.QueryID, "QueryID should not be empty")
		s.Equal("mssql", log.SqlDialect)
		s.NotNil(log.DwhContext)
		s.Equal(s.databaseName, log.DwhContext.Database)
		s.Contains([]string{"SUCCESS", "ABORTED", "FAILED", "UNKNOWN"}, log.Status)
		s.False(log.CreatedAt.IsZero(), "CreatedAt should be set")
	}
}

// MSSQLComplianceSuite runs the standard compliance test suite.
type MSSQLComplianceSuite struct {
	scrappertest.ComplianceSuite
}

func TestMSSQLComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MSSQL compliance tests in CI")
	}
	suite.Run(t, new(MSSQLComplianceSuite))
}

func (s *MSSQLComplianceSuite) SetupSuite() {
	sc, err := newMSSQLScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MSSQL: %v", err)
	}
	s.Scrapper = sc
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
		t.Skip("Skipping MSSQL scope compliance tests in CI")
	}
	suite.Run(t, new(MSSQLScopeComplianceSuite))
}

func (s *MSSQLScopeComplianceSuite) SetupSuite() {
	sc, err := newMSSQLScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MSSQL: %v", err)
	}
	s.Scrapper = sc
}

func (s *MSSQLScopeComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		s.Scrapper.Close()
	}
}

// MSSQLMonitorComplianceSuite runs the monitor compliance checks.
type MSSQLMonitorComplianceSuite struct {
	scrappertest.MonitorComplianceSuite
}

func TestMSSQLMonitorComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MSSQL monitor compliance tests in CI")
	}
	suite.Run(t, new(MSSQLMonitorComplianceSuite))
}

func (s *MSSQLMonitorComplianceSuite) SetupSuite() {
	sc, err := newMSSQLScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MSSQL: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.MonitorComplianceConfig{
		SegmentsSQL:          `SELECT DISTINCT category as segment FROM schema_a.products`,
		CustomMetricsSQL:     `SELECT category as segment_name, CAST(SUM(price * quantity) AS FLOAT) as total_value, COUNT(*) as product_count FROM schema_a.products GROUP BY category`,
		ShapeSQL:             `SELECT id, name, price, created_at, is_active FROM schema_a.products`,
		ExpectedSegments:     []string{"Electronics", "Accessories"},
		ExpectedShapeColumns: []string{"id", "name", "price", "created_at", "is_active"},
	}
}

func (s *MSSQLMonitorComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		s.Scrapper.Close()
	}
}

// MSSQLMetricsExecutionSuite runs metrics SQL generation + execution checks.
type MSSQLMetricsExecutionSuite struct {
	scrappertest.MetricsExecutionSuite
}

func TestMSSQLMetricsExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MSSQL metrics execution tests in CI")
	}
	suite.Run(t, new(MSSQLMetricsExecutionSuite))
}

func (s *MSSQLMetricsExecutionSuite) SetupSuite() {
	sc, err := newMSSQLScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MSSQL: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.MetricsExecutionConfig{
		TableFqn:          sqldialect.TableFqn("synq_test", "schema_a", "products"),
		PartitioningField: "created_at",
		SegmentField:      "category",
		NumericField:      "price",
		TextField:         "name",
		TimeField:         "created_at",
	}
}

func (s *MSSQLMetricsExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		s.Scrapper.Close()
	}
}
