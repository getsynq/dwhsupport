package fabric

import (
	"context"
	"os"
	"testing"
	"time"

	dwhexecfabric "github.com/getsynq/dwhsupport/exec/fabric"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// newFabricScrapperFromEnv builds a scrapper from FABRIC_* env vars. These point
// at the COALESCE_QUALITY_DWHTESTING Fabric Warehouse seeded by
// dev-infra/dwhtesting/lib/fabric/seed.sql. Reaching it requires an Entra
// service principal (see dwhtesting/lib/fabric/TODO_SCRAPER_AUTH.md); without
// credentials the suites skip.
func newFabricScrapperFromEnv(ctx context.Context) (*FabricScrapper, error) {
	conf := &FabricScrapperConf{
		FabricConf: dwhexecfabric.FabricConf{
			Host:         testenv.EnvOrDefault("FABRIC_HOST", ""),
			Database:     testenv.EnvOrDefault("FABRIC_DATABASE", "COALESCE_QUALITY_DWHTESTING"),
			ClientID:     testenv.EnvOrDefault("FABRIC_CLIENT_ID", ""),
			ClientSecret: testenv.EnvOrDefault("FABRIC_CLIENT_SECRET", ""),
			TenantID:     testenv.EnvOrDefault("FABRIC_TENANT_ID", ""),
			AccessToken:  testenv.EnvOrDefault("FABRIC_ACCESS_TOKEN", ""),
		},
	}
	return NewFabricScrapper(ctx, conf)
}

// FabricScrapperSuite tests all scrapper methods against the seeded Fabric
// Warehouse fixture.
type FabricScrapperSuite struct {
	suite.Suite
	scrapper     *FabricScrapper
	ctx          context.Context
	databaseName string
}

func TestFabricScrapperSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Fabric tests in CI")
	}
	suite.Run(t, new(FabricScrapperSuite))
}

func (s *FabricScrapperSuite) SetupSuite() {
	s.ctx = context.Background()
	s.databaseName = testenv.EnvOrDefault("FABRIC_DATABASE", "COALESCE_QUALITY_DWHTESTING")
	sc, err := newFabricScrapperFromEnv(s.ctx)
	if err != nil {
		s.T().Skipf("Could not connect to Fabric: %v", err)
	}
	s.scrapper = sc
}

func (s *FabricScrapperSuite) TearDownSuite() {
	if s.scrapper != nil {
		s.scrapper.Close()
	}
}

func (s *FabricScrapperSuite) TestQueryDatabases() {
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

func (s *FabricScrapperSuite) TestQueryTables() {
	tables, err := s.scrapper.QueryTables(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(tables, "Should return tables")

	var foundProducts, foundCustomers, foundOrderItems bool
	var foundActiveProducts, foundOrderEnriched, foundProductRevenue bool
	for _, t := range tables {
		s.Equal(s.databaseName, t.Database)

		switch {
		case t.Schema == "sales" && t.Table == "products":
			foundProducts = true
			s.Equal("BASE TABLE", t.TableType)
		case t.Schema == "sales" && t.Table == "customers":
			foundCustomers = true
			s.Equal("BASE TABLE", t.TableType)
		case t.Schema == "sales" && t.Table == "order_items":
			foundOrderItems = true
			s.Equal("BASE TABLE", t.TableType)
		case t.Schema == "analytics" && t.Table == "active_products":
			foundActiveProducts = true
			s.Equal("VIEW", t.TableType)
		case t.Schema == "analytics" && t.Table == "order_enriched":
			foundOrderEnriched = true
			s.Equal("VIEW", t.TableType)
		case t.Schema == "analytics" && t.Table == "product_revenue":
			// CTAS-produced tables surface as base tables.
			foundProductRevenue = true
			s.Equal("BASE TABLE", t.TableType)
		}
	}

	s.True(foundProducts, "Should find sales.products")
	s.True(foundCustomers, "Should find sales.customers")
	s.True(foundOrderItems, "Should find sales.order_items")
	s.True(foundActiveProducts, "Should find analytics.active_products view")
	s.True(foundOrderEnriched, "Should find analytics.order_enriched view")
	s.True(foundProductRevenue, "Should find analytics.product_revenue CTAS table")
}

func (s *FabricScrapperSuite) TestQueryCatalog() {
	catalog, err := s.scrapper.QueryCatalog(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(catalog, "Should return catalog entries")

	var foundProductId, foundSku, foundUnitPrice, foundCreatedAt bool
	var foundVarcharMax bool
	for _, col := range catalog {
		s.Equal(s.databaseName, col.Database)

		if col.Schema == "sales" && col.Table == "products" {
			switch col.Column {
			case "product_id":
				foundProductId = true
				s.Contains(col.Type, "bigint")
			case "sku":
				foundSku = true
				s.Contains(col.Type, "varchar")
			case "unit_price":
				foundUnitPrice = true
				s.Contains(col.Type, "decimal")
			case "created_at":
				foundCreatedAt = true
				s.Contains(col.Type, "datetime2")
			}
		}

		// all_types carries one column per supported Fabric type, incl. VARCHAR(MAX).
		if col.Schema == "analytics" && col.Table == "all_types" && col.Column == "c_varcharmax" {
			foundVarcharMax = true
			s.Equal("varchar(MAX)", col.Type)
		}
	}

	s.True(foundProductId, "Should find products.product_id (bigint)")
	s.True(foundSku, "Should find products.sku (varchar)")
	s.True(foundUnitPrice, "Should find products.unit_price (decimal)")
	s.True(foundCreatedAt, "Should find products.created_at (datetime2)")
	s.True(foundVarcharMax, "Should find all_types.c_varcharmax as varchar(MAX)")
}

func (s *FabricScrapperSuite) TestQueryTableMetrics() {
	metrics, err := s.scrapper.QueryTableMetrics(s.ctx, time.Time{})
	s.Require().NoError(err)
	s.NotEmpty(metrics, "Should return table metrics")

	var foundProducts, foundCustomers bool
	for _, m := range metrics {
		s.Equal(s.databaseName, m.Database)

		if m.Schema == "sales" && m.Table == "products" {
			foundProducts = true
			s.NotNil(m.RowCount, "products should have row_count")
			s.GreaterOrEqual(*m.RowCount, int64(4))
		}
		if m.Schema == "sales" && m.Table == "customers" {
			foundCustomers = true
			s.NotNil(m.RowCount, "customers should have row_count")
			s.GreaterOrEqual(*m.RowCount, int64(3))
		}
	}

	s.True(foundProducts, "Should find metrics for sales.products")
	s.True(foundCustomers, "Should find metrics for sales.customers")
}

func (s *FabricScrapperSuite) TestQuerySqlDefinitions() {
	definitions, err := s.scrapper.QuerySqlDefinitions(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(definitions, "Should return SQL definitions")

	var foundActiveProducts, foundOrderEnriched bool
	for _, def := range definitions {
		s.Equal(s.databaseName, def.Database)

		if def.Schema == "analytics" && def.Table == "active_products" {
			foundActiveProducts = true
			s.True(def.IsView)
			s.NotEmpty(def.Sql)
			s.Contains(def.Sql, "products")
			s.Contains(def.Sql, "is_active")
		}
		if def.Schema == "analytics" && def.Table == "order_enriched" {
			foundOrderEnriched = true
			s.True(def.IsView)
			s.NotEmpty(def.Sql)
			s.Contains(def.Sql, "order_items")
		}
	}

	s.True(foundActiveProducts, "Should find SQL definition for active_products view")
	s.True(foundOrderEnriched, "Should find SQL definition for order_enriched view")
}

func (s *FabricScrapperSuite) TestQueryTableConstraints() {
	constraints, err := s.scrapper.QueryTableConstraints(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(constraints, "Should return table constraints")

	var foundProductsPK, foundProductsSkuUnique bool
	var foundOrdersFK, foundOrderItemsPK bool
	for _, c := range constraints {
		s.Equal(s.databaseName, c.Database)
		s.NotEmpty(c.ConstraintName)
		s.NotEmpty(c.ColumnName)
		// Fabric constraints are always informational (NOT ENFORCED).
		s.NotNil(c.IsEnforced)
		if c.IsEnforced != nil {
			s.False(*c.IsEnforced, "Fabric constraints are NOT ENFORCED")
		}

		switch {
		case c.Schema == "sales" && c.Table == "products" && c.ConstraintType == scrapper.ConstraintTypePrimaryKey && c.ColumnName == "product_id":
			foundProductsPK = true
		case c.Schema == "sales" && c.Table == "products" && c.ConstraintType == scrapper.ConstraintTypeUniqueIndex && c.ColumnName == "sku":
			foundProductsSkuUnique = true
		case c.Schema == "sales" && c.Table == "orders" && c.ConstraintType == scrapper.ConstraintTypeForeignKey && c.ColumnName == "customer_id":
			foundOrdersFK = true
		case c.Schema == "sales" && c.Table == "order_items" && c.ConstraintType == scrapper.ConstraintTypePrimaryKey && c.ColumnName == "order_item_id":
			foundOrderItemsPK = true
		}
	}

	s.True(foundProductsPK, "Should find PRIMARY KEY for products.product_id")
	s.True(foundProductsSkuUnique, "Should find UNIQUE for products.sku")
	s.True(foundOrdersFK, "Should find FOREIGN KEY for orders.customer_id")
	s.True(foundOrderItemsPK, "Should find PRIMARY KEY for order_items.order_item_id")
}

// FabricComplianceSuite runs the standard compliance test suite.
type FabricComplianceSuite struct {
	scrappertest.ComplianceSuite
}

func TestFabricComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Fabric compliance tests in CI")
	}
	suite.Run(t, new(FabricComplianceSuite))
}

func (s *FabricComplianceSuite) SetupSuite() {
	sc, err := newFabricScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Fabric: %v", err)
	}
	s.Scrapper = sc
}

func (s *FabricComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		s.Scrapper.Close()
	}
}

// FabricScopeComplianceSuite runs the scope filtering compliance checks.
type FabricScopeComplianceSuite struct {
	scrappertest.ScopeComplianceSuite
}

func TestFabricScopeComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Fabric scope compliance tests in CI")
	}
	suite.Run(t, new(FabricScopeComplianceSuite))
}

func (s *FabricScopeComplianceSuite) SetupSuite() {
	sc, err := newFabricScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Fabric: %v", err)
	}
	s.Scrapper = sc
}

func (s *FabricScopeComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		s.Scrapper.Close()
	}
}

// FabricMonitorComplianceSuite runs the monitor compliance checks.
type FabricMonitorComplianceSuite struct {
	scrappertest.MonitorComplianceSuite
}

func TestFabricMonitorComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Fabric monitor compliance tests in CI")
	}
	suite.Run(t, new(FabricMonitorComplianceSuite))
}

func (s *FabricMonitorComplianceSuite) SetupSuite() {
	sc, err := newFabricScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Fabric: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.MonitorComplianceConfig{
		SegmentsSQL:          `SELECT DISTINCT category as segment FROM sales.products`,
		CustomMetricsSQL:     `SELECT category as segment_name, CAST(SUM(unit_price) AS FLOAT) as total_value, COUNT(*) as product_count FROM sales.products GROUP BY category`,
		ShapeSQL:             `SELECT product_id, sku, name, unit_price, is_active FROM sales.products`,
		ExpectedSegments:     []string{"tools", "gadgets"},
		ExpectedShapeColumns: []string{"product_id", "sku", "name", "unit_price", "is_active"},
	}
}

func (s *FabricMonitorComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		s.Scrapper.Close()
	}
}

// FabricMetricsExecutionSuite runs metrics SQL generation + execution checks.
type FabricMetricsExecutionSuite struct {
	scrappertest.MetricsExecutionSuite
}

func TestFabricMetricsExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Fabric metrics execution tests in CI")
	}
	suite.Run(t, new(FabricMetricsExecutionSuite))
}

func (s *FabricMetricsExecutionSuite) SetupSuite() {
	sc, err := newFabricScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Fabric: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.MetricsExecutionConfig{
		TableFqn:          sqldialect.TableFqn(testenv.EnvOrDefault("FABRIC_DATABASE", "COALESCE_QUALITY_DWHTESTING"), "sales", "products"),
		PartitioningField: "created_at",
		SegmentField:      "category",
		NumericField:      "unit_price",
		TextField:         "name",
		TimeField:         "created_at",
	}
}

func (s *FabricMetricsExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		s.Scrapper.Close()
	}
}
