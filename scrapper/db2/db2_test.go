package db2

import (
	"context"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
)

// Db2ScrapperSuite checks the scrapper against the dwhtesting seed
// (lib/db2/seed.sql in the cloud repo's dev-infra/dwhtesting).
type Db2ScrapperSuite struct {
	suite.Suite
	scrapper *Db2Scrapper
	ctx      context.Context
}

func TestDb2ScrapperSuite(t *testing.T) {
	skipInCI(t)
	suite.Run(t, new(Db2ScrapperSuite))
}

func (s *Db2ScrapperSuite) SetupSuite() {
	s.ctx = context.Background()
	sc, err := newDb2ScrapperFromEnv(s.ctx)
	if err != nil {
		s.T().Skipf("Could not connect to Db2: %v", err)
	}
	s.scrapper = sc
}

func (s *Db2ScrapperSuite) TearDownSuite() {
	if s.scrapper != nil {
		_ = s.scrapper.Close()
	}
}

func (s *Db2ScrapperSuite) TestQueryDatabases() {
	databases, err := s.scrapper.QueryDatabases(s.ctx)
	s.Require().NoError(err)
	s.Require().Len(databases, 1)
	s.Equal("TESTDB", databases[0].Database)
}

func (s *Db2ScrapperSuite) TestQuerySchemas() {
	schemas, err := s.scrapper.QuerySchemas(s.ctx)
	s.Require().NoError(err)
	names := lo.Map(schemas, func(r *scrapper.SchemaRow, _ int) string { return r.Schema })
	s.Contains(names, "DWH_SALES")
	s.Contains(names, "DWH_ANALYTICS")
	for _, n := range names {
		s.NotContains([]string{"SYSIBM", "SYSCAT", "NULLID"}, n, "system schema listed")
	}
}

func (s *Db2ScrapperSuite) TestQueryTables() {
	tables, err := s.scrapper.QueryTables(s.ctx)
	s.Require().NoError(err)
	byName := lo.KeyBy(tables, func(t *scrapper.TableRow) string { return t.Schema + "." + t.Table })

	customers := byName["DWH_SALES.CUSTOMERS"]
	s.Require().NotNil(customers)
	s.Equal("TESTDB", customers.Database)
	s.Equal("TABLE", customers.TableType)
	s.True(customers.IsTable)
	s.Require().NotNil(customers.Description)
	s.Equal("One row per customer account", *customers.Description)

	s.Equal("VIEW", byName["DWH_ANALYTICS.ORDER_ENRICHED"].TableType)
	s.True(byName["DWH_ANALYTICS.ORDER_ENRICHED"].IsView)
	s.Equal("MATERIALIZED VIEW", byName["DWH_ANALYTICS.DAILY_REVENUE"].TableType)
	s.True(byName["DWH_ANALYTICS.DAILY_REVENUE"].IsMaterializedView)
	s.Nil(byName["DWH_ANALYTICS.CUSTOMERS_ALIAS"], "aliases are not tables")
}

func (s *Db2ScrapperSuite) TestQueryCatalog() {
	catalog, err := s.scrapper.QueryCatalog(s.ctx)
	s.Require().NoError(err)
	types := map[string]string{}
	for _, c := range catalog {
		if c.Schema == "DWH_ANALYTICS" && c.Table == "TYPE_COVERAGE" {
			types[c.Column] = c.Type
			s.Equal("TESTDB", c.Database)
		}
		if c.Schema == "DWH_SALES" && c.Table == "CUSTOMERS" && c.Column == "EMAIL" {
			s.Require().NotNil(c.Comment)
			s.Equal("Primary contact e-mail, unique", *c.Comment)
			s.Equal(int32(3), c.Position)
		}
	}
	s.Equal(map[string]string{
		"ID":             "INTEGER",
		"C_SMALLINT":     "SMALLINT",
		"C_INTEGER":      "INTEGER",
		"C_BIGINT":       "BIGINT",
		"C_DECIMAL":      "DECIMAL(31,8)",
		"C_DECFLOAT16":   "DECFLOAT(16)",
		"C_DECFLOAT34":   "DECFLOAT(34)",
		"C_REAL":         "REAL",
		"C_DOUBLE":       "DOUBLE",
		"C_BOOLEAN":      "BOOLEAN",
		"C_CHAR":         "CHARACTER(10)",
		"C_VARCHAR":      "VARCHAR(200)",
		"C_CLOB":         "CLOB(1048576)",
		"C_GRAPHIC":      "GRAPHIC(5)",
		"C_VARGRAPHIC":   "VARGRAPHIC(50)",
		"C_DBCLOB":       "DBCLOB(1024)",
		"C_CHAR_FOR_BIT": "CHARACTER(8) FOR BIT DATA",
		"C_BINARY":       "BINARY(8)",
		"C_VARBINARY":    "VARBINARY(32)",
		"C_BLOB":         "BLOB(1048576)",
		"C_DATE":         "DATE",
		"C_TIME":         "TIME",
		"C_TIMESTAMP":    "TIMESTAMP",
		"C_TIMESTAMP12":  "TIMESTAMP(12)",
		"C_XML":          "XML",
		"mixedCase Col":  "VARCHAR(20)",
	}, types)
}

func (s *Db2ScrapperSuite) TestQueryTableMetrics() {
	metrics, err := s.scrapper.QueryTableMetrics(s.ctx, time.Time{})
	s.Require().NoError(err)
	byName := lo.KeyBy(metrics, func(m *scrapper.TableMetricsRow) string { return m.Schema + "." + m.Table })

	customers := byName["DWH_SALES.CUSTOMERS"]
	s.Require().NotNil(customers)
	s.Require().NotNil(customers.RowCount)
	s.EqualValues(5, *customers.RowCount)
	s.Require().NotNil(customers.SizeBytes)
	s.Positive(*customers.SizeBytes)
	s.NotNil(customers.UpdatedAt)
	s.Nil(byName["DWH_ANALYTICS.ORDER_ENRICHED"], "views have no metrics")
}

func (s *Db2ScrapperSuite) TestQuerySqlDefinitions() {
	defs, err := s.scrapper.QuerySqlDefinitions(s.ctx)
	s.Require().NoError(err)
	byName := lo.KeyBy(defs, func(d *scrapper.SqlDefinitionRow) string { return d.Schema + "." + d.Table })

	view := byName["DWH_ANALYTICS.TOP_CUSTOMERS"]
	s.Require().NotNil(view)
	s.True(view.IsView)
	s.Contains(view.Sql, "FROM DWH_ANALYTICS.ORDER_ENRICHED")

	mqt := byName["DWH_ANALYTICS.DAILY_REVENUE"]
	s.Require().NotNil(mqt)
	s.True(mqt.IsMaterializedView)
	s.Contains(mqt.Sql, "FROM DWH_SALES.ORDERS")
}

func (s *Db2ScrapperSuite) TestQueryTableConstraints() {
	constraints, err := s.scrapper.QueryTableConstraints(s.ctx)
	s.Require().NoError(err)

	find := func(table, name string) []*scrapper.TableConstraintRow {
		return lo.Filter(constraints, func(c *scrapper.TableConstraintRow, _ int) bool {
			return c.Table == table && c.ConstraintName == name
		})
	}

	pk := find("ORDER_ITEMS", "PK_ORDER_ITEMS")
	s.Require().Len(pk, 2)
	s.Equal(scrapper.ConstraintTypePrimaryKey, pk[0].ConstraintType)
	s.Equal([]string{"ORDER_ID", "LINE_NO"}, lo.Map(pk, func(c *scrapper.TableConstraintRow, _ int) string { return c.ColumnName }))
	s.Equal("TESTDB", pk[0].Database)

	notEnforced := find("ORDER_ITEMS", "FK_ITEMS_PRODUCT")
	s.Require().Len(notEnforced, 1)
	s.Equal(scrapper.ConstraintTypeForeignKey, notEnforced[0].ConstraintType)
	s.False(*notEnforced[0].IsEnforced)

	check := find("CUSTOMERS", "CK_CUSTOMERS_STATUS")
	s.Require().Len(check, 1)
	s.Equal(scrapper.ConstraintTypeCheck, check[0].ConstraintType)
	s.Contains(check[0].ConstraintExpression, "STATUS IN")

	s.Len(find("CUSTOMERS", "UQ_CUSTOMERS_EMAIL"), 1)

	partition := find("EVENTS", "PARTITION BY")
	s.Require().Len(partition, 1)
	s.Equal(scrapper.ConstraintTypePartitionBy, partition[0].ConstraintType)
	s.Equal("EVENT_TS", partition[0].ColumnName)
}

func (s *Db2ScrapperSuite) TestPermissionError() {
	_, err := s.scrapper.RunRawQuery(s.ctx, "SELECT APPLICATION_HANDLE FROM TABLE(MON_GET_CONNECTION(NULL, -2))")
	if err == nil {
		s.T().Skip("the configured user may run MON_GET_CONNECTION")
	}
	s.True(s.scrapper.IsPermissionError(err), "expected a permission error, got: %v", err)
}

type Db2ComplianceSuite struct {
	scrappertest.ComplianceSuite
}

func TestDb2ComplianceSuite(t *testing.T) {
	skipInCI(t)
	suite.Run(t, new(Db2ComplianceSuite))
}

func (s *Db2ComplianceSuite) SetupSuite() {
	sc, err := newDb2ScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Db2: %v", err)
	}
	s.Scrapper = sc
}

func (s *Db2ComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

type Db2ScopeComplianceSuite struct {
	scrappertest.ScopeComplianceSuite
}

func TestDb2ScopeComplianceSuite(t *testing.T) {
	skipInCI(t)
	suite.Run(t, new(Db2ScopeComplianceSuite))
}

func (s *Db2ScopeComplianceSuite) SetupSuite() {
	sc, err := newDb2ScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Db2: %v", err)
	}
	s.Scrapper = sc
}

func (s *Db2ScopeComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

type Db2MonitorComplianceSuite struct {
	scrappertest.MonitorComplianceSuite
}

func TestDb2MonitorComplianceSuite(t *testing.T) {
	skipInCI(t)
	suite.Run(t, new(Db2MonitorComplianceSuite))
}

func (s *Db2MonitorComplianceSuite) SetupSuite() {
	sc, err := newDb2ScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Db2: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.MonitorComplianceConfig{
		SegmentsSQL:          `SELECT DISTINCT category AS "segment" FROM dwh_sales.products`,
		CustomMetricsSQL:     `SELECT category AS "segment_name", CAST(SUM(price) AS DOUBLE) AS "total_value", COUNT(*) AS "product_count" FROM dwh_sales.products GROUP BY category`,
		ShapeSQL:             `SELECT product_id, name, price, is_active FROM dwh_sales.products`,
		ExpectedSegments:     []string{"tools", "supplies"},
		ExpectedShapeColumns: []string{"PRODUCT_ID", "NAME", "PRICE", "IS_ACTIVE"},
	}
}

func (s *Db2MonitorComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

type Db2MetricsExecutionSuite struct {
	scrappertest.MetricsExecutionSuite
}

func TestDb2MetricsExecutionSuite(t *testing.T) {
	skipInCI(t)
	suite.Run(t, new(Db2MetricsExecutionSuite))
}

func (s *Db2MetricsExecutionSuite) SetupSuite() {
	sc, err := newDb2ScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Db2: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.MetricsExecutionConfig{
		TableFqn:          sqldialect.TableFqn("TESTDB", "DWH_SALES", "ORDERS"),
		PartitioningField: "ORDER_TS",
		SegmentField:      "STATUS",
		NumericField:      "TOTAL_AMOUNT",
		TextField:         "STATUS",
		TimeField:         "ORDER_TS",
	}
}

func (s *Db2MetricsExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

type Db2SqlDialectExecutionSuite struct {
	scrappertest.SqlDialectExecutionSuite
}

func TestDb2SqlDialectExecutionSuite(t *testing.T) {
	skipInCI(t)
	suite.Run(t, new(Db2SqlDialectExecutionSuite))
}

func (s *Db2SqlDialectExecutionSuite) SetupSuite() {
	sc, err := newDb2ScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Db2: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.SqlDialectExecutionConfig{
		TableFqn:     sqldialect.TableFqn("TESTDB", "DWH_SALES", "ORDERS"),
		KeyField:     "ORDER_ID",
		SegmentField: "STATUS",
	}
}

func (s *Db2SqlDialectExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

type Db2ValueRoundTripSuite struct {
	scrappertest.ValueRoundTripSuite
}

func TestDb2ValueRoundTripSuite(t *testing.T) {
	skipInCI(t)
	suite.Run(t, new(Db2ValueRoundTripSuite))
}

func (s *Db2ValueRoundTripSuite) SetupSuite() {
	sc, err := newDb2ScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Db2: %v", err)
	}
	s.Scrapper = sc
}

func (s *Db2ValueRoundTripSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
