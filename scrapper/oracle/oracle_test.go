package oracle

import (
	"context"
	_ "embed"
	"os"
	"strings"
	"testing"
	"time"

	"io"

	dwhexecoracle "github.com/getsynq/dwhsupport/exec/oracle"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/suite"
)

//go:embed testdata/init.sql
var initSQL string

// execStatements splits a SQL script on lines containing only ";" and executes each statement.
// PL/SQL blocks (BEGIN...END;) are detected and executed with the trailing semicolon.
// Regular DDL/DML statements have trailing semicolons removed.
func execStatements(db *sqlx.DB, script string) error {
	for _, stmt := range strings.Split(script, "\n;\n") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		// Strip leading SQL comments to detect PL/SQL blocks
		stripped := stmt
		for strings.HasPrefix(stripped, "--") {
			if idx := strings.Index(stripped, "\n"); idx >= 0 {
				stripped = strings.TrimSpace(stripped[idx+1:])
			} else {
				stripped = ""
				break
			}
		}
		// PL/SQL blocks need their trailing semicolons preserved
		isPLSQL := strings.HasPrefix(strings.ToUpper(stripped), "BEGIN") ||
			strings.HasPrefix(strings.ToUpper(stripped), "DECLARE")
		if !isPLSQL {
			stmt = strings.TrimRight(stmt, ";")
			stmt = strings.TrimSpace(stmt)
		}
		if stmt == "" {
			continue
		}
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func newOracleConf(user, password string) *dwhexecoracle.OracleConf {
	return &dwhexecoracle.OracleConf{
		User:        user,
		Password:    password,
		Host:        testenv.EnvOrDefault("ORACLE_HOST", "127.0.0.1"),
		Port:        testenv.EnvOrDefaultInt("ORACLE_PORT", 1521),
		ServiceName: testenv.EnvOrDefault("ORACLE_SERVICE", "FREEPDB1"),
	}
}

// setupDatabase connects as system to create schemas, fixtures, and a restricted
// synq monitoring user. Returns a scrapper connected as the synq user with
// realistic minimal permissions (catalog metadata + SELECT ANY TABLE + V$PARAMETER).
func setupDatabase(t *testing.T, ctx context.Context) *OracleScrapper {
	t.Helper()

	sysConf := newOracleConf("system", testenv.EnvOrDefault("ORACLE_SYS_PASSWORD", "SynqTest1"))

	// Connect as system to create users/schemas and fixtures.
	sysExec, err := dwhexecoracle.NewOracleExecutor(ctx, sysConf)
	if err != nil {
		t.Skipf("Skipping: could not connect to local Oracle: %v", err)
	}

	// Clean up previous test runs (ignore errors if objects don't exist).
	for _, stmt := range []string{
		"BEGIN EXECUTE IMMEDIATE 'DROP USER synq CASCADE'; EXCEPTION WHEN OTHERS THEN NULL; END;",
		"BEGIN EXECUTE IMMEDIATE 'DROP USER synq_b CASCADE'; EXCEPTION WHEN OTHERS THEN NULL; END;",
		"BEGIN EXECUTE IMMEDIATE 'DROP VIEW synq_a.order_summary'; EXCEPTION WHEN OTHERS THEN NULL; END;",
		"BEGIN EXECUTE IMMEDIATE 'DROP VIEW synq_a.active_products'; EXCEPTION WHEN OTHERS THEN NULL; END;",
		"BEGIN EXECUTE IMMEDIATE 'DROP TABLE synq_a.order_items CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;",
		"BEGIN EXECUTE IMMEDIATE 'DROP TABLE synq_a.products CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;",
	} {
		sysExec.GetDb().Exec(stmt)
	}

	if err := execStatements(sysExec.GetDb(), initSQL); err != nil {
		sysExec.Close()
		t.Fatalf("Failed to execute init SQL: %v", err)
	}
	sysExec.Close()

	// Connect as the restricted synq monitoring user.
	synqConf := &OracleScrapperConf{
		OracleConf: *newOracleConf(
			testenv.EnvOrDefault("ORACLE_USER", "synq"),
			testenv.EnvOrDefault("ORACLE_PASSWORD", "SynqTest1"),
		),
	}
	sc, err := NewOracleScrapper(ctx, synqConf)
	if err != nil {
		t.Fatalf("Failed to create Oracle scrapper as synq user: %v", err)
	}
	return sc
}

// OracleScrapperSuite tests all scrapper methods against a local Oracle instance.
// Start the database with: docker compose -f scrapper/oracle/docker-compose.yml up -d --wait
type OracleScrapperSuite struct {
	suite.Suite
	scrapper    *OracleScrapper
	ctx         context.Context
	serviceName string
}

func TestOracleScrapperSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping local Oracle tests in CI")
	}
	suite.Run(t, new(OracleScrapperSuite))
}

func (s *OracleScrapperSuite) SetupSuite() {
	s.ctx = context.Background()
	s.serviceName = testenv.EnvOrDefault("ORACLE_SERVICE", "FREEPDB1")
	s.scrapper = setupDatabase(s.T(), s.ctx)
}

func (s *OracleScrapperSuite) TearDownSuite() {
	if s.scrapper != nil {
		s.scrapper.Close()
	}
}

func (s *OracleScrapperSuite) TestQueryDatabases() {
	databases, err := s.scrapper.QueryDatabases(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(databases, "Should return at least one database (user/schema)")

	var foundA, foundB bool
	for _, db := range databases {
		s.Equal("127.0.0.1", db.Instance)
		if db.Database == "SYNQ_A" {
			foundA = true
		}
		if db.Database == "SYNQ_B" {
			foundB = true
		}
	}
	s.True(foundA, "Should find SYNQ_A user/schema")
	s.True(foundB, "Should find SYNQ_B user/schema")
}

func (s *OracleScrapperSuite) TestQueryTables() {
	tables, err := s.scrapper.QueryTables(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(tables, "Should return tables")

	var foundProducts, foundOrderItems, foundActiveProducts, foundOrderSummary bool
	var foundCustomers, foundCustomerRegions bool
	for _, t := range tables {
		s.Equal("127.0.0.1", t.Instance)
		s.Equal(s.serviceName, t.Database)

		switch {
		case t.Schema == "SYNQ_A" && t.Table == "PRODUCTS":
			foundProducts = true
			s.Equal("TABLE", t.TableType)
			s.NotNil(t.Description)
			s.Equal("Product catalog with inventory tracking", *t.Description)
		case t.Schema == "SYNQ_A" && t.Table == "ORDER_ITEMS":
			foundOrderItems = true
			s.Equal("TABLE", t.TableType)
			s.NotNil(t.Description)
			s.Equal("Line items within customer orders", *t.Description)
		case t.Schema == "SYNQ_A" && t.Table == "ACTIVE_PRODUCTS":
			foundActiveProducts = true
			s.Equal("VIEW", t.TableType)
		case t.Schema == "SYNQ_A" && t.Table == "ORDER_SUMMARY":
			foundOrderSummary = true
			s.Equal("VIEW", t.TableType)
		case t.Schema == "SYNQ_B" && t.Table == "CUSTOMERS":
			foundCustomers = true
			s.Equal("TABLE", t.TableType)
		case t.Schema == "SYNQ_B" && t.Table == "CUSTOMER_REGIONS":
			foundCustomerRegions = true
			s.Equal("VIEW", t.TableType)
		}
	}

	s.True(foundProducts, "Should find SYNQ_A.PRODUCTS")
	s.True(foundOrderItems, "Should find SYNQ_A.ORDER_ITEMS")
	s.True(foundActiveProducts, "Should find SYNQ_A.ACTIVE_PRODUCTS view")
	s.True(foundOrderSummary, "Should find SYNQ_A.ORDER_SUMMARY view")
	s.True(foundCustomers, "Should find SYNQ_B.CUSTOMERS")
	s.True(foundCustomerRegions, "Should find SYNQ_B.CUSTOMER_REGIONS view")
}

func (s *OracleScrapperSuite) TestQueryCatalog() {
	catalog, err := s.scrapper.QueryCatalog(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(catalog, "Should return catalog entries")

	var foundIdCol, foundNameCol, foundPriceCol, foundCreatedAtCol bool
	var foundIdComment, foundNameComment, foundTableComment bool
	for _, col := range catalog {
		s.Equal("127.0.0.1", col.Instance)
		s.Equal(s.serviceName, col.Database)

		if col.Schema == "SYNQ_A" && col.Table == "PRODUCTS" {
			switch col.Column {
			case "ID":
				foundIdCol = true
				s.Contains(col.Type, "NUMBER")
			case "NAME":
				foundNameCol = true
				s.Contains(col.Type, "NVARCHAR2")
			case "PRICE":
				foundPriceCol = true
				s.Contains(col.Type, "NUMBER")
			case "CREATED_AT":
				foundCreatedAtCol = true
				s.Contains(col.Type, "TIMESTAMP")
			}
			if col.Comment != nil {
				switch col.Column {
				case "ID":
					foundIdComment = true
					s.Equal("Unique product identifier", *col.Comment)
				case "NAME":
					foundNameComment = true
					s.Equal("Product display name", *col.Comment)
				}
			}
			if col.TableComment != nil && *col.TableComment == "Product catalog with inventory tracking" {
				foundTableComment = true
			}
		}
	}

	s.True(foundIdCol, "Should find ID column")
	s.True(foundNameCol, "Should find NAME column")
	s.True(foundPriceCol, "Should find PRICE column")
	s.True(foundCreatedAtCol, "Should find CREATED_AT column")
	s.True(foundIdComment, "Should find comment on ID column")
	s.True(foundNameComment, "Should find comment on NAME column")
	s.True(foundTableComment, "Should find table comment")
}

func (s *OracleScrapperSuite) TestQueryTableMetrics() {
	metrics, err := s.scrapper.QueryTableMetrics(s.ctx, time.Time{})
	s.Require().NoError(err)
	s.NotEmpty(metrics, "Should return table metrics")

	var foundProducts, foundOrderItems bool
	for _, m := range metrics {
		s.Equal("127.0.0.1", m.Instance)
		s.Equal(s.serviceName, m.Database)

		if m.Schema == "SYNQ_A" && m.Table == "PRODUCTS" {
			foundProducts = true
			s.NotNil(m.RowCount, "products should have row_count")
			s.Equal(int64(3), *m.RowCount)
		}
		if m.Schema == "SYNQ_A" && m.Table == "ORDER_ITEMS" {
			foundOrderItems = true
			s.NotNil(m.RowCount, "order_items should have row_count")
			s.Equal(int64(3), *m.RowCount)
		}
	}

	s.True(foundProducts, "Should find metrics for SYNQ_A.PRODUCTS")
	s.True(foundOrderItems, "Should find metrics for SYNQ_A.ORDER_ITEMS")
}

func (s *OracleScrapperSuite) TestQuerySqlDefinitions() {
	definitions, err := s.scrapper.QuerySqlDefinitions(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(definitions, "Should return SQL definitions")

	var foundActiveProducts, foundOrderSummary bool
	for _, def := range definitions {
		s.Equal("127.0.0.1", def.Instance)
		s.Equal(s.serviceName, def.Database)

		if def.Schema == "SYNQ_A" && def.Table == "ACTIVE_PRODUCTS" {
			foundActiveProducts = true
			s.True(def.IsView)
			s.NotEmpty(def.Sql)
			s.Contains(strings.ToUpper(def.Sql), "PRODUCTS")
		}
		if def.Schema == "SYNQ_A" && def.Table == "ORDER_SUMMARY" {
			foundOrderSummary = true
			s.True(def.IsView)
			s.NotEmpty(def.Sql)
			s.Contains(strings.ToUpper(def.Sql), "ORDER_ITEMS")
		}
	}

	s.True(foundActiveProducts, "Should find SQL definition for ACTIVE_PRODUCTS view")
	s.True(foundOrderSummary, "Should find SQL definition for ORDER_SUMMARY view")
}

func (s *OracleScrapperSuite) TestQueryTableConstraints() {
	constraints, err := s.scrapper.QueryTableConstraints(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(constraints, "Should return table constraints")

	var foundProductsPK, foundOrderItemsCompositePK bool
	var foundOrderItemsUniqueConstraint bool
	var foundCheckConstraint bool
	for _, c := range constraints {
		s.Equal("127.0.0.1", c.Instance)
		s.Equal(s.serviceName, c.Database)
		s.NotEmpty(c.ConstraintName)
		s.NotNil(c.IsEnforced, "IsEnforced should always be set for Oracle constraints")

		if c.ConstraintType != scrapper.ConstraintTypeCheck {
			s.NotEmpty(c.ColumnName)
		}

		switch {
		case c.Schema == "SYNQ_A" && c.Table == "PRODUCTS" && c.ConstraintType == scrapper.ConstraintTypePrimaryKey && c.ColumnName == "ID":
			foundProductsPK = true
			s.True(*c.IsEnforced, "PRIMARY KEY should be enforced")
		case c.Schema == "SYNQ_A" && c.Table == "ORDER_ITEMS" && c.ConstraintType == scrapper.ConstraintTypePrimaryKey:
			foundOrderItemsCompositePK = true
		case c.Schema == "SYNQ_A" && c.Table == "ORDER_ITEMS" && c.ConstraintType == scrapper.ConstraintTypeUniqueIndex:
			foundOrderItemsUniqueConstraint = true
		case c.ConstraintType == scrapper.ConstraintTypeCheck:
			foundCheckConstraint = true
			s.Empty(c.ColumnName, "CHECK constraints should have empty column name")
		}
	}

	s.True(foundProductsPK, "Should find PRIMARY KEY for PRODUCTS.ID")
	s.True(foundOrderItemsCompositePK, "Should find composite PRIMARY KEY for ORDER_ITEMS")
	s.True(foundOrderItemsUniqueConstraint, "Should find UNIQUE constraint on ORDER_ITEMS")
	s.True(foundCheckConstraint, "Should find at least one CHECK constraint")
}

func (s *OracleScrapperSuite) TestQuerySegments() {
	sql := `SELECT DISTINCT category as "segment" FROM synq_a.products`
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

func (s *OracleScrapperSuite) TestQueryCustomMetrics() {
	sql := `SELECT
		category as "segment_name",
		CAST(SUM(price * quantity) AS FLOAT) as "total_value",
		COUNT(*) as "product_count"
	FROM synq_a.products
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

func (s *OracleScrapperSuite) TestQueryShape() {
	sql := `SELECT id, name, price, created_at, is_active FROM synq_a.products`
	columns, err := s.scrapper.QueryShape(s.ctx, sql)
	s.Require().NoError(err)
	s.Require().Len(columns, 5)

	s.Equal("ID", columns[0].Name)
	s.Equal(int32(1), columns[0].Position)

	s.Equal("NAME", columns[1].Name)
	s.Equal(int32(2), columns[1].Position)

	s.Equal("PRICE", columns[2].Name)
	s.Equal(int32(3), columns[2].Position)

	s.Equal("CREATED_AT", columns[3].Name)
	s.Equal(int32(4), columns[3].Position)

	s.Equal("IS_ACTIVE", columns[4].Name)
	s.Equal(int32(5), columns[4].Position)
}

func (s *OracleScrapperSuite) TestFetchQueryLogs() {
	// Run a query through the scrapper executor to ensure V$SQL has recent entries.
	s.scrapper.executor.GetDb().Exec("SELECT COUNT(*) FROM synq_a.products WHERE category = 'Electronics'")

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

	s.NotEmpty(logs, "Should return query logs from V$SQL")

	for _, log := range logs {
		s.NotEmpty(log.SQL, "SQL should not be empty")
		s.NotEmpty(log.QueryID, "QueryID (sql_id) should not be empty")
		s.Equal("oracle", log.SqlDialect)
		s.NotNil(log.DwhContext)
		s.Equal("127.0.0.1", log.DwhContext.Instance)
		s.Equal(s.serviceName, log.DwhContext.Database)
		s.NotEmpty(log.DwhContext.Schema, "Schema (parsing_schema_name) should be set")
		s.Equal("SUCCESS", log.Status)
		s.False(log.CreatedAt.IsZero(), "CreatedAt should be set")
		s.NotEmpty(log.QueryType, "QueryType should be mapped from command_type")
	}
}

// OracleComplianceSuite runs the standard compliance test suite.
type OracleComplianceSuite struct {
	scrappertest.ComplianceSuite
}

func TestOracleComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping local Oracle compliance tests in CI")
	}
	suite.Run(t, new(OracleComplianceSuite))
}

func (s *OracleComplianceSuite) SetupSuite() {
	s.Scrapper = setupDatabase(s.T(), s.Ctx())
}

func (s *OracleComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		s.Scrapper.Close()
	}
}

// OracleScopeComplianceSuite runs the scope filtering compliance checks.
type OracleScopeComplianceSuite struct {
	scrappertest.ScopeComplianceSuite
}

func TestOracleScopeComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping local Oracle scope compliance tests in CI")
	}
	suite.Run(t, new(OracleScopeComplianceSuite))
}

func (s *OracleScopeComplianceSuite) SetupSuite() {
	s.Scrapper = setupDatabase(s.T(), s.Ctx())
}

func (s *OracleScopeComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		s.Scrapper.Close()
	}
}
