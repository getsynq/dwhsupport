package mysql

import (
	"context"
	"os"
	"testing"

	dwhexecmysql "github.com/getsynq/dwhsupport/exec/mysql"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// MySQLComplianceSuite runs the generic scrapper compliance checks.
type MySQLComplianceSuite struct {
	scrappertest.ComplianceSuite
}

func TestMySQLComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MySQL compliance tests in CI")
	}
	suite.Run(t, new(MySQLComplianceSuite))
}

func (s *MySQLComplianceSuite) SetupSuite() {
	host := testenv.EnvOrDefault("MYSQL_HOST", "")
	if host == "" {
		s.T().Skip("MYSQL_HOST env var not set")
	}
	sc, err := newMySQLScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MySQL: %v", err)
	}
	s.Scrapper = sc
}

func (s *MySQLComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// MySQLScopeComplianceSuite runs scope filtering compliance checks.
type MySQLScopeComplianceSuite struct {
	scrappertest.ScopeComplianceSuite
}

func TestMySQLScopeComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MySQL scope compliance tests in CI")
	}
	suite.Run(t, new(MySQLScopeComplianceSuite))
}

func (s *MySQLScopeComplianceSuite) SetupSuite() {
	host := testenv.EnvOrDefault("MYSQL_HOST", "")
	if host == "" {
		s.T().Skip("MYSQL_HOST env var not set")
	}
	sc, err := newMySQLScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MySQL: %v", err)
	}
	s.Scrapper = sc
}

func (s *MySQLScopeComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// MySQLMonitorComplianceSuite runs the monitor compliance checks.
type MySQLMonitorComplianceSuite struct {
	scrappertest.MonitorComplianceSuite
}

func TestMySQLMonitorComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MySQL monitor compliance tests in CI")
	}
	suite.Run(t, new(MySQLMonitorComplianceSuite))
}

func (s *MySQLMonitorComplianceSuite) SetupSuite() {
	host := testenv.EnvOrDefault("MYSQL_HOST", "")
	if host == "" {
		s.T().Skip("MYSQL_HOST env var not set")
	}
	sc, err := newMySQLScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MySQL: %v", err)
	}
	s.Scrapper = sc
	// MariaDB/MySQL uses flat table names with schema prefix
	s.Config = scrappertest.MonitorComplianceConfig{
		SegmentsSQL:          "SELECT DISTINCT category as segment FROM schema_a_products",
		CustomMetricsSQL:     "SELECT category as segment_name, SUM(price * quantity) as total_value, COUNT(*) as product_count FROM schema_a_products GROUP BY category",
		ShapeSQL:             "SELECT id, name, price, created_at, is_active FROM schema_a_products",
		ExpectedSegments:     []string{"Electronics", "Accessories"},
		ExpectedShapeColumns: []string{"id", "name", "price", "created_at", "is_active"},
	}
}

func (s *MySQLMonitorComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// MySQLMetricsExecutionSuite runs metrics SQL generation + execution checks.
type MySQLMetricsExecutionSuite struct {
	scrappertest.MetricsExecutionSuite
}

func TestMySQLMetricsExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MySQL metrics execution tests in CI")
	}
	suite.Run(t, new(MySQLMetricsExecutionSuite))
}

func (s *MySQLMetricsExecutionSuite) SetupSuite() {
	host := testenv.EnvOrDefault("MYSQL_HOST", "")
	if host == "" {
		s.T().Skip("MYSQL_HOST env var not set")
	}
	sc, err := newMySQLScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MySQL: %v", err)
	}
	s.Scrapper = sc
	dbName := testenv.EnvOrDefault("MYSQL_DATABASE", "synq_test")
	// MariaDB/MySQL uses flat table names, so TableFqn uses db as project, empty schema
	s.Config = scrappertest.MetricsExecutionConfig{
		TableFqn:          sqldialect.TableFqn(dbName, "", "schema_a_products"),
		PartitioningField: "created_at",
		SegmentField:      "category",
		NumericField:      "price",
		TextField:         "name",
		TimeField:         "created_at",
	}
}

func (s *MySQLMetricsExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

func newMySQLScrapperFromEnv(ctx context.Context) (*MySQLScrapper, error) {
	conf := &MySQLScrapperConf{
		MySQLConf: dwhexecmysql.MySQLConf{
			User:          testenv.EnvOrDefault("MYSQL_USER", "synq"),
			Password:      testenv.EnvOrDefault("MYSQL_PASSWORD", "SynqTest1!"),
			Host:          testenv.EnvOrDefault("MYSQL_HOST", ""),
			Port:          testenv.EnvOrDefaultInt("MYSQL_PORT", 3306),
			Database:      testenv.EnvOrDefault("MYSQL_DATABASE", "synq_test"),
			AllowInsecure: true,
		},
	}
	return NewMySQLScrapper(ctx, conf)
}
