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

// ================================================================================
// MariaDB test suites (MARIADB_* env vars)
// ================================================================================

func newMariaDBScrapperFromEnv(ctx context.Context) (*MySQLScrapper, error) {
	conf := &MySQLScrapperConf{
		MySQLConf: dwhexecmysql.MySQLConf{
			User:          testenv.EnvOrDefault("MARIADB_USER", "synq"),
			Password:      testenv.EnvOrDefault("MARIADB_PASSWORD", "SynqTest1!"),
			Host:          testenv.EnvOrDefault("MARIADB_HOST", ""),
			Port:          testenv.EnvOrDefaultInt("MARIADB_PORT", 3306),
			Database:      testenv.EnvOrDefault("MARIADB_DATABASE", "synq_test"),
			AllowInsecure: true,
		},
	}
	return NewMySQLScrapper(ctx, conf)
}

type MariaDBComplianceSuite struct {
	scrappertest.ComplianceSuite
}

func TestMariaDBComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MariaDB compliance tests in CI")
	}
	suite.Run(t, new(MariaDBComplianceSuite))
}

func (s *MariaDBComplianceSuite) SetupSuite() {
	if testenv.EnvOrDefault("MARIADB_HOST", "") == "" {
		s.T().Skip("MARIADB_HOST env var not set")
	}
	sc, err := newMariaDBScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MariaDB: %v", err)
	}
	s.Scrapper = sc
}

func (s *MariaDBComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

type MariaDBScopeComplianceSuite struct {
	scrappertest.ScopeComplianceSuite
}

func TestMariaDBScopeComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MariaDB scope compliance tests in CI")
	}
	suite.Run(t, new(MariaDBScopeComplianceSuite))
}

func (s *MariaDBScopeComplianceSuite) SetupSuite() {
	if testenv.EnvOrDefault("MARIADB_HOST", "") == "" {
		s.T().Skip("MARIADB_HOST env var not set")
	}
	sc, err := newMariaDBScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MariaDB: %v", err)
	}
	s.Scrapper = sc
}

func (s *MariaDBScopeComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

type MariaDBMonitorComplianceSuite struct {
	scrappertest.MonitorComplianceSuite
}

func TestMariaDBMonitorComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MariaDB monitor compliance tests in CI")
	}
	suite.Run(t, new(MariaDBMonitorComplianceSuite))
}

func (s *MariaDBMonitorComplianceSuite) SetupSuite() {
	if testenv.EnvOrDefault("MARIADB_HOST", "") == "" {
		s.T().Skip("MARIADB_HOST env var not set")
	}
	sc, err := newMariaDBScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MariaDB: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.MonitorComplianceConfig{
		SegmentsSQL:          "SELECT DISTINCT category as segment FROM schema_a_products",
		CustomMetricsSQL:     "SELECT category as segment_name, SUM(price * quantity) as total_value, COUNT(*) as product_count FROM schema_a_products GROUP BY category",
		ShapeSQL:             "SELECT id, name, price, created_at, is_active FROM schema_a_products",
		ExpectedSegments:     []string{"Electronics", "Accessories"},
		ExpectedShapeColumns: []string{"id", "name", "price", "created_at", "is_active"},
	}
}

func (s *MariaDBMonitorComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

type MariaDBMetricsExecutionSuite struct {
	scrappertest.MetricsExecutionSuite
}

func TestMariaDBMetricsExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MariaDB metrics execution tests in CI")
	}
	suite.Run(t, new(MariaDBMetricsExecutionSuite))
}

func (s *MariaDBMetricsExecutionSuite) SetupSuite() {
	if testenv.EnvOrDefault("MARIADB_HOST", "") == "" {
		s.T().Skip("MARIADB_HOST env var not set")
	}
	sc, err := newMariaDBScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MariaDB: %v", err)
	}
	s.Scrapper = sc
	dbName := testenv.EnvOrDefault("MARIADB_DATABASE", "synq_test")
	s.Config = scrappertest.MetricsExecutionConfig{
		TableFqn:          sqldialect.TableFqn("", dbName, "schema_a_products"),
		PartitioningField: "created_at",
		SegmentField:      "category",
		NumericField:      "price",
		TextField:         "name",
		TimeField:         "created_at",
	}
}

func (s *MariaDBMetricsExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// ================================================================================
// MySQL test suites (MYSQL_* env vars)
// ================================================================================

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
	if testenv.EnvOrDefault("MYSQL_HOST", "") == "" {
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
	if testenv.EnvOrDefault("MYSQL_HOST", "") == "" {
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
	if testenv.EnvOrDefault("MYSQL_HOST", "") == "" {
		s.T().Skip("MYSQL_HOST env var not set")
	}
	sc, err := newMySQLScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MySQL: %v", err)
	}
	s.Scrapper = sc
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
	if testenv.EnvOrDefault("MYSQL_HOST", "") == "" {
		s.T().Skip("MYSQL_HOST env var not set")
	}
	sc, err := newMySQLScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MySQL: %v", err)
	}
	s.Scrapper = sc
	dbName := testenv.EnvOrDefault("MYSQL_DATABASE", "synq_test")
	s.Config = scrappertest.MetricsExecutionConfig{
		TableFqn:          sqldialect.TableFqn("", dbName, "schema_a_products"),
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
