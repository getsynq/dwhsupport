package clickhouse

import (
	"context"
	"os"
	"testing"

	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// ClickHouseComplianceSuite runs the generic scrapper compliance checks against
// a real ClickHouse instance configured via environment variables.
type ClickHouseComplianceSuite struct {
	scrappertest.ComplianceSuite
}

func TestClickHouseComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping ClickHouse compliance tests in CI")
	}
	suite.Run(t, new(ClickHouseComplianceSuite))
}

func (s *ClickHouseComplianceSuite) SetupSuite() {
	host := testenv.EnvOrDefault("CLICKHOUSE_HOST", "")
	if host == "" {
		s.T().Skip("CLICKHOUSE_HOST env var not set")
	}

	sc, err := newClickhouseScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to ClickHouse: %v", err)
	}
	s.Scrapper = sc
}

func (s *ClickHouseComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// ClickHouseScopeComplianceSuite runs scope filtering compliance checks.
type ClickHouseScopeComplianceSuite struct {
	scrappertest.ScopeComplianceSuite
}

func TestClickHouseScopeComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping ClickHouse scope compliance tests in CI")
	}
	suite.Run(t, new(ClickHouseScopeComplianceSuite))
}

func (s *ClickHouseScopeComplianceSuite) SetupSuite() {
	host := testenv.EnvOrDefault("CLICKHOUSE_HOST", "")
	if host == "" {
		s.T().Skip("CLICKHOUSE_HOST env var not set")
	}

	sc, err := newClickhouseScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to ClickHouse: %v", err)
	}
	s.Scrapper = sc
}

func (s *ClickHouseScopeComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// ClickHouseMonitorComplianceSuite runs the monitor compliance checks.
type ClickHouseMonitorComplianceSuite struct {
	scrappertest.MonitorComplianceSuite
}

func TestClickHouseMonitorComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping ClickHouse monitor compliance tests in CI")
	}
	suite.Run(t, new(ClickHouseMonitorComplianceSuite))
}

func (s *ClickHouseMonitorComplianceSuite) SetupSuite() {
	host := testenv.EnvOrDefault("CLICKHOUSE_HOST", "")
	if host == "" {
		s.T().Skip("CLICKHOUSE_HOST env var not set")
	}

	sc, err := newClickhouseScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to ClickHouse: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.MonitorComplianceConfig{
		SegmentsSQL:          `SELECT DISTINCT category as segment FROM synq_test.products`,
		CustomMetricsSQL:     `SELECT category as segment_name, SUM(price * quantity) as total_value, COUNT(*) as product_count FROM synq_test.products GROUP BY category`,
		ShapeSQL:             `SELECT id, name, price, created_at, is_active FROM synq_test.products`,
		ExpectedSegments:     []string{"Electronics", "Accessories"},
		ExpectedShapeColumns: []string{"id", "name", "price", "created_at", "is_active"},
	}
}

func (s *ClickHouseMonitorComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// ClickHouseMetricsExecutionSuite runs metrics SQL generation + execution checks.
type ClickHouseMetricsExecutionSuite struct {
	scrappertest.MetricsExecutionSuite
}

func TestClickHouseMetricsExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping ClickHouse metrics execution tests in CI")
	}
	suite.Run(t, new(ClickHouseMetricsExecutionSuite))
}

func (s *ClickHouseMetricsExecutionSuite) SetupSuite() {
	host := testenv.EnvOrDefault("CLICKHOUSE_HOST", "")
	if host == "" {
		s.T().Skip("CLICKHOUSE_HOST env var not set")
	}

	sc, err := newClickhouseScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to ClickHouse: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.MetricsExecutionConfig{
		TableFqn:          sqldialect.TableFqn("", "synq_test", "products"),
		PartitioningField: "created_at",
		SegmentField:      "category",
		NumericField:      "price",
		TextField:         "name",
		TimeField:         "created_at",
	}
}

func (s *ClickHouseMetricsExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

func newClickhouseScrapperFromEnv(ctx context.Context) (*ClickhouseScrapper, error) {
	conf := ClickhouseScrapperConf{
		ClickhouseConf: dwhexecclickhouse.ClickhouseConf{
			Hostname:        testenv.EnvOrDefault("CLICKHOUSE_HOST", ""),
			Port:            testenv.EnvOrDefaultInt("CLICKHOUSE_PORT", 9000),
			Username:        os.Getenv("CLICKHOUSE_USER"),
			Password:        os.Getenv("CLICKHOUSE_PASSWORD"),
			DefaultDatabase: testenv.EnvOrDefault("CLICKHOUSE_DATABASE", "default"),
			NoSsl:           testenv.EnvOrDefaultBool("CLICKHOUSE_NO_SSL", true),
		},
		DatabaseName: testenv.EnvOrDefault("CLICKHOUSE_DATABASE", "default"),
	}
	return NewClickhouseScrapper(ctx, conf)
}
