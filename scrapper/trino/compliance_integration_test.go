package trino

import (
	"context"
	"os"
	"testing"

	dwhexectrino "github.com/getsynq/dwhsupport/exec/trino"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// TrinoComplianceSuite runs the generic scrapper compliance checks against
// a real Trino/Starburst instance configured via environment variables.
type TrinoComplianceSuite struct {
	scrappertest.ComplianceSuite
}

func TestTrinoComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Trino compliance tests in CI")
	}
	suite.Run(t, new(TrinoComplianceSuite))
}

func (s *TrinoComplianceSuite) SetupSuite() {
	host := testenv.EnvOrDefault("STARBURST_HOST", "")
	if host == "" {
		s.T().Skip("STARBURST_HOST env var not set")
	}

	catalog := testenv.EnvOrDefault("STARBURST_CATALOG", "tpch")

	conf := &TrinoScrapperConf{
		TrinoConf: &dwhexectrino.TrinoConf{
			Host:     host,
			Port:     testenv.EnvOrDefaultInt("STARBURST_PORT", 443),
			User:     os.Getenv("STARBURST_USER"),
			Password: os.Getenv("STARBURST_PASSWORD"),
		},
		Catalogs: []string{catalog},
	}

	sc, err := NewTrinoScrapper(s.Ctx(), conf)
	if err != nil {
		s.T().Skipf("Could not connect to Trino: %v", err)
	}
	s.Scrapper = sc
}

func (s *TrinoComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// TrinoMonitorComplianceSuite runs the monitor compliance checks using TPCH data.
type TrinoMonitorComplianceSuite struct {
	scrappertest.MonitorComplianceSuite
}

func TestTrinoMonitorComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Trino monitor compliance tests in CI")
	}
	suite.Run(t, new(TrinoMonitorComplianceSuite))
}

func (s *TrinoMonitorComplianceSuite) SetupSuite() {
	host := testenv.EnvOrDefault("STARBURST_HOST", "")
	if host == "" {
		s.T().Skip("STARBURST_HOST env var not set")
	}
	testTable := testenv.EnvOrDefault("TRINO_TEST_TABLE", "")
	if testTable == "" {
		s.T().Skip("TRINO_TEST_TABLE env var not set (e.g. tpch.tiny.orders)")
	}

	catalog := testenv.EnvOrDefault("STARBURST_CATALOG", "tpch")
	sc, err := newTrinoScrapperFromEnv(s.Ctx(), catalog)
	if err != nil {
		s.T().Skipf("Could not connect to Trino: %v", err)
	}
	s.Scrapper = sc
	segmentCol := testenv.EnvOrDefault("TRINO_TEST_SEGMENT_FIELD", "orderstatus")
	numericCol := testenv.EnvOrDefault("TRINO_TEST_NUMERIC_FIELD", "totalprice")
	s.Config = scrappertest.MonitorComplianceConfig{
		SegmentsSQL:      `SELECT DISTINCT ` + segmentCol + ` as segment FROM ` + testTable,
		CustomMetricsSQL: `SELECT ` + segmentCol + ` as segment_name, SUM(` + numericCol + `) as total_value, COUNT(*) as row_count FROM ` + testTable + ` GROUP BY ` + segmentCol,
		ShapeSQL:         `SELECT * FROM ` + testTable + ` LIMIT 1`,
	}
}

func (s *TrinoMonitorComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// TrinoMetricsExecutionSuite runs metrics SQL generation + execution checks.
type TrinoMetricsExecutionSuite struct {
	scrappertest.MetricsExecutionSuite
}

func TestTrinoMetricsExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Trino metrics execution tests in CI")
	}
	suite.Run(t, new(TrinoMetricsExecutionSuite))
}

func (s *TrinoMetricsExecutionSuite) SetupSuite() {
	host := testenv.EnvOrDefault("STARBURST_HOST", "")
	if host == "" {
		s.T().Skip("STARBURST_HOST env var not set")
	}
	testTable := testenv.EnvOrDefault("TRINO_TEST_TABLE", "")
	if testTable == "" {
		s.T().Skip("TRINO_TEST_TABLE env var not set (e.g. tpch.tiny.orders)")
	}

	catalog := testenv.EnvOrDefault("STARBURST_CATALOG", "tpch")
	sc, err := newTrinoScrapperFromEnv(s.Ctx(), catalog)
	if err != nil {
		s.T().Skipf("Could not connect to Trino: %v", err)
	}
	s.Scrapper = sc
	testCatalog := testenv.EnvOrDefault("TRINO_TEST_CATALOG", "tpch")
	s.Config = scrappertest.MetricsExecutionConfig{
		TableFqn: sqldialect.TableFqn(
			testCatalog,
			testenv.EnvOrDefault("TRINO_TEST_SCHEMA", "tiny"),
			testenv.EnvOrDefault("TRINO_TEST_TABLE_NAME", "orders"),
		),
		PartitioningField: testenv.EnvOrDefault("TRINO_TEST_TIME_FIELD", "orderdate"),
		SegmentField:      testenv.EnvOrDefault("TRINO_TEST_SEGMENT_FIELD", "orderstatus"),
		NumericField:      testenv.EnvOrDefault("TRINO_TEST_NUMERIC_FIELD", "totalprice"),
		TextField:         testenv.EnvOrDefault("TRINO_TEST_SEGMENT_FIELD", "orderstatus"),
		TimeField:         testenv.EnvOrDefault("TRINO_TEST_TIME_FIELD", "orderdate"),
	}
}

func (s *TrinoMetricsExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

func newTrinoScrapperFromEnv(ctx context.Context, catalog string) (*TrinoScrapper, error) {
	conf := &TrinoScrapperConf{
		TrinoConf: &dwhexectrino.TrinoConf{
			Host:     testenv.EnvOrDefault("STARBURST_HOST", ""),
			Port:     testenv.EnvOrDefaultInt("STARBURST_PORT", 443),
			User:     os.Getenv("STARBURST_USER"),
			Password: os.Getenv("STARBURST_PASSWORD"),
		},
		Catalogs: []string{catalog},
	}
	return NewTrinoScrapper(ctx, conf)
}
