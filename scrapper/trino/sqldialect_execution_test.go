package trino

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

func trinoSqlDialectExecutionConfig() scrappertest.SqlDialectExecutionConfig {
	return scrappertest.SqlDialectExecutionConfig{
		TableFqn: sqldialect.TableFqn(
			testenv.EnvOrDefault("TRINO_TEST_CATALOG", "tpch"),
			testenv.EnvOrDefault("TRINO_TEST_SCHEMA", "tiny"),
			testenv.EnvOrDefault("TRINO_TEST_TABLE_NAME", "orders"),
		),
		KeyField:     "orderkey",
		SegmentField: testenv.EnvOrDefault("TRINO_TEST_SEGMENT_FIELD", "orderstatus"),
	}
}

// StarburstSqlDialectExecutionSuite runs the sqldialect builder execution
// checks against Starburst Galaxy.
type StarburstSqlDialectExecutionSuite struct {
	scrappertest.SqlDialectExecutionSuite
}

func TestStarburstSqlDialectExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Starburst sqldialect execution tests in CI")
	}
	suite.Run(t, new(StarburstSqlDialectExecutionSuite))
}

func (s *StarburstSqlDialectExecutionSuite) SetupSuite() {
	if testenv.EnvOrDefault("STARBURST_HOST", "") == "" {
		s.T().Skip("STARBURST_HOST env var not set")
	}
	sc, err := newTrinoScrapperFromEnv(s.Ctx(), testenv.EnvOrDefault("TRINO_TEST_CATALOG", "tpch"))
	if err != nil {
		s.T().Skipf("Could not connect to Starburst: %v", err)
	}
	s.Scrapper = sc
	s.Config = trinoSqlDialectExecutionConfig()
}

func (s *StarburstSqlDialectExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// SelfHostedTrinoSqlDialectExecutionSuite runs the same checks against
// self-hosted Trino, which trails Galaxy by a release or two.
type SelfHostedTrinoSqlDialectExecutionSuite struct {
	scrappertest.SqlDialectExecutionSuite
}

func TestSelfHostedTrinoSqlDialectExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping self-hosted Trino sqldialect execution tests in CI")
	}
	suite.Run(t, new(SelfHostedTrinoSqlDialectExecutionSuite))
}

func (s *SelfHostedTrinoSqlDialectExecutionSuite) SetupSuite() {
	if testenv.EnvOrDefault("TRINO_HOST", "") == "" {
		s.T().Skip("TRINO_HOST env var not set")
	}
	sc, err := newSelfHostedTrinoScrapperFromEnv(s.Ctx(), testenv.EnvOrDefault("TRINO_TEST_CATALOG", "tpch"))
	if err != nil {
		s.T().Skipf("Could not connect to self-hosted Trino: %v", err)
	}
	s.Scrapper = sc
	s.Config = trinoSqlDialectExecutionConfig()
}

func (s *SelfHostedTrinoSqlDialectExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
