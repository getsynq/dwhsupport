package clickhouse

import (
	"context"
	"os"
	"testing"

	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
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
