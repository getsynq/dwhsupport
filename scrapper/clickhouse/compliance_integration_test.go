package clickhouse

import (
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

	conf := ClickhouseScrapperConf{
		ClickhouseConf: dwhexecclickhouse.ClickhouseConf{
			Hostname:        host,
			Port:            testenv.EnvOrDefaultInt("CLICKHOUSE_PORT", 9000),
			Username:        os.Getenv("CLICKHOUSE_USER"),
			Password:        os.Getenv("CLICKHOUSE_PASSWORD"),
			DefaultDatabase: testenv.EnvOrDefault("CLICKHOUSE_DATABASE", "default"),
			NoSsl:           testenv.EnvOrDefaultBool("CLICKHOUSE_NO_SSL", true),
		},
		DatabaseName: testenv.EnvOrDefault("CLICKHOUSE_DATABASE", "default"),
	}

	sc, err := NewClickhouseScrapper(s.Ctx(), conf)
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
