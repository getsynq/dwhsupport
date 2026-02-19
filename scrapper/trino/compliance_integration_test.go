package trino

import (
	"os"
	"testing"

	dwhexectrino "github.com/getsynq/dwhsupport/exec/trino"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
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
