package redshift

import (
	"os"
	"strconv"
	"testing"

	dwhexecredshift "github.com/getsynq/dwhsupport/exec/redshift"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// RedshiftValueRoundTripSuite checks that values read from Redshift decode to
// what they stand for and can be written back as literals that compare equal.
type RedshiftValueRoundTripSuite struct {
	scrappertest.ValueRoundTripSuite
}

func TestRedshiftValueRoundTripSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Redshift value round-trip tests in CI")
	}
	suite.Run(t, new(RedshiftValueRoundTripSuite))
}

func (s *RedshiftValueRoundTripSuite) SetupSuite() {
	host := testenv.EnvOrDefault("REDSHIFT_HOST", "")
	if host == "" {
		s.T().Skip("REDSHIFT_HOST env var not set")
	}
	port, err := strconv.Atoi(testenv.EnvOrDefault("REDSHIFT_PORT", "5439"))
	s.Require().NoError(err)

	sc, err := NewRedshiftScrapper(s.Ctx(), &RedshiftScrapperConf{
		RedshiftConf: dwhexecredshift.RedshiftConf{
			Host:     host,
			Port:     port,
			User:     testenv.EnvOrDefault("REDSHIFT_USER", ""),
			Password: testenv.EnvOrDefault("REDSHIFT_PASSWORD", ""),
			Database: testenv.EnvOrDefault("REDSHIFT_DATABASE", "dev"),
		},
	})
	if err != nil {
		s.T().Skipf("Could not connect to Redshift: %v", err)
	}
	s.Scrapper = sc
}

func (s *RedshiftValueRoundTripSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
