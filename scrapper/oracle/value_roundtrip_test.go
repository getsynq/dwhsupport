package oracle

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// OracleValueRoundTripSuite checks that values read from Oracle decode to
// what they stand for and can be written back as literals that compare equal.
type OracleValueRoundTripSuite struct {
	scrappertest.ValueRoundTripSuite
}

func TestOracleValueRoundTripSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Oracle value round-trip tests in CI")
	}
	suite.Run(t, new(OracleValueRoundTripSuite))
}

func (s *OracleValueRoundTripSuite) SetupSuite() {
	sc, err := newOracleScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Oracle: %v", err)
	}
	s.Scrapper = sc
}

func (s *OracleValueRoundTripSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
