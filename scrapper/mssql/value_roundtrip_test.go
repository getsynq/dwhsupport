package mssql

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// MSSQLValueRoundTripSuite checks that values read from MSSQL decode to
// what they stand for and can be written back as literals that compare equal.
type MSSQLValueRoundTripSuite struct {
	scrappertest.ValueRoundTripSuite
}

func TestMSSQLValueRoundTripSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MSSQL value round-trip tests in CI")
	}
	suite.Run(t, new(MSSQLValueRoundTripSuite))
}

func (s *MSSQLValueRoundTripSuite) SetupSuite() {
	sc, err := newMSSQLScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MSSQL: %v", err)
	}
	s.Scrapper = sc
}

func (s *MSSQLValueRoundTripSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
