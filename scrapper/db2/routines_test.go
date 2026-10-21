package db2

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// Db2RoutinesSuite reads the function and the procedure of DWH_ANALYTICS from the dwhtesting seed.
type Db2RoutinesSuite struct {
	scrappertest.RoutinesSuite
}

func TestDb2RoutinesSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Db2 routines tests in CI")
	}
	suite.Run(t, new(Db2RoutinesSuite))
}

func (s *Db2RoutinesSuite) SetupSuite() {
	if os.Getenv("DB2_HOSTNAME") == "" {
		s.T().Skip("DB2_HOSTNAME env var not set")
	}
	sc, err := newDb2ScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Db2: %v", err)
	}
	s.Scrapper = sc
	s.Expect = []scrappertest.FixtureRoutine{
		{
			Schema:             "DWH_ANALYTICS",
			Name:               "REFRESH_DAILY_REVENUE",
			Kind:               scrapper.RoutineKindProcedure,
			DefinitionContains: "DWH_ANALYTICS.DAILY_REVENUE",
		},
		{Schema: "DWH_ANALYTICS", Name: "ORDER_BAND", Kind: scrapper.RoutineKindFunction, DefinitionContains: "'large'"},
	}
}

func (s *Db2RoutinesSuite) TearDownSuite() {
	if s.Scrapper != nil {
		s.Scrapper.Close()
	}
}
