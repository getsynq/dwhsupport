package oracle

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// OracleRoutinesSuite reads the procedure and the function of synq_a from the dwhtesting seed.
type OracleRoutinesSuite struct {
	scrappertest.RoutinesSuite
}

func TestOracleRoutinesSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Oracle routines tests in CI")
	}
	suite.Run(t, new(OracleRoutinesSuite))
}

func (s *OracleRoutinesSuite) SetupSuite() {
	if os.Getenv("ORACLE_HOST") == "" {
		s.T().Skip("ORACLE_HOST env var not set")
	}
	sc, err := newOracleScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Oracle: %v", err)
	}
	s.Scrapper = sc
	s.Expect = []scrappertest.FixtureRoutine{
		{Schema: "SYNQ_A", Name: "ACTIVE_ORDER_TOTAL", Kind: scrapper.RoutineKindProcedure, DefinitionContains: "synq_a.products"},
		{Schema: "SYNQ_A", Name: "NET_PRICE", Kind: scrapper.RoutineKindFunction, DefinitionContains: "p_discount"},
	}
}

func (s *OracleRoutinesSuite) TearDownSuite() {
	if s.Scrapper != nil {
		s.Scrapper.Close()
	}
}
