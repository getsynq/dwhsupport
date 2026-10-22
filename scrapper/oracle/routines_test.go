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

// TestRoutines_NoOracleMaintainedSchema holds the listing to the schemas a
// customer owns: Oracle installs routines of its own (Database Vault's DVF
// factor functions among them) that are no asset of anyone's.
func (s *OracleRoutinesSuite) TestRoutines_NoOracleMaintainedSchema() {
	sc := s.Scrapper.(*OracleScrapper)
	var maintained []string
	s.Require().NoError(sc.executor.GetDb().SelectContext(s.Ctx(), &maintained,
		`SELECT USERNAME FROM ALL_USERS WHERE ORACLE_MAINTAINED = 'Y'`))
	s.Require().NotEmpty(maintained)

	routines, err := s.Scrapper.QueryRoutines(s.Ctx())
	s.Require().NoError(err)
	for _, routine := range routines {
		s.NotContains(maintained, routine.Schema, "%s.%s is Oracle's own", routine.Schema, routine.Name)
	}
}
