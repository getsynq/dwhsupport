package mssql

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// MSSQLRoutinesSuite reads the scalar function and the procedure of schema_a from the dwhtesting seed.
type MSSQLRoutinesSuite struct {
	scrappertest.RoutinesSuite
}

func TestMSSQLRoutinesSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MSSQL routines tests in CI")
	}
	suite.Run(t, new(MSSQLRoutinesSuite))
}

func (s *MSSQLRoutinesSuite) SetupSuite() {
	if os.Getenv("MSSQL_HOST") == "" {
		s.T().Skip("MSSQL_HOST env var not set")
	}
	sc, err := newMSSQLScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MSSQL: %v", err)
	}
	s.Scrapper = sc
	s.Expect = []scrappertest.FixtureRoutine{
		{Schema: "schema_a", Name: "sp_order_lines", Kind: scrapper.RoutineKindProcedure, DefinitionContains: "schema_a.order_items"},
		{Schema: "schema_a", Name: "fn_net_price", Kind: scrapper.RoutineKindFunction, DefinitionContains: "@discount"},
	}
}

func (s *MSSQLRoutinesSuite) TearDownSuite() {
	if s.Scrapper != nil {
		s.Scrapper.Close()
	}
}
