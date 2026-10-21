package fabric

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// FabricRoutinesSuite reads the analytics schema's procedure, scalar function and inline table-valued function from the dwhtesting seed.
type FabricRoutinesSuite struct {
	scrappertest.RoutinesSuite
}

func TestFabricRoutinesSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Fabric routines tests in CI")
	}
	suite.Run(t, new(FabricRoutinesSuite))
}

func (s *FabricRoutinesSuite) SetupSuite() {
	if os.Getenv("FABRIC_HOST") == "" {
		s.T().Skip("FABRIC_HOST env var not set")
	}
	sc, err := newFabricScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Fabric: %v", err)
	}
	s.Scrapper = sc
	s.Expect = []scrappertest.FixtureRoutine{
		{Schema: "analytics", Name: "sp_rebuild_revenue", Kind: scrapper.RoutineKindProcedure, DefinitionContains: "analytics.product_revenue"},
		{Schema: "analytics", Name: "fn_net_amount", Kind: scrapper.RoutineKindFunction, DefinitionContains: "@discount"},
		{Schema: "analytics", Name: "fn_orders_for_product", Kind: scrapper.RoutineKindFunction, DefinitionContains: "sales.order_items"},
	}
}

func (s *FabricRoutinesSuite) TearDownSuite() {
	if s.Scrapper != nil {
		s.Scrapper.Close()
	}
}
