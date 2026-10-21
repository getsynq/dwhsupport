package fabric

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// FabricForeignKeysSuite checks the NOT ENFORCED foreign keys of the sales
// fixtures, including order_items with two.
type FabricForeignKeysSuite struct {
	scrappertest.ForeignKeysSuite
}

func TestFabricForeignKeysSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Fabric constraint tests in CI")
	}
	suite.Run(t, new(FabricForeignKeysSuite))
}

func (s *FabricForeignKeysSuite) SetupSuite() {
	sc, err := newFabricScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Fabric: %v", err)
	}
	s.Scrapper = sc
	table := func(name string) scrappertest.FixtureTable {
		return scrappertest.FixtureTable{Schema: "sales", Table: name}
	}
	s.ForeignKeys = []scrappertest.ForeignKey{
		{Table: table("orders"), Columns: []string{"customer_id"}, Referenced: table("customers"), ReferencedColumns: []string{"customer_id"}},
		{Table: table("order_items"), Columns: []string{"order_id"}, Referenced: table("orders"), ReferencedColumns: []string{"order_id"}},
		{Table: table("order_items"), Columns: []string{"product_id"}, Referenced: table("products"), ReferencedColumns: []string{"product_id"}},
	}
}

func (s *FabricForeignKeysSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
