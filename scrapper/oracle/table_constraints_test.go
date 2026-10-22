package oracle

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
)

// OracleTableConstraintsSuite checks the indexes of the seed that back no
// constraint, and that the index backing a primary key is not reported again.
type OracleTableConstraintsSuite struct {
	scrappertest.TableConstraintsSuite
}

func TestOracleTableConstraintsSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Oracle constraint tests in CI")
	}
	suite.Run(t, new(OracleTableConstraintsSuite))
}

func (s *OracleTableConstraintsSuite) SetupSuite() {
	sc, err := newOracleScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Oracle: %v", err)
	}
	s.Scrapper = sc
	products := scrappertest.FixtureTable{Schema: "SYNQ_A", Table: "PRODUCTS"}
	orderItems := scrappertest.FixtureTable{Schema: "SYNQ_A", Table: "ORDER_ITEMS"}
	customers := scrappertest.FixtureTable{Schema: "SYNQ_B", Table: "CUSTOMERS"}
	s.Expect = []scrappertest.Constraint{
		{Table: products, Type: scrapper.ConstraintTypePrimaryKey, Columns: []string{"ID"}, IsEnforced: lo.ToPtr(true), IndexType: "NORMAL"},
		{Table: products, Type: scrapper.ConstraintTypeUniqueIndex, Columns: []string{"SKU"}, IndexType: "NORMAL"},
		{Table: products, Type: scrapper.ConstraintTypeIndex, Columns: []string{"CATEGORY"}, IndexType: "NORMAL"},
		// a function-based index's column is its expression, not the hidden SYS_NC column the index lists
		{
			Table:      products,
			Type:       scrapper.ConstraintTypeIndex,
			Columns:    []string{`UPPER("NAME")`},
			IsEnforced: lo.ToPtr(true),
			IndexType:  "FUNCTION-BASED NORMAL",
		},
		{Table: products, Type: scrapper.ConstraintTypeIndex, Columns: []string{"CREATED_AT"}},
		{Table: customers, Type: scrapper.ConstraintTypeUniqueIndex, Columns: []string{"EMAIL"}},
		{Table: orderItems, Type: scrapper.ConstraintTypeUniqueIndex, Columns: []string{"ORDER_ID", "PRODUCT_ID"}},
	}
	s.Unexpected = []scrappertest.Constraint{
		{Table: products, Type: scrapper.ConstraintTypeUniqueIndex, Columns: []string{"ID"}},
		{Table: orderItems, Type: scrapper.ConstraintTypeUniqueIndex, Columns: []string{"ORDER_ID", "ITEM_ID"}},
	}
}

func (s *OracleTableConstraintsSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
