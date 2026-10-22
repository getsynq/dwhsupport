package mssql

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
)

// MSSQLTableConstraintsSuite checks the keys and indexes of the products
// fixture: the primary key on its clustered index, and the unique and plain
// indexes created beside it, each with its index type.
type MSSQLTableConstraintsSuite struct {
	scrappertest.TableConstraintsSuite
}

func TestMSSQLTableConstraintsSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MSSQL constraint tests in CI")
	}
	suite.Run(t, new(MSSQLTableConstraintsSuite))
}

func (s *MSSQLTableConstraintsSuite) SetupSuite() {
	sc, err := newMSSQLScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MSSQL: %v", err)
	}
	s.Scrapper = sc
	products := scrappertest.FixtureTable{Schema: "schema_a", Table: "products"}
	s.Expect = []scrappertest.Constraint{
		{Table: products, Type: scrapper.ConstraintTypePrimaryKey, Columns: []string{"id"}, IsEnforced: lo.ToPtr(true), IndexType: "CLUSTERED"},
		{Table: products, Type: scrapper.ConstraintTypeUniqueIndex, Columns: []string{"sku"}, IsEnforced: lo.ToPtr(true), IndexType: "NONCLUSTERED"},
		{Table: products, Type: scrapper.ConstraintTypeIndex, Columns: []string{"category"}, IndexType: "NONCLUSTERED"},
	}
}

func (s *MSSQLTableConstraintsSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
