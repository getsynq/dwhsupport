package oracle

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// OracleForeignKeysSuite checks the foreign key of the ORDER_ITEMS fixture.
type OracleForeignKeysSuite struct {
	scrappertest.ForeignKeysSuite
}

func TestOracleForeignKeysSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Oracle constraint tests in CI")
	}
	suite.Run(t, new(OracleForeignKeysSuite))
}

func (s *OracleForeignKeysSuite) SetupSuite() {
	sc, err := newOracleScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Oracle: %v", err)
	}
	s.Scrapper = sc
	s.ForeignKeys = []scrappertest.ForeignKey{{
		Table:             scrappertest.FixtureTable{Schema: "SYNQ_A", Table: "ORDER_ITEMS"},
		Columns:           []string{"PRODUCT_ID"},
		Referenced:        scrappertest.FixtureTable{Schema: "SYNQ_A", Table: "PRODUCTS"},
		ReferencedColumns: []string{"ID"},
	}}
}

func (s *OracleForeignKeysSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
