package mssql

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// MSSQLForeignKeysSuite checks the foreign key of the order_items fixture.
type MSSQLForeignKeysSuite struct {
	scrappertest.ForeignKeysSuite
}

func TestMSSQLForeignKeysSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MSSQL constraint tests in CI")
	}
	suite.Run(t, new(MSSQLForeignKeysSuite))
}

func (s *MSSQLForeignKeysSuite) SetupSuite() {
	sc, err := newMSSQLScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MSSQL: %v", err)
	}
	s.Scrapper = sc
	s.ForeignKeys = []scrappertest.ForeignKey{{
		Table:             scrappertest.FixtureTable{Schema: "schema_a", Table: "order_items"},
		Columns:           []string{"product_id"},
		Referenced:        scrappertest.FixtureTable{Schema: "schema_a", Table: "products"},
		ReferencedColumns: []string{"id"},
	}}
}

func (s *MSSQLForeignKeysSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
