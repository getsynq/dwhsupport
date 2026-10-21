package db2

import (
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// Db2ForeignKeysSuite checks the foreign keys of the DWH_SALES fixtures, one
// of them NOT ENFORCED.
type Db2ForeignKeysSuite struct {
	scrappertest.ForeignKeysSuite
}

func TestDb2ForeignKeysSuite(t *testing.T) {
	skipInCI(t)
	suite.Run(t, new(Db2ForeignKeysSuite))
}

func (s *Db2ForeignKeysSuite) SetupSuite() {
	if testenv.EnvOrDefault("DB2_HOSTNAME", "") == "" {
		s.T().Skip("DB2_HOSTNAME env var not set")
	}
	sc, err := newDb2ScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Db2: %v", err)
	}
	s.Scrapper = sc
	table := func(name string) scrappertest.FixtureTable {
		return scrappertest.FixtureTable{Schema: "DWH_SALES", Table: name}
	}
	s.ForeignKeys = []scrappertest.ForeignKey{
		{Table: table("ORDERS"), Columns: []string{"CUSTOMER_ID"}, Referenced: table("CUSTOMERS"), ReferencedColumns: []string{"CUSTOMER_ID"}},
		{Table: table("ORDER_ITEMS"), Columns: []string{"ORDER_ID"}, Referenced: table("ORDERS"), ReferencedColumns: []string{"ORDER_ID"}},
		{Table: table("ORDER_ITEMS"), Columns: []string{"PRODUCT_ID"}, Referenced: table("PRODUCTS"), ReferencedColumns: []string{"PRODUCT_ID"}},
	}
}

func (s *Db2ForeignKeysSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
