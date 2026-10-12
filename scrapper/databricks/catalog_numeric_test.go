package databricks

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// DatabricksCatalogNumericSuite checks the precision, scale and numeric category
// QueryCatalog reports for a orders fixture.
type DatabricksCatalogNumericSuite struct {
	scrappertest.CatalogNumericSuite
}

func TestDatabricksCatalogNumericSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Databricks catalog tests in CI")
	}
	suite.Run(t, new(DatabricksCatalogNumericSuite))
}

func (s *DatabricksCatalogNumericSuite) SetupSuite() {
	s.Scrapper = newIntegrationScrapper(s.T(), s.Ctx())

	oTotalpriceP, oTotalpriceS := scrappertest.Sized(18, 2)
	// The samples catalog every Databricks workspace has.
	s.Database, s.Schema, s.Table = "samples", "tpch", "orders"
	s.Want = []scrappertest.CatalogNumericColumn{
		{Column: "o_orderkey", Category: scrapper.NumericCategoryInteger},
		{Column: "o_totalprice", Category: scrapper.NumericCategoryDecimal, Precision: oTotalpriceP, Scale: oTotalpriceS},
		{Column: "o_orderstatus", Category: scrapper.NumericCategoryNone},
	}
}

func (s *DatabricksCatalogNumericSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
