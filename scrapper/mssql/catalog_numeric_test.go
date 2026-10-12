package mssql

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// MSSQLCatalogNumericSuite checks the precision, scale and numeric category
// QueryCatalog reports for a products fixture.
type MSSQLCatalogNumericSuite struct {
	scrappertest.CatalogNumericSuite
}

func TestMSSQLCatalogNumericSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MSSQL catalog tests in CI")
	}
	suite.Run(t, new(MSSQLCatalogNumericSuite))
}

func (s *MSSQLCatalogNumericSuite) SetupSuite() {
	sc, err := newMSSQLScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MSSQL: %v", err)
	}
	s.Scrapper = sc

	priceP, priceS := scrappertest.Sized(10, 2)
	s.Database, s.Schema, s.Table = "", "schema_a", "products"
	s.Want = []scrappertest.CatalogNumericColumn{
		{Column: "id", Category: scrapper.NumericCategoryInteger},
		{Column: "price", Category: scrapper.NumericCategoryDecimal, Precision: priceP, Scale: priceS},
		{Column: "weight", Category: scrapper.NumericCategoryFloat},
		{Column: "is_active", Category: scrapper.NumericCategoryNone},
		{Column: "name", Category: scrapper.NumericCategoryNone},
	}
}

func (s *MSSQLCatalogNumericSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
