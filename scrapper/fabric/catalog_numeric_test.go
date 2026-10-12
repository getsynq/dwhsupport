package fabric

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// FabricCatalogNumericSuite checks the precision, scale and numeric category
// QueryCatalog reports for a all_types fixture.
type FabricCatalogNumericSuite struct {
	scrappertest.CatalogNumericSuite
}

func TestFabricCatalogNumericSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Fabric catalog tests in CI")
	}
	suite.Run(t, new(FabricCatalogNumericSuite))
}

func (s *FabricCatalogNumericSuite) SetupSuite() {
	sc, err := newFabricScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Fabric: %v", err)
	}
	s.Scrapper = sc

	cDecimalP, cDecimalS := scrappertest.Sized(38, 10)
	cNumericP, cNumericS := scrappertest.Sized(18, 2)
	s.Database, s.Schema, s.Table = "", "analytics", "all_types"
	s.Want = []scrappertest.CatalogNumericColumn{
		{Column: "c_bigint", Category: scrapper.NumericCategoryInteger},
		{Column: "c_decimal", Category: scrapper.NumericCategoryDecimal, Precision: cDecimalP, Scale: cDecimalS},
		{Column: "c_numeric", Category: scrapper.NumericCategoryDecimal, Precision: cNumericP, Scale: cNumericS},
		{Column: "c_float", Category: scrapper.NumericCategoryFloat},
		{Column: "c_real", Category: scrapper.NumericCategorySinglePrecisionFloat},
		{Column: "c_varchar", Category: scrapper.NumericCategoryNone},
	}
}

func (s *FabricCatalogNumericSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
