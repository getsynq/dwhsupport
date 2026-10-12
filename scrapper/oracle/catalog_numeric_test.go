package oracle

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// OracleCatalogNumericSuite checks the precision, scale and numeric category
// QueryCatalog reports for a PRODUCTS fixture.
type OracleCatalogNumericSuite struct {
	scrappertest.CatalogNumericSuite
}

func TestOracleCatalogNumericSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Oracle catalog tests in CI")
	}
	suite.Run(t, new(OracleCatalogNumericSuite))
}

func (s *OracleCatalogNumericSuite) SetupSuite() {
	sc, err := newOracleScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Oracle: %v", err)
	}
	s.Scrapper = sc

	idP, idS := scrappertest.Sized(10, 0)
	priceP, priceS := scrappertest.Sized(10, 2)
	isActiveP, isActiveS := scrappertest.Sized(1, 0)
	// Oracle's FLOAT is a NUMBER whose precision is in bits, so it has no size
	// in decimal digits.
	s.Database, s.Schema, s.Table = "", "SYNQ_A", "PRODUCTS"
	s.Want = []scrappertest.CatalogNumericColumn{
		{Column: "ID", Category: scrapper.NumericCategoryInteger, Precision: idP, Scale: idS},
		{Column: "PRICE", Category: scrapper.NumericCategoryDecimal, Precision: priceP, Scale: priceS},
		{Column: "IS_ACTIVE", Category: scrapper.NumericCategoryInteger, Precision: isActiveP, Scale: isActiveS},
		{Column: "WEIGHT", Category: scrapper.NumericCategoryDecimal},
		{Column: "NAME", Category: scrapper.NumericCategoryNone},
	}
}

func (s *OracleCatalogNumericSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
