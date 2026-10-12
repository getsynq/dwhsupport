package athena

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// AthenaCatalogNumericSuite checks the precision, scale and numeric category
// QueryCatalog reports for a products fixture.
type AthenaCatalogNumericSuite struct {
	scrappertest.CatalogNumericSuite
}

func TestAthenaCatalogNumericSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Athena catalog tests in CI")
	}
	suite.Run(t, new(AthenaCatalogNumericSuite))
}

func (s *AthenaCatalogNumericSuite) SetupSuite() {
	if os.Getenv("ATHENA_ACCESS_KEY_ID") == "" || os.Getenv("ATHENA_SECRET_ACCESS_KEY") == "" {
		s.T().Skip("ATHENA_ACCESS_KEY_ID / ATHENA_SECRET_ACCESS_KEY env vars not set")
	}
	sc, err := newAthenaScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Athena: %v", err)
	}
	s.Scrapper = sc

	priceP, priceS := scrappertest.Sized(10, 2)
	s.Database, s.Schema, s.Table = "", "synq_dwhtesting", "products"
	s.Want = []scrappertest.CatalogNumericColumn{
		{Column: "id", Category: scrapper.NumericCategoryInteger},
		{Column: "quantity", Category: scrapper.NumericCategoryInteger},
		{Column: "price", Category: scrapper.NumericCategoryDecimal, Precision: priceP, Scale: priceS},
		{Column: "name", Category: scrapper.NumericCategoryNone},
	}
}

func (s *AthenaCatalogNumericSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
