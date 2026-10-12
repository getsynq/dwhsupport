package bigquery

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// BigQueryCatalogNumericSuite checks the precision, scale and numeric category
// QueryCatalog reports for a products fixture.
type BigQueryCatalogNumericSuite struct {
	scrappertest.CatalogNumericSuite
}

func TestBigQueryCatalogNumericSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping BigQuery catalog tests in CI")
	}
	suite.Run(t, new(BigQueryCatalogNumericSuite))
}

func (s *BigQueryCatalogNumericSuite) SetupSuite() {
	if os.Getenv("BIGQUERY_CREDENTIALS_FILE") == "" {
		s.T().Skip("BIGQUERY_CREDENTIALS_FILE env var not set")
	}
	sc, err := newBigQueryScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to BigQuery: %v", err)
	}
	s.Scrapper = sc

	priceP, priceS := scrappertest.Sized(38, 9)
	bigPriceP, bigPriceS := scrappertest.Sized(76, 38)
	// An unparameterised NUMERIC and BIGNUMERIC have the type's fixed size, as
	// QueryShape reports it.
	s.Database, s.Schema, s.Table = "", "synq_dwhtesting", "products"
	s.Want = []scrappertest.CatalogNumericColumn{
		{Column: "price", Category: scrapper.NumericCategoryDecimal, Precision: priceP, Scale: priceS},
		{Column: "big_price", Category: scrapper.NumericCategoryDecimal, Precision: bigPriceP, Scale: bigPriceS},
		{Column: "weight_kg", Category: scrapper.NumericCategoryFloat},
		{Column: "name", Category: scrapper.NumericCategoryNone},
	}
}

func (s *BigQueryCatalogNumericSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
