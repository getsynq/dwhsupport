package clickhouse

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// ClickhouseCatalogNumericSuite checks the precision, scale and numeric category
// QueryCatalog reports for a products fixture.
type ClickhouseCatalogNumericSuite struct {
	scrappertest.CatalogNumericSuite
}

func TestClickhouseCatalogNumericSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Clickhouse catalog tests in CI")
	}
	suite.Run(t, new(ClickhouseCatalogNumericSuite))
}

func (s *ClickhouseCatalogNumericSuite) SetupSuite() {
	if testenv.EnvOrDefault("CLICKHOUSE_HOST", "") == "" {
		s.T().Skip("CLICKHOUSE_HOST env var not set")
	}
	sc, err := newClickhouseScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to ClickHouse: %v", err)
	}
	s.Scrapper = sc

	priceP, priceS := scrappertest.Sized(10, 2)
	s.Database, s.Schema, s.Table = "", "synq_test", "products"
	s.Want = []scrappertest.CatalogNumericColumn{
		{Column: "id", Category: scrapper.NumericCategoryInteger},
		{Column: "price", Category: scrapper.NumericCategoryDecimal, Precision: priceP, Scale: priceS},
		{Column: "weight", Category: scrapper.NumericCategoryFloat},
		{Column: "name", Category: scrapper.NumericCategoryNone},
	}
}

func (s *ClickhouseCatalogNumericSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
