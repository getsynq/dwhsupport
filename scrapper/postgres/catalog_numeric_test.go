package postgres

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// PostgresCatalogNumericSuite checks the precision, scale and numeric category
// QueryCatalog reports for a products fixture.
type PostgresCatalogNumericSuite struct {
	scrappertest.CatalogNumericSuite
}

func TestPostgresCatalogNumericSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Postgres catalog tests in CI")
	}
	suite.Run(t, new(PostgresCatalogNumericSuite))
}

func (s *PostgresCatalogNumericSuite) SetupSuite() {
	if testenv.EnvOrDefault("POSTGRES_HOST", "") == "" {
		s.T().Skip("POSTGRES_HOST env var not set")
	}
	sc, err := newPostgresScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Postgres: %v", err)
	}
	s.Scrapper = sc

	priceP, priceS := scrappertest.Sized(10, 2)
	s.Database, s.Schema, s.Table = "", "schema_a", "products"
	s.Want = []scrappertest.CatalogNumericColumn{
		{Column: "id", Category: scrapper.NumericCategoryInteger},
		{Column: "price", Category: scrapper.NumericCategoryDecimal, Precision: priceP, Scale: priceS},
		{Column: "weight", Category: scrapper.NumericCategoryFloat},
		{Column: "name", Category: scrapper.NumericCategoryNone},
	}
}

func (s *PostgresCatalogNumericSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
