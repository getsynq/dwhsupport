package trino

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// StarburstCatalogNumericSuite checks the precision, scale and numeric category
// QueryCatalog reports for the tpcds item table.
type StarburstCatalogNumericSuite struct {
	scrappertest.CatalogNumericSuite
}

func TestStarburstCatalogNumericSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Starburst catalog tests in CI")
	}
	suite.Run(t, new(StarburstCatalogNumericSuite))
}

func (s *StarburstCatalogNumericSuite) SetupSuite() {
	if testenv.EnvOrDefault("STARBURST_HOST", "") == "" {
		s.T().Skip("STARBURST_HOST env var not set")
	}
	sc, err := newTrinoScrapperFromEnv(s.Ctx(), "tpcds")
	if err != nil {
		s.T().Skipf("Could not connect to Starburst: %v", err)
	}
	s.Scrapper = sc

	iCurrentPriceP, iCurrentPriceS := scrappertest.Sized(7, 2)
	// The tpcds connector's tables, the one sample with exact numerics.
	s.Database, s.Schema, s.Table = "tpcds", "tiny", "item"
	s.Want = []scrappertest.CatalogNumericColumn{
		{Column: "i_item_sk", Category: scrapper.NumericCategoryInteger},
		{Column: "i_current_price", Category: scrapper.NumericCategoryDecimal, Precision: iCurrentPriceP, Scale: iCurrentPriceS},
		{Column: "i_item_id", Category: scrapper.NumericCategoryNone},
	}
}

func (s *StarburstCatalogNumericSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// SelfHostedTrinoCatalogNumericSuite checks the numeric category QueryCatalog
// reports for the tpch orders table.
type SelfHostedTrinoCatalogNumericSuite struct {
	scrappertest.CatalogNumericSuite
}

func TestSelfHostedTrinoCatalogNumericSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping SelfHostedTrino catalog tests in CI")
	}
	suite.Run(t, new(SelfHostedTrinoCatalogNumericSuite))
}

func (s *SelfHostedTrinoCatalogNumericSuite) SetupSuite() {
	if testenv.EnvOrDefault("TRINO_HOST", "") == "" {
		s.T().Skip("TRINO_HOST env var not set")
	}
	sc, err := newSelfHostedTrinoScrapperFromEnv(s.Ctx(), "tpch")
	if err != nil {
		s.T().Skipf("Could not connect to self-hosted Trino: %v", err)
	}
	s.Scrapper = sc

	// The self-hosted Trino has only the tpch connector, whose numbers are
	// bigints and doubles. Starburst's tpcds covers an exact numeric.
	s.Database, s.Schema, s.Table = "tpch", "tiny", "orders"
	s.Want = []scrappertest.CatalogNumericColumn{
		{Column: "orderkey", Category: scrapper.NumericCategoryInteger},
		{Column: "totalprice", Category: scrapper.NumericCategoryFloat},
		{Column: "orderstatus", Category: scrapper.NumericCategoryNone},
	}
}

func (s *SelfHostedTrinoCatalogNumericSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
