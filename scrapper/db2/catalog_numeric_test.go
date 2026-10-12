package db2

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// Db2CatalogNumericSuite checks the precision, scale and numeric category
// QueryCatalog reports for a TYPE_COVERAGE fixture.
type Db2CatalogNumericSuite struct {
	scrappertest.CatalogNumericSuite
}

func TestDb2CatalogNumericSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Db2 catalog tests in CI")
	}
	suite.Run(t, new(Db2CatalogNumericSuite))
}

func (s *Db2CatalogNumericSuite) SetupSuite() {
	if testenv.EnvOrDefault("DB2_HOSTNAME", "") == "" {
		s.T().Skip("DB2_HOSTNAME env var not set")
	}
	sc, err := newDb2ScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Db2: %v", err)
	}
	s.Scrapper = sc

	cDecimalP, cDecimalS := scrappertest.Sized(31, 8)
	s.Database, s.Schema, s.Table = "", "DWH_ANALYTICS", "TYPE_COVERAGE"
	s.Want = []scrappertest.CatalogNumericColumn{
		{Column: "C_BIGINT", Category: scrapper.NumericCategoryInteger},
		{Column: "C_DECIMAL", Category: scrapper.NumericCategoryDecimal, Precision: cDecimalP, Scale: cDecimalS},
		{Column: "C_DECFLOAT34", Category: scrapper.NumericCategoryDecimal},
		{Column: "C_REAL", Category: scrapper.NumericCategorySinglePrecisionFloat},
		{Column: "C_DOUBLE", Category: scrapper.NumericCategoryFloat},
		{Column: "C_VARCHAR", Category: scrapper.NumericCategoryNone},
	}
}

func (s *Db2CatalogNumericSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
