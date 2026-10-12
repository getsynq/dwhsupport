package redshift

import (
	"os"
	"strconv"
	"testing"

	dwhexecredshift "github.com/getsynq/dwhsupport/exec/redshift"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// RedshiftCatalogNumericSuite checks the precision, scale and numeric category
// QueryCatalog reports for a all_types fixture.
type RedshiftCatalogNumericSuite struct {
	scrappertest.CatalogNumericSuite
}

func TestRedshiftCatalogNumericSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Redshift catalog tests in CI")
	}
	suite.Run(t, new(RedshiftCatalogNumericSuite))
}

func (s *RedshiftCatalogNumericSuite) SetupSuite() {
	host := testenv.EnvOrDefault("REDSHIFT_HOST", "")
	if host == "" {
		s.T().Skip("REDSHIFT_HOST env var not set")
	}
	port, err := strconv.Atoi(testenv.EnvOrDefault("REDSHIFT_PORT", "5439"))
	s.Require().NoError(err)
	sc, err := NewRedshiftScrapper(s.Ctx(), &RedshiftScrapperConf{
		RedshiftConf: dwhexecredshift.RedshiftConf{
			Host:     host,
			Port:     port,
			User:     testenv.EnvOrDefault("REDSHIFT_USER", ""),
			Password: testenv.EnvOrDefault("REDSHIFT_PASSWORD", ""),
			Database: testenv.EnvOrDefault("REDSHIFT_DATABASE", "dev"),
		},
	})
	if err != nil {
		s.T().Skipf("Could not connect to Redshift: %v", err)
	}
	s.Scrapper = sc

	colDecimalP, colDecimalS := scrappertest.Sized(18, 6)
	s.Database, s.Schema, s.Table = "", "pipelines", "all_types"
	s.Want = []scrappertest.CatalogNumericColumn{
		{Column: "col_bigint", Category: scrapper.NumericCategoryInteger},
		{Column: "col_decimal", Category: scrapper.NumericCategoryDecimal, Precision: colDecimalP, Scale: colDecimalS},
		{Column: "col_real", Category: scrapper.NumericCategorySinglePrecisionFloat},
		{Column: "col_double", Category: scrapper.NumericCategoryFloat},
		{Column: "col_varchar", Category: scrapper.NumericCategoryNone},
	}
}

func (s *RedshiftCatalogNumericSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
