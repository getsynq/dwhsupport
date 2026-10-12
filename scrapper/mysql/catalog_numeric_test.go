package mysql

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// MySQLCatalogNumericSuite checks the precision, scale and numeric category
// QueryCatalog reports for a schema_a_products fixture.
type MySQLCatalogNumericSuite struct {
	scrappertest.CatalogNumericSuite
}

func TestMySQLCatalogNumericSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MySQL catalog tests in CI")
	}
	suite.Run(t, new(MySQLCatalogNumericSuite))
}

func (s *MySQLCatalogNumericSuite) SetupSuite() {
	if testenv.EnvOrDefault("MYSQL_HOST", "") == "" {
		s.T().Skip("MYSQL_HOST env var not set")
	}
	sc, err := newMySQLScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MySQL: %v", err)
	}
	s.Scrapper = sc

	priceP, priceS := scrappertest.Sized(10, 2)
	s.Database, s.Schema, s.Table = "", "", "schema_a_products"
	s.Want = []scrappertest.CatalogNumericColumn{
		{Column: "id", Category: scrapper.NumericCategoryInteger},
		{Column: "price", Category: scrapper.NumericCategoryDecimal, Precision: priceP, Scale: priceS},
		{Column: "weight", Category: scrapper.NumericCategorySinglePrecisionFloat},
		{Column: "name", Category: scrapper.NumericCategoryNone},
	}
}

func (s *MySQLCatalogNumericSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// MariaDBCatalogNumericSuite checks the precision, scale and numeric category
// QueryCatalog reports for a schema_a_products fixture.
type MariaDBCatalogNumericSuite struct {
	scrappertest.CatalogNumericSuite
}

func TestMariaDBCatalogNumericSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MariaDB catalog tests in CI")
	}
	suite.Run(t, new(MariaDBCatalogNumericSuite))
}

func (s *MariaDBCatalogNumericSuite) SetupSuite() {
	if testenv.EnvOrDefault("MARIADB_HOST", "") == "" {
		s.T().Skip("MARIADB_HOST env var not set")
	}
	sc, err := newMariaDBScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MariaDB: %v", err)
	}
	s.Scrapper = sc

	priceP, priceS := scrappertest.Sized(10, 2)
	s.Database, s.Schema, s.Table = "", "", "schema_a_products"
	s.Want = []scrappertest.CatalogNumericColumn{
		{Column: "id", Category: scrapper.NumericCategoryInteger},
		{Column: "price", Category: scrapper.NumericCategoryDecimal, Precision: priceP, Scale: priceS},
		{Column: "weight", Category: scrapper.NumericCategorySinglePrecisionFloat},
		{Column: "name", Category: scrapper.NumericCategoryNone},
	}
}

func (s *MariaDBCatalogNumericSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
