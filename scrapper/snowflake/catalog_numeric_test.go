package snowflake

import (
	"os"
	"testing"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// SnowflakeCatalogNumericSuite checks the precision, scale and numeric category
// QueryCatalog reports for the TPC-H orders sample.
type SnowflakeCatalogNumericSuite struct {
	scrappertest.CatalogNumericSuite
}

func TestSnowflakeCatalogNumericSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Snowflake catalog tests in CI")
	}
	suite.Run(t, new(SnowflakeCatalogNumericSuite))
}

func (s *SnowflakeCatalogNumericSuite) SetupSuite() {
	if os.Getenv("SNOWFLAKE_ACCOUNT") == "" {
		s.T().Skip("SNOWFLAKE_ACCOUNT env var not set")
	}
	sfConf := dwhexecsnowflake.SnowflakeConf{
		User:           os.Getenv("SNOWFLAKE_USER"),
		Password:       os.Getenv("SNOWFLAKE_PASSWORD"),
		Account:        os.Getenv("SNOWFLAKE_ACCOUNT"),
		Warehouse:      os.Getenv("SNOWFLAKE_WAREHOUSE"),
		Databases:      []string{"SNOWFLAKE_SAMPLE_DATA"},
		Role:           os.Getenv("SNOWFLAKE_ROLE"),
		PrivateKeyFile: os.Getenv("SNOWFLAKE_PRIVATE_KEY_FILE"),
	}
	if pk := os.Getenv("SNOWFLAKE_PRIVATE_KEY"); pk != "" {
		sfConf.PrivateKey = []byte(pk)
	}
	sc, err := NewSnowflakeScrapper(s.Ctx(), &SnowflakeScrapperConf{SnowflakeConf: sfConf})
	if err != nil {
		s.T().Skipf("Could not connect to Snowflake: %v", err)
	}
	s.Scrapper = sc

	orderkeyP, orderkeyS := scrappertest.Sized(38, 0)
	totalpriceP, totalpriceS := scrappertest.Sized(12, 2)
	// The sample share every Snowflake account has. Every Snowflake integer is a
	// NUMBER(38,0), and the catalog names it NUMBER whatever its size, which
	// comes from numeric_precision and numeric_scale.
	s.Database, s.Schema, s.Table = "SNOWFLAKE_SAMPLE_DATA", "TPCH_SF1", "ORDERS"
	s.Want = []scrappertest.CatalogNumericColumn{
		{Column: "O_ORDERKEY", Category: scrapper.NumericCategoryInteger, Precision: orderkeyP, Scale: orderkeyS},
		{Column: "O_TOTALPRICE", Category: scrapper.NumericCategoryDecimal, Precision: totalpriceP, Scale: totalpriceS},
		{Column: "O_ORDERSTATUS", Category: scrapper.NumericCategoryNone},
	}
}


func (s *SnowflakeCatalogNumericSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
