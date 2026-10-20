package snowflake

import (
	"os"
	"testing"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// SnowflakeCatalogFactsSuite checks column nullability on the TPC-DS sample,
// and owner, creation time and size on a table of the test database: a table
// of the sample share has no owner in the consuming account.
type SnowflakeCatalogFactsSuite struct {
	scrappertest.CatalogFactsSuite
}

func TestSnowflakeCatalogFactsSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Snowflake catalog tests in CI")
	}
	suite.Run(t, new(SnowflakeCatalogFactsSuite))
}

func (s *SnowflakeCatalogFactsSuite) SetupSuite() {
	if os.Getenv("SNOWFLAKE_ACCOUNT") == "" {
		s.T().Skip("SNOWFLAKE_ACCOUNT env var not set")
	}
	database := testenv.EnvOrDefault("SNOWFLAKE_DATABASE", "NYC_TAXI")
	sfConf := dwhexecsnowflake.SnowflakeConf{
		User:           os.Getenv("SNOWFLAKE_USER"),
		Password:       os.Getenv("SNOWFLAKE_PASSWORD"),
		Account:        os.Getenv("SNOWFLAKE_ACCOUNT"),
		Warehouse:      os.Getenv("SNOWFLAKE_WAREHOUSE"),
		Databases:      []string{"SNOWFLAKE_SAMPLE_DATA", database},
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

	s.ColumnsTable = scrappertest.FixtureTable{Database: "SNOWFLAKE_SAMPLE_DATA", Schema: "TPCDS_SF10TCL", Table: "CALL_CENTER"}
	s.NotNullColumn, s.NullableColumn = "CC_CALL_CENTER_ID", "CC_CITY"

	s.Table = scrappertest.FixtureTable{
		Database: database,
		Schema:   testenv.EnvOrDefault("SNOWFLAKE_SCHEMA", "PUBLIC"),
		Table:    testenv.EnvOrDefault("SNOWFLAKE_FACTS_TABLE", "SEED_RATE_CODES"),
	}
	s.ExpectOwner, s.ExpectCreatedAt, s.ExpectSizeBytes = true, true, true
}

func (s *SnowflakeCatalogFactsSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
