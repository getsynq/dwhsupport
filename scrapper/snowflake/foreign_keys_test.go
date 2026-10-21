package snowflake

import (
	"os"
	"testing"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// SnowflakeForeignKeysSuite checks the RELY foreign keys of the dwhtesting
// seed: one within a schema and one referencing a table in another schema.
type SnowflakeForeignKeysSuite struct {
	scrappertest.ForeignKeysSuite
}

func TestSnowflakeForeignKeysSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Snowflake constraint tests in CI")
	}
	suite.Run(t, new(SnowflakeForeignKeysSuite))
}

func (s *SnowflakeForeignKeysSuite) SetupSuite() {
	if os.Getenv("SNOWFLAKE_ACCOUNT") == "" {
		s.T().Skip("SNOWFLAKE_ACCOUNT env var not set")
	}
	database := testenv.EnvOrDefault("SNOWFLAKE_FK_DATABASE", "DWH_TESTING")
	sfConf := dwhexecsnowflake.SnowflakeConf{
		User:           os.Getenv("SNOWFLAKE_USER"),
		Password:       os.Getenv("SNOWFLAKE_PASSWORD"),
		Account:        os.Getenv("SNOWFLAKE_ACCOUNT"),
		Warehouse:      os.Getenv("SNOWFLAKE_WAREHOUSE"),
		Databases:      []string{database},
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
	s.ForeignKeys = []scrappertest.ForeignKey{
		{
			Table:             scrappertest.FixtureTable{Database: database, Schema: "SCHEMA_A", Table: "ORDER_ITEMS"},
			Columns:           []string{"PRODUCT_ID"},
			Referenced:        scrappertest.FixtureTable{Database: database, Schema: "SCHEMA_A", Table: "PRODUCTS"},
			ReferencedColumns: []string{"ID"},
		},
		{
			Table:             scrappertest.FixtureTable{Database: database, Schema: "SEMANTIC_VIEWS", Table: "ORDERS"},
			Columns:           []string{"CUSTOMER_ID"},
			Referenced:        scrappertest.FixtureTable{Database: database, Schema: "SCHEMA_B", Table: "CUSTOMERS"},
			ReferencedColumns: []string{"ID"},
		},
	}
}

func (s *SnowflakeForeignKeysSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
