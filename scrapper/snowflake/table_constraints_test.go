package snowflake

import (
	"os"
	"testing"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
)

// SnowflakeTableConstraintsSuite checks the RELY unique keys and the clustering
// key of the dwhtesting seed.
type SnowflakeTableConstraintsSuite struct {
	scrappertest.TableConstraintsSuite
}

func TestSnowflakeTableConstraintsSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Snowflake constraint tests in CI")
	}
	suite.Run(t, new(SnowflakeTableConstraintsSuite))
}

func (s *SnowflakeTableConstraintsSuite) SetupSuite() {
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
	products := scrappertest.FixtureTable{Database: database, Schema: "SCHEMA_A", Table: "PRODUCTS"}
	s.Expect = []scrappertest.Constraint{
		// a key on a standard table is recorded and never checked, whatever its RELY
		{Table: products, Type: scrapper.ConstraintTypePrimaryKey, Columns: []string{"ID"}, IsEnforced: lo.ToPtr(false)},
		{Table: products, Type: scrapper.ConstraintTypeUniqueIndex, Columns: []string{"SKU"}, IsEnforced: lo.ToPtr(false), IsRely: lo.ToPtr(true)},
		{Table: products, Type: scrapper.ConstraintTypeClusterBy, Columns: []string{"CATEGORY", "ID"}, Expression: "LINEAR(CATEGORY, ID)"},
		{
			Table:   scrappertest.FixtureTable{Database: database, Schema: "SCHEMA_A", Table: "ORDER_ITEMS"},
			Type:    scrapper.ConstraintTypeUniqueIndex,
			Columns: []string{"ORDER_ID", "PRODUCT_ID"},
		},
		{
			Table:   scrappertest.FixtureTable{Database: database, Schema: "SCHEMA_B", Table: "CUSTOMERS"},
			Type:    scrapper.ConstraintTypeUniqueIndex,
			Columns: []string{"EMAIL"},
		},
	}
}

func (s *SnowflakeTableConstraintsSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
