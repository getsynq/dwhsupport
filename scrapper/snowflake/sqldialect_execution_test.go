package snowflake

import (
	"os"
	"testing"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// SnowflakeSqlDialectExecutionSuite runs the sqldialect builder execution
// checks against Snowflake. Snowflake is the dialect these checks matter most
// for: it folds an unquoted identifier to upper case, so an alias and the
// reference to it have to be spelled the same way or the query resolves to
// something else.
type SnowflakeSqlDialectExecutionSuite struct {
	scrappertest.SqlDialectExecutionSuite
}

func TestSnowflakeSqlDialectExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Snowflake sqldialect execution tests in CI")
	}
	suite.Run(t, new(SnowflakeSqlDialectExecutionSuite))
}

func (s *SnowflakeSqlDialectExecutionSuite) SetupSuite() {
	database := os.Getenv("SNOWFLAKE_DATABASE")
	if database == "" {
		s.T().Skip("SNOWFLAKE_DATABASE env var not set")
	}

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
	s.Config = scrappertest.SqlDialectExecutionConfig{
		TableFqn: sqldialect.TableFqn(
			database,
			testenv.EnvOrDefault("SNOWFLAKE_SCHEMA", "PUBLIC"),
			testenv.EnvOrDefault("SNOWFLAKE_TEST_TABLE_NAME", "SEED_TAXI_ZONES"),
		),
		KeyField:     testenv.EnvOrDefault("SNOWFLAKE_TEST_KEY_FIELD", "LOCATION_ID"),
		SegmentField: testenv.EnvOrDefault("SNOWFLAKE_TEST_SEGMENT_FIELD", "BOROUGH"),
	}
}

func (s *SnowflakeSqlDialectExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
