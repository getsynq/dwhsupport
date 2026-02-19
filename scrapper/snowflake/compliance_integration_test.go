package snowflake

import (
	"os"
	"testing"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// SnowflakeComplianceSuite runs the generic scrapper compliance checks against
// a real Snowflake instance configured via environment variables.
type SnowflakeComplianceSuite struct {
	scrappertest.ComplianceSuite
}

func TestSnowflakeComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Snowflake compliance tests in CI")
	}
	suite.Run(t, new(SnowflakeComplianceSuite))
}

func (s *SnowflakeComplianceSuite) SetupSuite() {
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
}

func (s *SnowflakeComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
