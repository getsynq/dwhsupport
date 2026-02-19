package snowflake

import (
	"context"
	"os"
	"testing"
	"time"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/suite"
)

type TableChangeHistoryIntegrationSuite struct {
	suite.Suite
	scrapper *SnowflakeScrapper
	ctx      context.Context
}

func TestTableChangeHistoryIntegrationSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Snowflake integration tests in CI")
	}

	suite.Run(t, new(TableChangeHistoryIntegrationSuite))
}

func (s *TableChangeHistoryIntegrationSuite) SetupSuite() {
	s.ctx = context.Background()

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
	conf := &SnowflakeScrapperConf{
		SnowflakeConf: sfConf,
	}

	var err error
	s.scrapper, err = NewSnowflakeScrapper(s.ctx, conf)
	if err != nil {
		s.T().Skipf("Could not connect to Snowflake: %v", err)
	}
}

func (s *TableChangeHistoryIntegrationSuite) TearDownSuite() {
	if s.scrapper != nil {
		_ = s.scrapper.Close()
	}
}

func (s *TableChangeHistoryIntegrationSuite) TestFetchTableChangeHistoryDML() {
	database := os.Getenv("SNOWFLAKE_DATABASE")
	schema := os.Getenv("SNOWFLAKE_SCHEMA")
	if schema == "" {
		schema = "PUBLIC"
	}
	table := os.Getenv("SNOWFLAKE_TABLE")
	if table == "" {
		s.T().Skip("SNOWFLAKE_TABLE env var not set")
	}

	fqn := scrapper.DwhFqn{
		DatabaseName: database,
		SchemaName:   schema,
		ObjectName:   table,
	}

	to := time.Now().UTC()
	from := to.Add(-7 * 24 * time.Hour) // last 7 days (DML history has ~6h lag)

	events, err := s.scrapper.FetchTableChangeHistory(s.ctx, fqn, from, to, 50)
	s.Require().NoError(err)
	// Events may be empty if no DML in the window, but the call must succeed.
	for _, event := range events {
		s.NotZero(event.Timestamp)
		// Snowflake DML history doesn't provide version or operation
	}
}

func (s *TableChangeHistoryIntegrationSuite) TestFetchTableChangeHistoryAccessHistory() {
	database := os.Getenv("SNOWFLAKE_DATABASE")
	schema := os.Getenv("SNOWFLAKE_SCHEMA")
	if schema == "" {
		schema = "PUBLIC"
	}
	table := os.Getenv("SNOWFLAKE_TABLE")
	if table == "" {
		s.T().Skip("SNOWFLAKE_TABLE env var not set")
	}

	fqn := scrapper.DwhFqn{
		DatabaseName: database,
		SchemaName:   schema,
		ObjectName:   table,
	}

	// Use ACCESS_HISTORY mode (requires Enterprise edition)
	s.scrapper.conf.UseAccessHistoryForTableChanges = true
	defer func() { s.scrapper.conf.UseAccessHistoryForTableChanges = false }()

	to := time.Now().UTC()
	from := to.Add(-7 * 24 * time.Hour)

	events, err := s.scrapper.FetchTableChangeHistory(s.ctx, fqn, from, to, 50)
	s.Require().NoError(err)
	for _, event := range events {
		s.NotZero(event.Timestamp)
	}
}
