package databricks

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// DatabricksSqlDialectExecutionSuite runs the sqldialect builder execution
// checks against a Databricks SQL warehouse.
type DatabricksSqlDialectExecutionSuite struct {
	scrappertest.SqlDialectExecutionSuite
}

func TestDatabricksSqlDialectExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Databricks sqldialect execution tests in CI")
	}
	suite.Run(t, new(DatabricksSqlDialectExecutionSuite))
}

func (s *DatabricksSqlDialectExecutionSuite) SetupSuite() {
	catalog := testenv.EnvOrDefault("DATABRICKS_CATALOG", "")
	schema := testenv.EnvOrDefault("DATABRICKS_SCHEMA", "")
	table := testenv.EnvOrDefault("DATABRICKS_TABLE", "")
	if catalog == "" || schema == "" || table == "" {
		s.T().Skip("DATABRICKS_CATALOG / DATABRICKS_SCHEMA / DATABRICKS_TABLE env vars not set")
	}

	s.Scrapper = newIntegrationScrapper(s.T(), s.Ctx())
	s.Config = scrappertest.SqlDialectExecutionConfig{
		TableFqn:     sqldialect.TableFqn(catalog, schema, table),
		KeyField:     testenv.EnvOrDefault("DATABRICKS_TEST_KEY_FIELD", "id"),
		SegmentField: testenv.EnvOrDefault("DATABRICKS_TEST_SEGMENT_FIELD", "category"),
	}
}

func (s *DatabricksSqlDialectExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
