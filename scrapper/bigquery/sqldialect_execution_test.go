package bigquery

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/stretchr/testify/suite"
)

// BigQuerySqlDialectExecutionSuite runs the sqldialect builder execution checks
// against BigQuery.
type BigQuerySqlDialectExecutionSuite struct {
	scrappertest.SqlDialectExecutionSuite
}

func TestBigQuerySqlDialectExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping BigQuery sqldialect execution tests in CI")
	}
	suite.Run(t, new(BigQuerySqlDialectExecutionSuite))
}

func (s *BigQuerySqlDialectExecutionSuite) SetupSuite() {
	if os.Getenv("BIGQUERY_CREDENTIALS_FILE") == "" {
		s.T().Skip("BIGQUERY_CREDENTIALS_FILE env var not set")
	}
	sc, err := newBigQueryScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to BigQuery: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.SqlDialectExecutionConfig{
		TableFqn:     sqldialect.TableFqn(os.Getenv("BIGQUERY_PROJECT_ID"), datasetA(), "products"),
		KeyField:     "id",
		SegmentField: "category",
	}
}

func (s *BigQuerySqlDialectExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
