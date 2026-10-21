package bigquery

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// BigQueryTableConstraintsSuite checks the partitioning and clustering of the
// seed, among them a table partitioned by ingestion time, which has no column
// of its own to partition on.
type BigQueryTableConstraintsSuite struct {
	scrappertest.TableConstraintsSuite
}

func TestBigQueryTableConstraintsSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping BigQuery constraint tests in CI")
	}
	suite.Run(t, new(BigQueryTableConstraintsSuite))
}

func (s *BigQueryTableConstraintsSuite) SetupSuite() {
	if os.Getenv("BIGQUERY_CREDENTIALS_FILE") == "" {
		s.T().Skip("BIGQUERY_CREDENTIALS_FILE env var not set")
	}
	sc, err := newBigQueryScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to BigQuery: %v", err)
	}
	s.Scrapper = sc
	pageViews := scrappertest.FixtureTable{Schema: "synq_dwhtesting", Table: "page_views"}
	s.Expect = []scrappertest.Constraint{
		{Table: pageViews, Type: scrapper.ConstraintTypePartitionBy, Columns: []string{"_PARTITIONDATE"}},
		{Table: pageViews, Type: scrapper.ConstraintTypeClusterBy, Columns: []string{"session_id"}},
	}
}

func (s *BigQueryTableConstraintsSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
