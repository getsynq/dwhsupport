package bigquery

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// BigQueryForeignKeysSuite checks the NOT ENFORCED foreign key of the
// order_items fixture, read with the table metadata.
type BigQueryForeignKeysSuite struct {
	scrappertest.ForeignKeysSuite
}

func TestBigQueryForeignKeysSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping BigQuery constraint tests in CI")
	}
	suite.Run(t, new(BigQueryForeignKeysSuite))
}

func (s *BigQueryForeignKeysSuite) SetupSuite() {
	if os.Getenv("BIGQUERY_CREDENTIALS_FILE") == "" {
		s.T().Skip("BIGQUERY_CREDENTIALS_FILE env var not set")
	}
	sc, err := newBigQueryScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to BigQuery: %v", err)
	}
	s.Scrapper = sc
	s.ForeignKeys = []scrappertest.ForeignKey{{
		Table:             scrappertest.FixtureTable{Schema: "synq_dwhtesting", Table: "order_items"},
		Columns:           []string{"product_id"},
		Referenced:        scrappertest.FixtureTable{Database: "nifty-motif-341212", Schema: "synq_dwhtesting", Table: "products"},
		ReferencedColumns: []string{"id"},
	}}
}

func (s *BigQueryForeignKeysSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
