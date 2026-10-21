package bigquery

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// BigQueryRoutinesSuite reads the synq_dwhtesting dataset's procedure, SQL
// and JavaScript functions and table function from the dwhtesting seed.
// BigQuery applies only the configured datasets itself, so the scrapper is
// wrapped in a ScopedScrapper for the context scope, as in production.
type BigQueryRoutinesSuite struct {
	scrappertest.RoutinesSuite
	inner *BigQueryScrapper
}

func TestBigQueryRoutinesSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping BigQuery routines tests in CI")
	}
	suite.Run(t, new(BigQueryRoutinesSuite))
}

func (s *BigQueryRoutinesSuite) SetupSuite() {
	if os.Getenv("BIGQUERY_CREDENTIALS_FILE") == "" {
		s.T().Skip("BIGQUERY_CREDENTIALS_FILE env var not set")
	}
	sc, err := newBigQueryScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to BigQuery: %v", err)
	}
	s.inner = sc
	s.Scrapper = scope.NewScopedScrapper(sc, nil)
	s.Expect = []scrappertest.FixtureRoutine{
		{Schema: "synq_dwhtesting", Name: "purge_old_events", Kind: scrapper.RoutineKindProcedure, DefinitionContains: "events_hourly"},
		{Schema: "synq_dwhtesting", Name: "net_price", Kind: scrapper.RoutineKindFunction, DefinitionContains: "unit_price"},
		{Schema: "synq_dwhtesting", Name: "domain_of", Kind: scrapper.RoutineKindFunction, DefinitionContains: "indexOf"},
		{Schema: "synq_dwhtesting", Name: "orders_for_product", Kind: scrapper.RoutineKindFunction, DefinitionContains: "order_items"},
	}
}

func (s *BigQueryRoutinesSuite) TearDownSuite() {
	if s.inner != nil {
		_ = s.inner.Close()
	}
}
