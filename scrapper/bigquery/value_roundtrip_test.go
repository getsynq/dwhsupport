package bigquery

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// BigQueryValueRoundTripSuite checks that values read from BigQuery decode to
// what they stand for and can be written back as literals that compare equal.
type BigQueryValueRoundTripSuite struct {
	scrappertest.ValueRoundTripSuite
}

func TestBigQueryValueRoundTripSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping BigQuery value round-trip tests in CI")
	}
	suite.Run(t, new(BigQueryValueRoundTripSuite))
}

func (s *BigQueryValueRoundTripSuite) SetupSuite() {
	if os.Getenv("BIGQUERY_CREDENTIALS_FILE") == "" {
		s.T().Skip("BIGQUERY_CREDENTIALS_FILE env var not set")
	}
	sc, err := newBigQueryScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to BigQuery: %v", err)
	}
	s.Scrapper = sc
}

func (s *BigQueryValueRoundTripSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
