package bigquery

import (
	"os"
	"testing"

	dwhexecbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// BigQueryComplianceSuite runs the generic scrapper compliance checks against
// a real BigQuery instance configured via environment variables.
type BigQueryComplianceSuite struct {
	scrappertest.ComplianceSuite
}

func TestBigQueryComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping BigQuery compliance tests in CI")
	}
	suite.Run(t, new(BigQueryComplianceSuite))
}

func (s *BigQueryComplianceSuite) SetupSuite() {
	projectID := os.Getenv("BIGQUERY_PROJECT_ID")
	if projectID == "" {
		s.T().Skip("BIGQUERY_PROJECT_ID env var not set")
	}

	credentialsFile := testenv.EnvOrDefault("BIGQUERY_CREDENTIALS_FILE", "../../nifty-motif-341212-88499dbfc22e.json")
	credentialsJson, err := os.ReadFile(credentialsFile)
	if err != nil {
		s.T().Skipf("Could not read BigQuery credentials file: %v", err)
	}

	conf := &BigQueryScrapperConf{
		BigQueryConf: dwhexecbigquery.BigQueryConf{
			ProjectId:       projectID,
			Region:          testenv.EnvOrDefault("BIGQUERY_REGION", "EU"),
			CredentialsJson: string(credentialsJson),
		},
	}

	sc, err := NewBigQueryScrapper(s.Ctx(), conf)
	if err != nil {
		s.T().Skipf("Could not connect to BigQuery: %v", err)
	}
	s.Scrapper = sc
}

func (s *BigQueryComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
