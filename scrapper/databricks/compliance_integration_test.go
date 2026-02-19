package databricks

import (
	"os"
	"testing"

	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// DatabricksComplianceSuite runs the generic scrapper compliance checks against
// a real Databricks instance configured via environment variables.
type DatabricksComplianceSuite struct {
	scrappertest.ComplianceSuite
}

func TestDatabricksComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Databricks compliance tests in CI")
	}
	suite.Run(t, new(DatabricksComplianceSuite))
}

func (s *DatabricksComplianceSuite) SetupSuite() {
	workspaceUrl := os.Getenv("DATABRICKS_HOST")
	warehouseId := os.Getenv("DATABRICKS_WAREHOUSE_ID")
	if workspaceUrl == "" || warehouseId == "" {
		s.T().Skip("DATABRICKS_HOST or DATABRICKS_WAREHOUSE_ID env var not set")
	}

	var auth dwhexecdatabricks.Auth
	if clientId := os.Getenv("DATABRICKS_OAUTH_CLIENT_ID"); clientId != "" {
		auth = dwhexecdatabricks.NewOAuthM2mAuth(clientId, os.Getenv("DATABRICKS_OAUTH_CLIENT_SECRET"))
	} else if token := os.Getenv("DATABRICKS_TOKEN"); token != "" {
		auth = dwhexecdatabricks.NewTokenAuth(token)
	} else {
		s.T().Skip("Neither DATABRICKS_OAUTH_CLIENT_ID nor DATABRICKS_TOKEN env var set")
	}

	conf := &DatabricksScrapperConf{
		DatabricksConf: dwhexecdatabricks.DatabricksConf{
			WorkspaceUrl: workspaceUrl,
			Auth:         auth,
			WarehouseId:  warehouseId,
		},
	}

	sc, err := NewDatabricksScrapper(s.Ctx(), conf)
	if err != nil {
		s.T().Skipf("Could not connect to Databricks: %v", err)
	}
	s.Scrapper = sc
}

func (s *DatabricksComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
