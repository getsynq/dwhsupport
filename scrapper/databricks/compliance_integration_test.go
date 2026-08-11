package databricks

import (
	"context"
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
	sc := newIntegrationScrapper(s.T(), s.Ctx())
	s.Scrapper = sc
}

func (s *DatabricksComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// DatabricksScopeComplianceSuite checks that scope filtering holds against a real
// workspace — that a per-call scope.WithScope reaches every Unity Catalog walk, which
// only became true once the walks stopped consulting the conf-derived scope directly.
type DatabricksScopeComplianceSuite struct {
	scrappertest.ScopeComplianceSuite
}

func TestDatabricksScopeComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Databricks compliance tests in CI")
	}
	suite.Run(t, new(DatabricksScopeComplianceSuite))
}

func (s *DatabricksScopeComplianceSuite) SetupSuite() {
	s.Scrapper = newIntegrationScrapper(s.T(), s.Ctx())
}

func (s *DatabricksScopeComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// newIntegrationScrapper connects to the Databricks workspace the environment names,
// skipping the calling suite when it is not configured or not reachable.
func newIntegrationScrapper(t *testing.T, ctx context.Context) *DatabricksScrapper {
	t.Helper()

	workspaceUrl := os.Getenv("DATABRICKS_HOST")
	warehouseId := os.Getenv("DATABRICKS_WAREHOUSE_ID")
	if workspaceUrl == "" || warehouseId == "" {
		t.Skip("DATABRICKS_HOST or DATABRICKS_WAREHOUSE_ID env var not set")
	}

	var auth dwhexecdatabricks.Auth
	if clientId := os.Getenv("DATABRICKS_OAUTH_CLIENT_ID"); clientId != "" {
		auth = dwhexecdatabricks.NewOAuthM2mAuth(clientId, os.Getenv("DATABRICKS_OAUTH_CLIENT_SECRET"))
	} else if token := os.Getenv("DATABRICKS_TOKEN"); token != "" {
		auth = dwhexecdatabricks.NewTokenAuth(token)
	} else {
		t.Skip("Neither DATABRICKS_OAUTH_CLIENT_ID nor DATABRICKS_TOKEN env var set")
	}

	sc, err := NewDatabricksScrapper(ctx, &DatabricksScrapperConf{
		DatabricksConf: dwhexecdatabricks.DatabricksConf{
			WorkspaceUrl: workspaceUrl,
			Auth:         auth,
			WarehouseId:  warehouseId,
		},
	})
	if err != nil {
		t.Skipf("Could not connect to Databricks: %v", err)
	}
	return sc
}
