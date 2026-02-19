package databricks

import (
	"context"
	"os"
	"testing"
	"time"

	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/suite"
)

type TableChangeHistoryIntegrationSuite struct {
	suite.Suite
	scrapper *DatabricksScrapper
	ctx      context.Context
}

func TestTableChangeHistoryIntegrationSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Databricks integration tests in CI")
	}

	suite.Run(t, new(TableChangeHistoryIntegrationSuite))
}

func (s *TableChangeHistoryIntegrationSuite) SetupSuite() {
	s.ctx = context.Background()

	workspaceUrl := os.Getenv("DATABRICKS_HOST")
	token := os.Getenv("DATABRICKS_TOKEN")
	warehouseId := os.Getenv("DATABRICKS_WAREHOUSE_ID")

	if workspaceUrl == "" || token == "" || warehouseId == "" {
		s.T().Skip("DATABRICKS_HOST, DATABRICKS_TOKEN, or DATABRICKS_WAREHOUSE_ID env vars not set")
	}

	conf := &DatabricksScrapperConf{
		DatabricksConf: dwhexecdatabricks.DatabricksConf{
			WorkspaceUrl: workspaceUrl,
			Auth:         dwhexecdatabricks.NewTokenAuth(token),
			WarehouseId:  warehouseId,
		},
	}

	var err error
	s.scrapper, err = NewDatabricksScrapper(s.ctx, conf)
	if err != nil {
		s.T().Skipf("Could not connect to Databricks: %v", err)
	}
}

func (s *TableChangeHistoryIntegrationSuite) TearDownSuite() {
	if s.scrapper != nil {
		_ = s.scrapper.Close()
	}
}

func (s *TableChangeHistoryIntegrationSuite) TestFetchTableChangeHistory() {
	catalog := os.Getenv("DATABRICKS_CATALOG")
	schema := os.Getenv("DATABRICKS_SCHEMA")
	table := os.Getenv("DATABRICKS_TABLE")

	if catalog == "" || schema == "" || table == "" {
		s.T().Skip("DATABRICKS_CATALOG, DATABRICKS_SCHEMA, or DATABRICKS_TABLE env vars not set")
	}

	fqn := scrapper.DwhFqn{
		DatabaseName: catalog,
		SchemaName:   schema,
		ObjectName:   table,
	}

	to := time.Now().UTC()
	from := to.Add(-30 * 24 * time.Hour) // last 30 days

	events, err := s.scrapper.FetchTableChangeHistory(s.ctx, fqn, from, to, 50)
	s.Require().NoError(err)
	// Validate event structure if any events are returned
	for _, event := range events {
		s.NotZero(event.Timestamp)
		s.NotEmpty(event.Version, "Databricks events should have a version (Delta version number)")
		s.NotEmpty(event.Operation)
	}
}
