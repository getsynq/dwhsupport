package bigquery

import (
	"context"
	"os"
	"testing"
	"time"

	dwhexecbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// BigQuery compliance / integration tests.
//
// These suites expect the shared dwhtesting fixtures to be present:
//
//	getsynq/cloud/dev-infra/dwhtesting/lib/bigquery/seed.sql
//
// Apply the seed with:
//
//	cd dev-infra/dwhtesting && ./reseed.sh bigquery
//
// Required environment:
//
//	BIGQUERY_PROJECT_ID       — GCP project that hosts the seeded datasets
//	BIGQUERY_CREDENTIALS_FILE — path to a service-account JSON key with
//	                            at least the SynqDwhtestingJobs project role
//	                            + bigquery.metadataViewer on both datasets
//	BIGQUERY_REGION           — optional, default "EU"
//	BIGQUERY_DATASET_A        — optional, default "synq_dwhtesting"
//	BIGQUERY_DATASET_B        — optional, default "synq_dwhtesting_b"
//
// Tests are skipped when CI=1 or when BIGQUERY_PROJECT_ID /
// BIGQUERY_CREDENTIALS_FILE are unset.

const (
	defaultDatasetA = "synq_dwhtesting"
	defaultDatasetB = "synq_dwhtesting_b"
)

// newBigQueryScrapperFromEnv creates a BigQuery scrapper restricted to the
// dwhtesting datasets via the Datasets allowlist.
func newBigQueryScrapperFromEnv(ctx context.Context) (*BigQueryScrapper, error) {
	credentialsFile := os.Getenv("BIGQUERY_CREDENTIALS_FILE")
	credentialsJson, err := os.ReadFile(credentialsFile)
	if err != nil {
		return nil, err
	}

	conf := &BigQueryScrapperConf{
		BigQueryConf: dwhexecbigquery.BigQueryConf{
			ProjectId:       os.Getenv("BIGQUERY_PROJECT_ID"),
			Region:          testenv.EnvOrDefault("BIGQUERY_REGION", "EU"),
			CredentialsJson: string(credentialsJson),
		},
		// Allowlist both dwhtesting datasets so the scrapper never strays
		// into unrelated customer-like datasets in the demo project.
		Datasets: []string{
			testenv.EnvOrDefault("BIGQUERY_DATASET_A", defaultDatasetA),
			testenv.EnvOrDefault("BIGQUERY_DATASET_B", defaultDatasetB),
		},
	}

	return NewBigQueryScrapper(ctx, conf)
}

func skipIfNoBigQuery(t *testing.T) {
	t.Helper()
	if os.Getenv("CI") != "" {
		t.Skip("Skipping BigQuery tests in CI")
	}
	if os.Getenv("BIGQUERY_PROJECT_ID") == "" {
		t.Skip("BIGQUERY_PROJECT_ID env var not set")
	}
	if os.Getenv("BIGQUERY_CREDENTIALS_FILE") == "" {
		t.Skip("BIGQUERY_CREDENTIALS_FILE env var not set")
	}
}

func datasetA() string {
	return testenv.EnvOrDefault("BIGQUERY_DATASET_A", defaultDatasetA)
}

// --- ComplianceSuite ---

type BigQueryComplianceSuite struct {
	scrappertest.ComplianceSuite
}

func TestBigQueryComplianceSuite(t *testing.T) {
	skipIfNoBigQuery(t)
	suite.Run(t, new(BigQueryComplianceSuite))
}

func (s *BigQueryComplianceSuite) SetupSuite() {
	sc, err := newBigQueryScrapperFromEnv(s.Ctx())
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

// --- ScopeComplianceSuite ---

type BigQueryScopeComplianceSuite struct {
	scrappertest.ScopeComplianceSuite
	inner *BigQueryScrapper
}

func TestBigQueryScopeComplianceSuite(t *testing.T) {
	skipIfNoBigQuery(t)
	suite.Run(t, new(BigQueryScopeComplianceSuite))
}

func (s *BigQueryScopeComplianceSuite) SetupSuite() {
	sc, err := newBigQueryScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to BigQuery: %v", err)
	}
	s.inner = sc
	// Wrap with ScopedScrapper so context-based scope filtering works.
	// BigQuery's own scope handling uses API-level dataset filtering via the
	// Datasets allowlist; ScopeComplianceSuite layers an additional
	// context-based scope filter on top to exercise that code path too.
	s.Scrapper = scope.NewScopedScrapper(sc, nil)
}

func (s *BigQueryScopeComplianceSuite) TearDownSuite() {
	if s.inner != nil {
		_ = s.inner.Close()
	}
}

// --- MonitorComplianceSuite ---

type BigQueryMonitorComplianceSuite struct {
	scrappertest.MonitorComplianceSuite
}

func TestBigQueryMonitorComplianceSuite(t *testing.T) {
	skipIfNoBigQuery(t)
	suite.Run(t, new(BigQueryMonitorComplianceSuite))
}

func (s *BigQueryMonitorComplianceSuite) SetupSuite() {
	sc, err := newBigQueryScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to BigQuery: %v", err)
	}
	s.Scrapper = sc

	projectID := os.Getenv("BIGQUERY_PROJECT_ID")
	ds := datasetA()
	fqn := func(table string) string {
		return "`" + projectID + "." + ds + "." + table + "`"
	}

	s.Config = scrappertest.MonitorComplianceConfig{
		SegmentsSQL:          `SELECT DISTINCT category as segment FROM ` + fqn("products"),
		CustomMetricsSQL:     `SELECT category as segment_name, SUM(CAST(price AS FLOAT64) * quantity) as total_value, COUNT(*) as product_count FROM ` + fqn("products") + ` GROUP BY category`,
		ShapeSQL:             `SELECT id, name, price, created_at, is_active FROM ` + fqn("products"),
		ExpectedSegments:     []string{"Electronics", "Accessories"},
		ExpectedShapeColumns: []string{"id", "name", "price", "created_at", "is_active"},
	}
}

func (s *BigQueryMonitorComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// --- MetricsExecutionSuite ---

type BigQueryMetricsExecutionSuite struct {
	scrappertest.MetricsExecutionSuite
}

func TestBigQueryMetricsExecutionSuite(t *testing.T) {
	skipIfNoBigQuery(t)
	suite.Run(t, new(BigQueryMetricsExecutionSuite))
}

func (s *BigQueryMetricsExecutionSuite) SetupSuite() {
	sc, err := newBigQueryScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to BigQuery: %v", err)
	}
	s.Scrapper = sc

	projectID := os.Getenv("BIGQUERY_PROJECT_ID")
	s.Config = scrappertest.MetricsExecutionConfig{
		TableFqn:          sqldialect.TableFqn(projectID, datasetA(), "products"),
		PartitioningField: "created_at",
		SegmentField:      "category",
		NumericField:      "price",
		TextField:         "name",
		TimeField:         "created_at",
	}
}

func (s *BigQueryMetricsExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// --- BigQueryScrapperSuite: warehouse-specific tests ---
//
// Covers BigQuery-specific behaviour not exercised by the generic compliance
// suites: type handling (NUMERIC, BIGNUMERIC, TIMESTAMP, BOOL) and
// FetchTableChangeHistory. Uses the dwhtesting `test_bigquery_types` fixture.

type BigQueryScrapperSuite struct {
	suite.Suite
	scrapper *BigQueryScrapper
}

func TestBigQueryScrapperSuite(t *testing.T) {
	skipIfNoBigQuery(t)
	suite.Run(t, new(BigQueryScrapperSuite))
}

func (s *BigQueryScrapperSuite) SetupSuite() {
	sc, err := newBigQueryScrapperFromEnv(context.Background())
	if err != nil {
		s.T().Skipf("Could not connect to BigQuery: %v", err)
	}
	s.scrapper = sc
}

func (s *BigQueryScrapperSuite) TearDownSuite() {
	if s.scrapper != nil {
		_ = s.scrapper.Close()
	}
}

func (s *BigQueryScrapperSuite) typesTable() string {
	return "`" + s.scrapper.conf.ProjectId + "." + datasetA() + ".test_bigquery_types`"
}

func (s *BigQueryScrapperSuite) ctx() context.Context {
	return context.Background()
}

func (s *BigQueryScrapperSuite) TestQueryCustomMetrics_WithNumeric() {
	sql := `SELECT name as segment_name, amount as numeric_value FROM ` + s.typesTable() + ` ORDER BY id`

	result, err := s.scrapper.QueryCustomMetrics(s.ctx(), sql)
	s.Require().NoError(err)
	s.Require().Len(result, 2)

	for _, row := range result {
		s.Require().Len(row.ColumnValues, 1)
		col := row.ColumnValues[0]
		s.Equal("numeric_value", col.Name)
		s.False(col.IsNull)
		s.NotNil(col.Value, "NUMERIC value should not be nil")
	}
}

func (s *BigQueryScrapperSuite) TestQueryCustomMetrics_WithBigNumeric() {
	sql := `SELECT name as segment_name, big_amount as bignumeric_value FROM ` + s.typesTable() + ` ORDER BY id`

	result, err := s.scrapper.QueryCustomMetrics(s.ctx(), sql)
	s.Require().NoError(err)
	s.Require().Len(result, 2)

	for _, row := range result {
		s.Require().Len(row.ColumnValues, 1)
		col := row.ColumnValues[0]
		s.Equal("bignumeric_value", col.Name)
		s.False(col.IsNull)
		s.NotNil(col.Value, "BIGNUMERIC value should not be nil")
	}
}

func (s *BigQueryScrapperSuite) TestQueryCustomMetrics_WithTimestamp() {
	sql := `SELECT name as segment_name, created_at as timestamp_value FROM ` + s.typesTable() + ` ORDER BY id`

	result, err := s.scrapper.QueryCustomMetrics(s.ctx(), sql)
	s.Require().NoError(err)
	s.Require().Len(result, 2)

	for _, row := range result {
		s.Require().Len(row.ColumnValues, 1)
		col := row.ColumnValues[0]
		s.Equal("timestamp_value", col.Name)
		s.False(col.IsNull)
		_, ok := col.Value.(scrapper.TimeValue)
		s.True(ok, "Timestamp should be converted to TimeValue")
	}
}

func (s *BigQueryScrapperSuite) TestQueryCustomMetrics_WithBoolean() {
	sql := `SELECT name as segment_name, is_active as bool_value FROM ` + s.typesTable() + ` ORDER BY id`

	result, err := s.scrapper.QueryCustomMetrics(s.ctx(), sql)
	s.Require().NoError(err)
	s.Require().Len(result, 2)

	s.Equal("Alice", result[0].Segments[0].Value)
	s.IsType(scrapper.IntValue(0), result[0].ColumnValues[0].Value)
	s.Equal(scrapper.IntValue(1), result[0].ColumnValues[0].Value)

	s.Equal("Bob", result[1].Segments[0].Value)
	s.IsType(scrapper.IntValue(0), result[1].ColumnValues[0].Value)
	s.Equal(scrapper.IntValue(0), result[1].ColumnValues[0].Value)
}

func (s *BigQueryScrapperSuite) TestFetchTableChangeHistory() {
	to := time.Now().UTC()
	from := to.Add(-24 * time.Hour)

	fqn := scrapper.DwhFqn{
		DatabaseName: s.scrapper.conf.ProjectId,
		SchemaName:   datasetA(),
		ObjectName:   "test_bigquery_types",
	}

	events, err := s.scrapper.FetchTableChangeHistory(s.ctx(), fqn, from, to, 100)
	s.Require().NoError(err)
	for _, event := range events {
		s.NotZero(event.Timestamp)
		s.NotEmpty(event.Version, "BigQuery events should have a job_id as version")
		s.NotEmpty(event.Operation)
	}
}
