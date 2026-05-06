package athena

import (
	"context"
	"os"
	"testing"

	dwhexecathena "github.com/getsynq/dwhsupport/exec/athena"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

func newAthenaScrapperFromEnv(ctx context.Context) (*AthenaScrapper, error) {
	conf := &AthenaScrapperConf{
		AthenaConf: &dwhexecathena.AthenaConf{
			Region:          testenv.EnvOrDefault("ATHENA_REGION", "eu-central-1"),
			Workgroup:       testenv.EnvOrDefault("ATHENA_WORKGROUP", "synq-dwhtesting-wg"),
			AccessKeyID:     os.Getenv("ATHENA_ACCESS_KEY_ID"),
			SecretAccessKey: os.Getenv("ATHENA_SECRET_ACCESS_KEY"),
		},
	}
	return NewAthenaScrapper(ctx, conf)
}

// AthenaComplianceSuite runs the generic scrapper compliance checks against a
// real AWS Athena workgroup configured via environment variables. The seed lives
// in cloud/dev-infra/dwhtesting/lib/athena/seed.sql; bootstrap with
// dwhtesting/lib/athena/bootstrap.sh and reseed with `./reseed.sh athena`.
type AthenaComplianceSuite struct {
	scrappertest.ComplianceSuite
}

func TestAthenaComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Athena compliance tests in CI")
	}
	suite.Run(t, new(AthenaComplianceSuite))
}

func (s *AthenaComplianceSuite) SetupSuite() {
	region := testenv.EnvOrDefault("ATHENA_REGION", "eu-central-1")
	workgroup := testenv.EnvOrDefault("ATHENA_WORKGROUP", "synq-dwhtesting-wg")
	accessKeyID := os.Getenv("ATHENA_ACCESS_KEY_ID")
	secret := os.Getenv("ATHENA_SECRET_ACCESS_KEY")

	if accessKeyID == "" || secret == "" {
		s.T().Skip("ATHENA_ACCESS_KEY_ID / ATHENA_SECRET_ACCESS_KEY env vars not set")
	}

	conf := &AthenaScrapperConf{
		AthenaConf: &dwhexecathena.AthenaConf{
			Region:          region,
			Workgroup:       workgroup,
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secret,
		},
	}

	sc, err := NewAthenaScrapper(s.Ctx(), conf)
	if err != nil {
		s.T().Skipf("Could not connect to Athena: %v", err)
	}
	s.Scrapper = sc
}

func (s *AthenaComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// AthenaScopeComplianceSuite runs scope filtering compliance checks.
type AthenaScopeComplianceSuite struct {
	scrappertest.ScopeComplianceSuite
}

func TestAthenaScopeComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Athena scope compliance tests in CI")
	}
	suite.Run(t, new(AthenaScopeComplianceSuite))
}

func (s *AthenaScopeComplianceSuite) SetupSuite() {
	if os.Getenv("ATHENA_ACCESS_KEY_ID") == "" || os.Getenv("ATHENA_SECRET_ACCESS_KEY") == "" {
		s.T().Skip("ATHENA_ACCESS_KEY_ID / ATHENA_SECRET_ACCESS_KEY env vars not set")
	}
	sc, err := newAthenaScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Athena: %v", err)
	}
	s.Scrapper = sc
}

func (s *AthenaScopeComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// AthenaMonitorComplianceSuite runs the monitor compliance checks against the
// dwhtesting Athena seed (synq_dwhtesting.order_items).
type AthenaMonitorComplianceSuite struct {
	scrappertest.MonitorComplianceSuite
}

func TestAthenaMonitorComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Athena monitor compliance tests in CI")
	}
	suite.Run(t, new(AthenaMonitorComplianceSuite))
}

func (s *AthenaMonitorComplianceSuite) SetupSuite() {
	if os.Getenv("ATHENA_ACCESS_KEY_ID") == "" || os.Getenv("ATHENA_SECRET_ACCESS_KEY") == "" {
		s.T().Skip("ATHENA_ACCESS_KEY_ID / ATHENA_SECRET_ACCESS_KEY env vars not set")
	}
	testTable := testenv.EnvOrDefault("ATHENA_TEST_TABLE", "synq_dwhtesting.products")
	sc, err := newAthenaScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Athena: %v", err)
	}
	s.Scrapper = sc

	segmentCol := testenv.EnvOrDefault("ATHENA_TEST_SEGMENT_FIELD", "category")
	numericCol := testenv.EnvOrDefault("ATHENA_TEST_NUMERIC_FIELD", "price")
	s.Config = scrappertest.MonitorComplianceConfig{
		SegmentsSQL:      `SELECT DISTINCT CAST(` + segmentCol + ` AS VARCHAR) as segment FROM ` + testTable,
		CustomMetricsSQL: `SELECT CAST(` + segmentCol + ` AS VARCHAR) as segment_name, SUM(` + numericCol + `) as total_value, COUNT(*) as row_count FROM ` + testTable + ` GROUP BY ` + segmentCol,
		ShapeSQL:         `SELECT * FROM ` + testTable + ` LIMIT 1`,
	}
}

func (s *AthenaMonitorComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// AthenaMetricsExecutionSuite runs metrics SQL generation + execution checks
// against the dwhtesting Athena seed.
type AthenaMetricsExecutionSuite struct {
	scrappertest.MetricsExecutionSuite
}

func TestAthenaMetricsExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Athena metrics execution tests in CI")
	}
	suite.Run(t, new(AthenaMetricsExecutionSuite))
}

func (s *AthenaMetricsExecutionSuite) SetupSuite() {
	if os.Getenv("ATHENA_ACCESS_KEY_ID") == "" || os.Getenv("ATHENA_SECRET_ACCESS_KEY") == "" {
		s.T().Skip("ATHENA_ACCESS_KEY_ID / ATHENA_SECRET_ACCESS_KEY env vars not set")
	}
	sc, err := newAthenaScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Athena: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.MetricsExecutionConfig{
		TableFqn: sqldialect.TableFqn(
			testenv.EnvOrDefault("ATHENA_TEST_CATALOG", "AwsDataCatalog"),
			testenv.EnvOrDefault("ATHENA_TEST_SCHEMA", "synq_dwhtesting"),
			testenv.EnvOrDefault("ATHENA_TEST_TABLE_NAME", "products"),
		),
		PartitioningField: testenv.EnvOrDefault("ATHENA_TEST_TIME_FIELD", "created_at"),
		SegmentField:      testenv.EnvOrDefault("ATHENA_TEST_SEGMENT_FIELD", "category"),
		NumericField:      testenv.EnvOrDefault("ATHENA_TEST_NUMERIC_FIELD", "price"),
		TextField:         testenv.EnvOrDefault("ATHENA_TEST_TEXT_FIELD", "name"),
		TimeField:         testenv.EnvOrDefault("ATHENA_TEST_TIME_FIELD", "created_at"),
	}
}

func (s *AthenaMetricsExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
