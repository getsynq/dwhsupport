package postgres

import (
	"context"
	"os"
	"testing"

	dwhexecpostgres "github.com/getsynq/dwhsupport/exec/postgres"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// PostgresComplianceSuite runs the generic scrapper compliance checks.
type PostgresComplianceSuite struct {
	scrappertest.ComplianceSuite
}

func TestPostgresComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Postgres compliance tests in CI")
	}
	suite.Run(t, new(PostgresComplianceSuite))
}

func (s *PostgresComplianceSuite) SetupSuite() {
	host := testenv.EnvOrDefault("POSTGRES_HOST", "")
	if host == "" {
		s.T().Skip("POSTGRES_HOST env var not set")
	}
	sc, err := newPostgresScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Postgres: %v", err)
	}
	s.Scrapper = sc
}

func (s *PostgresComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// PostgresScopeComplianceSuite runs scope filtering compliance checks.
type PostgresScopeComplianceSuite struct {
	scrappertest.ScopeComplianceSuite
}

func TestPostgresScopeComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Postgres scope compliance tests in CI")
	}
	suite.Run(t, new(PostgresScopeComplianceSuite))
}

func (s *PostgresScopeComplianceSuite) SetupSuite() {
	host := testenv.EnvOrDefault("POSTGRES_HOST", "")
	if host == "" {
		s.T().Skip("POSTGRES_HOST env var not set")
	}
	sc, err := newPostgresScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Postgres: %v", err)
	}
	s.Scrapper = sc
}

func (s *PostgresScopeComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// PostgresMonitorComplianceSuite runs the monitor compliance checks.
type PostgresMonitorComplianceSuite struct {
	scrappertest.MonitorComplianceSuite
}

func TestPostgresMonitorComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Postgres monitor compliance tests in CI")
	}
	suite.Run(t, new(PostgresMonitorComplianceSuite))
}

func (s *PostgresMonitorComplianceSuite) SetupSuite() {
	host := testenv.EnvOrDefault("POSTGRES_HOST", "")
	if host == "" {
		s.T().Skip("POSTGRES_HOST env var not set")
	}
	sc, err := newPostgresScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Postgres: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.MonitorComplianceConfig{
		SegmentsSQL:          `SELECT DISTINCT category as segment FROM schema_a.products`,
		CustomMetricsSQL:     `SELECT category as segment_name, SUM(price * quantity) as total_value, COUNT(*) as product_count FROM schema_a.products GROUP BY category`,
		ShapeSQL:             `SELECT id, name, price, created_at, is_active FROM schema_a.products`,
		ExpectedSegments:     []string{"Electronics", "Accessories"},
		ExpectedShapeColumns: []string{"id", "name", "price", "created_at", "is_active"},
	}
}

func (s *PostgresMonitorComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// PostgresMetricsExecutionSuite runs metrics SQL generation + execution checks.
type PostgresMetricsExecutionSuite struct {
	scrappertest.MetricsExecutionSuite
}

func TestPostgresMetricsExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Postgres metrics execution tests in CI")
	}
	suite.Run(t, new(PostgresMetricsExecutionSuite))
}

func (s *PostgresMetricsExecutionSuite) SetupSuite() {
	host := testenv.EnvOrDefault("POSTGRES_HOST", "")
	if host == "" {
		s.T().Skip("POSTGRES_HOST env var not set")
	}
	sc, err := newPostgresScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Postgres: %v", err)
	}
	s.Scrapper = sc
	dbName := testenv.EnvOrDefault("POSTGRES_DATABASE", "synq_test")
	s.Config = scrappertest.MetricsExecutionConfig{
		TableFqn:          sqldialect.TableFqn(dbName, "schema_a", "products"),
		PartitioningField: "created_at",
		SegmentField:      "category",
		NumericField:      "price",
		TextField:         "name",
		TimeField:         "created_at",
	}
}

func (s *PostgresMetricsExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

func newPostgresScrapperFromEnv(ctx context.Context) (*PostgresScrapper, error) {
	conf := &PostgresScapperConf{
		PostgresConf: dwhexecpostgres.PostgresConf{
			User:          testenv.EnvOrDefault("POSTGRES_USER", "synq"),
			Password:      testenv.EnvOrDefault("POSTGRES_PASSWORD", "SynqTest1!"),
			Host:          testenv.EnvOrDefault("POSTGRES_HOST", ""),
			Port:          testenv.EnvOrDefaultInt("POSTGRES_PORT", 5432),
			Database:      testenv.EnvOrDefault("POSTGRES_DATABASE", "synq_test"),
			AllowInsecure: true,
		},
	}
	return NewPostgresScrapper(ctx, conf)
}
