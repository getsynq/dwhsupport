package postgres

import (
	"context"
	"os"
	"testing"

	dwhexecpostgres "github.com/getsynq/dwhsupport/exec/postgres"
	"github.com/getsynq/dwhsupport/scrapper"
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

// PostgresPlatformUsersSuite lists the server's login roles.
type PostgresPlatformUsersSuite struct {
	scrappertest.PlatformUsersSuite
}

func TestPostgresPlatformUsersSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Postgres platform users tests in CI")
	}
	suite.Run(t, new(PostgresPlatformUsersSuite))
}

func (s *PostgresPlatformUsersSuite) SetupSuite() {
	if testenv.EnvOrDefault("POSTGRES_HOST", "") == "" {
		s.T().Skip("POSTGRES_HOST env var not set")
	}
	sc, err := newPostgresScrapperFromEnv(context.Background())
	if err != nil {
		s.T().Skipf("Could not connect to Postgres: %v", err)
	}
	s.Scrapper = sc
	s.ConnectedLogin = testenv.EnvOrDefault("POSTGRES_USER", "synq")
	s.ExpectFacts = []scrapper.PlatformUserFact{scrapper.PlatformUserFactPlatformId, scrapper.PlatformUserFactDisabled}
}

func (s *PostgresPlatformUsersSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// TestPlatformUsers_ListsLoginRolesOnly checks that a NOLOGIN role, such as
// the built-in pg_monitor, is never listed as a user, and that the facts
// Postgres does not keep are reported as skipped.
func (s *PostgresPlatformUsersSuite) TestPlatformUsers_ListsLoginRolesOnly() {
	users, err := scrappertest.OnlyPlatformUserSource(s.Scrapper.QueryPlatformUsers(context.Background()))
	s.Require().NoError(err)
	s.Equal(scrapper.PlatformUsersComplete, users.Completeness)
	for _, u := range users.Users {
		s.NotEqual("pg_monitor", u.Login)
		s.NotEqual("pg_read_all_data", u.Login)
	}
	for _, fact := range []scrapper.PlatformUserFact{
		scrapper.PlatformUserFactType, scrapper.PlatformUserFactEmail, scrapper.PlatformUserFactCreatedAt, scrapper.PlatformUserFactLastLoginAt,
	} {
		scrappertest.AssertSkipped(s.T(), users, fact, scrapper.PlatformUserSkipUnavailable)
	}
	// The fact queries run on a real server: none of them may be skipped.
	for _, fact := range []scrapper.PlatformUserFact{
		scrapper.PlatformUserFactRoles, scrapper.PlatformUserFactComment, scrapper.PlatformUserFactDefaultRole,
	} {
		s.Falsef(users.IsSkipped(fact), "%s was skipped: %v", fact, users.SkippedFacts)
	}
}
