package postgres

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// PostgresSqlDialectExecutionSuite runs the sqldialect builder execution checks
// against Postgres.
type PostgresSqlDialectExecutionSuite struct {
	scrappertest.SqlDialectExecutionSuite
}

func TestPostgresSqlDialectExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Postgres sqldialect execution tests in CI")
	}
	suite.Run(t, new(PostgresSqlDialectExecutionSuite))
}

func (s *PostgresSqlDialectExecutionSuite) SetupSuite() {
	if testenv.EnvOrDefault("POSTGRES_HOST", "") == "" {
		s.T().Skip("POSTGRES_HOST env var not set")
	}
	sc, err := newPostgresScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Postgres: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.SqlDialectExecutionConfig{
		TableFqn:     sqldialect.TableFqn(testenv.EnvOrDefault("POSTGRES_DATABASE", "synq_test"), "schema_a", "products"),
		KeyField:     "id",
		SegmentField: "category",
	}
}

func (s *PostgresSqlDialectExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
