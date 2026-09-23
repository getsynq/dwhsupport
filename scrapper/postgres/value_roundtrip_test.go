package postgres

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// PostgresValueRoundTripSuite checks that values read from Postgres decode to
// what they stand for and can be written back as literals that compare equal.
type PostgresValueRoundTripSuite struct {
	scrappertest.ValueRoundTripSuite
}

func TestPostgresValueRoundTripSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Postgres value round-trip tests in CI")
	}
	suite.Run(t, new(PostgresValueRoundTripSuite))
}

func (s *PostgresValueRoundTripSuite) SetupSuite() {
	if testenv.EnvOrDefault("POSTGRES_HOST", "") == "" {
		s.T().Skip("POSTGRES_HOST env var not set")
	}
	sc, err := newPostgresScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Postgres: %v", err)
	}
	s.Scrapper = sc
}

func (s *PostgresValueRoundTripSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
