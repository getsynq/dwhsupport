package clickhouse

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// ClickhouseValueRoundTripSuite checks that values read from ClickHouse decode to
// what they stand for and can be written back as literals that compare equal.
type ClickhouseValueRoundTripSuite struct {
	scrappertest.ValueRoundTripSuite
}

func TestClickhouseValueRoundTripSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping ClickHouse value round-trip tests in CI")
	}
	suite.Run(t, new(ClickhouseValueRoundTripSuite))
}

func (s *ClickhouseValueRoundTripSuite) SetupSuite() {
	if testenv.EnvOrDefault("CLICKHOUSE_HOST", "") == "" {
		s.T().Skip("CLICKHOUSE_HOST env var not set")
	}
	sc, err := newClickhouseScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to ClickHouse: %v", err)
	}
	s.Scrapper = sc
}

func (s *ClickhouseValueRoundTripSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
