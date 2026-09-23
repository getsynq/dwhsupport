package trino

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// StarburstValueRoundTripSuite checks that values read from Starburst decode to
// what they stand for and can be written back as literals that compare equal.
type StarburstValueRoundTripSuite struct {
	scrappertest.ValueRoundTripSuite
}

func TestStarburstValueRoundTripSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Starburst value round-trip tests in CI")
	}
	suite.Run(t, new(StarburstValueRoundTripSuite))
}

func (s *StarburstValueRoundTripSuite) SetupSuite() {
	if testenv.EnvOrDefault("STARBURST_HOST", "") == "" {
		s.T().Skip("STARBURST_HOST env var not set")
	}
	sc, err := newTrinoScrapperFromEnv(s.Ctx(), testenv.EnvOrDefault("TRINO_TEST_CATALOG", "tpch"))
	if err != nil {
		s.T().Skipf("Could not connect to Starburst: %v", err)
	}
	s.Scrapper = sc
}

func (s *StarburstValueRoundTripSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// SelfHostedTrinoValueRoundTripSuite checks that values read from self-hosted Trino decode to
// what they stand for and can be written back as literals that compare equal.
type SelfHostedTrinoValueRoundTripSuite struct {
	scrappertest.ValueRoundTripSuite
}

func TestSelfHostedTrinoValueRoundTripSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping self-hosted Trino value round-trip tests in CI")
	}
	suite.Run(t, new(SelfHostedTrinoValueRoundTripSuite))
}

func (s *SelfHostedTrinoValueRoundTripSuite) SetupSuite() {
	if testenv.EnvOrDefault("TRINO_HOST", "") == "" {
		s.T().Skip("TRINO_HOST env var not set")
	}
	sc, err := newSelfHostedTrinoScrapperFromEnv(s.Ctx(), testenv.EnvOrDefault("TRINO_TEST_CATALOG", "tpch"))
	if err != nil {
		s.T().Skipf("Could not connect to self-hosted Trino: %v", err)
	}
	s.Scrapper = sc
}

func (s *SelfHostedTrinoValueRoundTripSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
