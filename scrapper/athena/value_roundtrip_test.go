package athena

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// AthenaValueRoundTripSuite checks that values read from Athena decode to
// what they stand for and can be written back as literals that compare equal.
type AthenaValueRoundTripSuite struct {
	scrappertest.ValueRoundTripSuite
}

func TestAthenaValueRoundTripSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Athena value round-trip tests in CI")
	}
	suite.Run(t, new(AthenaValueRoundTripSuite))
}

func (s *AthenaValueRoundTripSuite) SetupSuite() {
	if os.Getenv("ATHENA_ACCESS_KEY_ID") == "" || os.Getenv("ATHENA_SECRET_ACCESS_KEY") == "" {
		s.T().Skip("ATHENA_ACCESS_KEY_ID / ATHENA_SECRET_ACCESS_KEY env vars not set")
	}
	sc, err := newAthenaScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Athena: %v", err)
	}
	s.Scrapper = sc
}

func (s *AthenaValueRoundTripSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
