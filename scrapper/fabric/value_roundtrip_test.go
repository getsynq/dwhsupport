package fabric

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// FabricValueRoundTripSuite checks that values read from Fabric decode to
// what they stand for and can be written back as literals that compare equal.
type FabricValueRoundTripSuite struct {
	scrappertest.ValueRoundTripSuite
}

func TestFabricValueRoundTripSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Fabric value round-trip tests in CI")
	}
	suite.Run(t, new(FabricValueRoundTripSuite))
}

func (s *FabricValueRoundTripSuite) SetupSuite() {
	sc, err := newFabricScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Fabric: %v", err)
	}
	s.Scrapper = sc
}

func (s *FabricValueRoundTripSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
