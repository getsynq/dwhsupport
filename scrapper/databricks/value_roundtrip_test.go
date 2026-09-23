package databricks

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// DatabricksValueRoundTripSuite checks that values read from Databricks decode to
// what they stand for and can be written back as literals that compare equal.
type DatabricksValueRoundTripSuite struct {
	scrappertest.ValueRoundTripSuite
}

func TestDatabricksValueRoundTripSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Databricks value round-trip tests in CI")
	}
	suite.Run(t, new(DatabricksValueRoundTripSuite))
}

func (s *DatabricksValueRoundTripSuite) SetupSuite() {
	s.Scrapper = newIntegrationScrapper(s.T(), s.Ctx())
}

func (s *DatabricksValueRoundTripSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
