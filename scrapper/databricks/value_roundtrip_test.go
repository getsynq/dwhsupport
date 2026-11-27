package databricks

import (
	"os"
	"testing"

	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
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
	sc := newIntegrationScrapper(s.T(), s.Ctx())
	s.Scrapper = sc
	executor, err := sc.Executor()
	s.Require().NoError(err)
	s.QueryMaps = dwhexecdatabricks.NewQuerier[any](executor).QueryMaps
}

func (s *DatabricksValueRoundTripSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
