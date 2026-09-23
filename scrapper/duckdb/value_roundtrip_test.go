package duckdb

import (
	"context"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// DuckDBValueRoundTripSuite checks that values read from in-memory DuckDB
// decode to what they stand for and can be written back as literals that
// compare equal. It needs no credentials, so it is the copy that runs in CI.
type DuckDBValueRoundTripSuite struct {
	scrappertest.ValueRoundTripSuite
}

func TestDuckDBValueRoundTripSuite(t *testing.T) {
	suite.Run(t, new(DuckDBValueRoundTripSuite))
}

func (s *DuckDBValueRoundTripSuite) SetupSuite() {
	sc, err := NewLocalDuckDBScrapper(context.Background(), "", "test_instance")
	if err != nil {
		s.T().Skipf("Could not create DuckDB scrapper: %v", err)
	}
	s.Scrapper = sc
}

func (s *DuckDBValueRoundTripSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
