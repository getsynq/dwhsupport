package oracle

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/stretchr/testify/suite"
)

// OracleSqlDialectExecutionSuite runs the sqldialect builder execution checks
// against Oracle.
type OracleSqlDialectExecutionSuite struct {
	scrappertest.SqlDialectExecutionSuite
}

func TestOracleSqlDialectExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Oracle sqldialect execution tests in CI")
	}
	suite.Run(t, new(OracleSqlDialectExecutionSuite))
}

func (s *OracleSqlDialectExecutionSuite) SetupSuite() {
	sc, err := newOracleScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Oracle: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.SqlDialectExecutionConfig{
		TableFqn:     sqldialect.TableFqn("FREEPDB1", "SYNQ_A", "PRODUCTS"),
		KeyField:     "ID",
		SegmentField: "CATEGORY",
	}
}

func (s *OracleSqlDialectExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		s.Scrapper.Close()
	}
}
