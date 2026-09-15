package mssql

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// MSSQLSqlDialectExecutionSuite runs the sqldialect builder execution checks
// against SQL Server.
type MSSQLSqlDialectExecutionSuite struct {
	scrappertest.SqlDialectExecutionSuite
}

func TestMSSQLSqlDialectExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MSSQL sqldialect execution tests in CI")
	}
	suite.Run(t, new(MSSQLSqlDialectExecutionSuite))
}

func (s *MSSQLSqlDialectExecutionSuite) SetupSuite() {
	sc, err := newMSSQLScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MSSQL: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.SqlDialectExecutionConfig{
		TableFqn:     sqldialect.TableFqn(testenv.EnvOrDefault("MSSQL_DATABASE", "synq_test"), "schema_a", "products"),
		KeyField:     "id",
		SegmentField: "category",
	}
}

func (s *MSSQLSqlDialectExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
