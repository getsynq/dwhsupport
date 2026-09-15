package clickhouse

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// ClickhouseSqlDialectExecutionSuite runs the sqldialect builder execution
// checks against ClickHouse.
type ClickhouseSqlDialectExecutionSuite struct {
	scrappertest.SqlDialectExecutionSuite
}

func TestClickhouseSqlDialectExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping ClickHouse sqldialect execution tests in CI")
	}
	suite.Run(t, new(ClickhouseSqlDialectExecutionSuite))
}

func (s *ClickhouseSqlDialectExecutionSuite) SetupSuite() {
	testDB := testenv.EnvOrDefault("CLICKHOUSE_DATABASE", "default")
	if testDB == "default" {
		s.T().Skip("CLICKHOUSE_DATABASE not set to a test database (e.g. synq_test)")
	}
	sc, err := newClickhouseScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to ClickHouse: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.SqlDialectExecutionConfig{
		TableFqn:     sqldialect.TableFqn("", testDB, "products"),
		KeyField:     "id",
		SegmentField: "category",
	}
}

func (s *ClickhouseSqlDialectExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
