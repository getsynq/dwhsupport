package fabric

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// FabricSqlDialectExecutionSuite runs the sqldialect builder execution checks
// against Microsoft Fabric Warehouse, whose T-SQL is a subset of SQL Server's.
type FabricSqlDialectExecutionSuite struct {
	scrappertest.SqlDialectExecutionSuite
}

func TestFabricSqlDialectExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Fabric sqldialect execution tests in CI")
	}
	suite.Run(t, new(FabricSqlDialectExecutionSuite))
}

func (s *FabricSqlDialectExecutionSuite) SetupSuite() {
	sc, err := newFabricScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Fabric: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.SqlDialectExecutionConfig{
		TableFqn: sqldialect.TableFqn(
			testenv.EnvOrDefault("FABRIC_DATABASE", "COALESCE_QUALITY_DWHTESTING"),
			"sales",
			"products",
		),
		KeyField:     "id",
		SegmentField: "category",
	}
}

func (s *FabricSqlDialectExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
