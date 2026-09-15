package athena

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// AthenaSqlDialectExecutionSuite runs the sqldialect builder execution checks
// against the dwhtesting Athena seed.
type AthenaSqlDialectExecutionSuite struct {
	scrappertest.SqlDialectExecutionSuite
}

func TestAthenaSqlDialectExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Athena sqldialect execution tests in CI")
	}
	suite.Run(t, new(AthenaSqlDialectExecutionSuite))
}

func (s *AthenaSqlDialectExecutionSuite) SetupSuite() {
	if os.Getenv("ATHENA_ACCESS_KEY_ID") == "" || os.Getenv("ATHENA_SECRET_ACCESS_KEY") == "" {
		s.T().Skip("ATHENA_ACCESS_KEY_ID / ATHENA_SECRET_ACCESS_KEY env vars not set")
	}
	sc, err := newAthenaScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Athena: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.SqlDialectExecutionConfig{
		TableFqn: sqldialect.TableFqn(
			testenv.EnvOrDefault("ATHENA_TEST_CATALOG", "AwsDataCatalog"),
			testenv.EnvOrDefault("ATHENA_TEST_SCHEMA", "synq_dwhtesting"),
			testenv.EnvOrDefault("ATHENA_TEST_TABLE_NAME", "products"),
		),
		KeyField:     "id",
		SegmentField: testenv.EnvOrDefault("ATHENA_TEST_SEGMENT_FIELD", "category"),
	}
}

func (s *AthenaSqlDialectExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
