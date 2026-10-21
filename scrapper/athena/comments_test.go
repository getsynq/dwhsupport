package athena

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// AthenaCommentsSuite checks a view of the dwhtesting seed comes back without
// a comment. The seed has no commented table yet, so Table is left empty and
// the reading of comments is covered by the unit tests against a fake Glue.
type AthenaCommentsSuite struct {
	scrappertest.CommentsSuite
}

func TestAthenaCommentsSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Athena comment tests in CI")
	}
	suite.Run(t, new(AthenaCommentsSuite))
}

func (s *AthenaCommentsSuite) SetupSuite() {
	if os.Getenv("ATHENA_ACCESS_KEY_ID") == "" {
		s.T().Skip("ATHENA_ACCESS_KEY_ID env var not set")
	}
	sc, err := newAthenaScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Athena: %v", err)
	}
	s.Scrapper = sc

	schema := testenv.EnvOrDefault("ATHENA_TEST_SCHEMA", "synq_dwhtesting")
	s.View = scrappertest.FixtureTable{Schema: schema, Table: "active_products"}
	s.CatalogTableComments = true
}

func (s *AthenaCommentsSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
