package postgres

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// PostgresForeignKeysSuite checks the foreign key of the order_items fixture.
type PostgresForeignKeysSuite struct {
	scrappertest.ForeignKeysSuite
}

func TestPostgresForeignKeysSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Postgres constraint tests in CI")
	}
	suite.Run(t, new(PostgresForeignKeysSuite))
}

func (s *PostgresForeignKeysSuite) SetupSuite() {
	if testenv.EnvOrDefault("POSTGRES_HOST", "") == "" {
		s.T().Skip("POSTGRES_HOST env var not set")
	}
	sc, err := newPostgresScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Postgres: %v", err)
	}
	s.Scrapper = sc
	s.ForeignKeys = []scrappertest.ForeignKey{{
		Table:             scrappertest.FixtureTable{Schema: "schema_a", Table: "order_items"},
		Columns:           []string{"product_id"},
		Referenced:        scrappertest.FixtureTable{Schema: "schema_a", Table: "products"},
		ReferencedColumns: []string{"id"},
	}}
}

func (s *PostgresForeignKeysSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
