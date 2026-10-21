package postgres

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// PostgresTableConstraintsSuite checks the keys and checks of the products
// fixture. A CHECK reads as its expression, without the CHECK (...) around it.
type PostgresTableConstraintsSuite struct {
	scrappertest.TableConstraintsSuite
}

func TestPostgresTableConstraintsSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Postgres constraint tests in CI")
	}
	suite.Run(t, new(PostgresTableConstraintsSuite))
}

func (s *PostgresTableConstraintsSuite) SetupSuite() {
	if testenv.EnvOrDefault("POSTGRES_HOST", "") == "" {
		s.T().Skip("POSTGRES_HOST env var not set")
	}
	sc, err := newPostgresScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to Postgres: %v", err)
	}
	s.Scrapper = sc
	products := scrappertest.FixtureTable{Schema: "schema_a", Table: "products"}
	s.Expect = []scrappertest.Constraint{
		{Table: products, Type: scrapper.ConstraintTypePrimaryKey, Columns: []string{"id"}},
		{Table: products, Type: scrapper.ConstraintTypeCheck, Expression: "(price >= (0)::numeric)"},
		{Table: products, Type: scrapper.ConstraintTypeCheck, Expression: "((weight IS NULL) OR (weight >= (0)::double precision))"},
	}
}

func (s *PostgresTableConstraintsSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
