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
	table := func(name string) scrappertest.FixtureTable {
		return scrappertest.FixtureTable{Schema: "schema_a", Table: name}
	}
	s.ForeignKeys = []scrappertest.ForeignKey{
		{Table: table("order_items"), Columns: []string{"product_id"}, Referenced: table("products"), ReferencedColumns: []string{"id"}},
		// A composite key into a partitioned table.
		{
			Table:             table("order_event_notes"),
			Columns:           []string{"event_id", "event_month"},
			Referenced:        table("order_events"),
			ReferencedColumns: []string{"event_id", "event_month"},
		},
		// A key declared on a partitioned table, and the one each partition inherits.
		{Table: table("order_event_tags"), Columns: []string{"product_id"}, Referenced: table("products"), ReferencedColumns: []string{"id"}},
		{Table: table("order_event_tags_2026_01"), Columns: []string{"product_id"}, Referenced: table("products"), ReferencedColumns: []string{"id"}},
	}
	// Postgres also stores the key into order_events once for each of its
	// partitions, which is not a foreign key of its own.
	s.NeverReferenced = []scrappertest.FixtureTable{table("order_events_2026_01"), table("order_events_2026_02")}
}

func (s *PostgresForeignKeysSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
