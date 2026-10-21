package trino

import (
	"context"
	"os"
	"testing"

	dwhexectrino "github.com/getsynq/dwhsupport/exec/trino"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// TrinoSystemSchemasSuite reads the self-hosted Trino, whose catalogs sit over
// SQL Server, Oracle, Postgres, MySQL and an Iceberg JDBC catalog, and checks
// that no listing carries the schemas those databases keep for themselves,
// while the schemas the fixtures created are still there.
type TrinoSystemSchemasSuite struct {
	suite.Suite
	scrapper *TrinoScrapper
}

func TestTrinoSystemSchemasSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Trino system schema tests in CI")
	}
	suite.Run(t, new(TrinoSystemSchemasSuite))
}

// systemSchemaFixtures are, per catalog of the test instance, system schemas it
// lists today and a schema a fixture created.
var systemSchemaFixtures = map[string]struct {
	system []string
	user   string
}{
	"mssql":      {system: []string{"sys", "db_owner", "guest"}, user: "dbo"},
	"oracle":     {system: []string{"dvsys", "lbacsys", "gsmadmin_internal", "audsys", "vecsys"}, user: "synq_a"},
	"postgresql": {system: []string{"pg_catalog"}, user: "schema_a"},
	"mysql":      {system: []string{"performance_schema"}, user: "synq_test"},
	"iceberg":    {system: []string{"system"}, user: "test_db"},
}

func (s *TrinoSystemSchemasSuite) SetupSuite() {
	host := testenv.EnvOrDefault("TRINO_HOST", "")
	if host == "" {
		s.T().Skip("TRINO_HOST env var not set")
	}
	catalogs := make([]string, 0, len(systemSchemaFixtures))
	for catalog := range systemSchemaFixtures {
		catalogs = append(catalogs, catalog)
	}
	sc, err := NewTrinoScrapper(context.Background(), &TrinoScrapperConf{
		TrinoConf: &dwhexectrino.TrinoConf{
			Host:      host,
			Port:      testenv.EnvOrDefaultInt("TRINO_PORT", 8080),
			User:      testenv.EnvOrDefault("TRINO_USER", "trino"),
			Password:  os.Getenv("TRINO_PASSWORD"),
			Plaintext: true,
		},
		Catalogs:               catalogs,
		FetchTableComments:     true,
		FetchMaterializedViews: true,
	})
	if err != nil {
		s.T().Skipf("Could not connect to Trino: %v", err)
	}
	s.scrapper = sc
}

func (s *TrinoSystemSchemasSuite) TearDownSuite() {
	if s.scrapper != nil {
		_ = s.scrapper.Close()
	}
}

// checkSchemas fails for a system schema in seen and, when a listing is expected
// to reach every catalog, for a fixture schema missing from it.
func (s *TrinoSystemSchemasSuite) checkSchemas(seen map[string]map[string]bool, expectUserSchemas bool) {
	for catalog, fixture := range systemSchemaFixtures {
		for _, schema := range fixture.system {
			s.False(seen[catalog][schema], "system schema %s.%s is listed", catalog, schema)
		}
		if expectUserSchemas {
			s.True(seen[catalog][fixture.user], "fixture schema %s.%s is missing", catalog, fixture.user)
		}
	}
}

func schemasOf[T scrapper.HasTableFqn](rows []T) map[string]map[string]bool {
	seen := map[string]map[string]bool{}
	for _, row := range rows {
		fqn := row.TableFqn()
		if seen[fqn.DatabaseName] == nil {
			seen[fqn.DatabaseName] = map[string]bool{}
		}
		seen[fqn.DatabaseName][fqn.SchemaName] = true
	}
	return seen
}

func (s *TrinoSystemSchemasSuite) TestQueryTables() {
	rows, err := s.scrapper.QueryTables(context.Background())
	s.Require().NoError(err)
	s.checkSchemas(schemasOf(rows), true)
}

func (s *TrinoSystemSchemasSuite) TestQueryCatalog() {
	rows, err := s.scrapper.QueryCatalog(context.Background())
	s.Require().NoError(err)
	s.checkSchemas(schemasOf(rows), true)
}

func (s *TrinoSystemSchemasSuite) TestQuerySchemas() {
	rows, err := s.scrapper.QuerySchemas(context.Background())
	s.Require().NoError(err)
	seen := map[string]map[string]bool{}
	for _, row := range rows {
		if seen[row.Database] == nil {
			seen[row.Database] = map[string]bool{}
		}
		seen[row.Database][row.Schema] = true
	}
	s.checkSchemas(seen, true)
}

func (s *TrinoSystemSchemasSuite) TestQuerySqlDefinitions() {
	rows, err := s.scrapper.QuerySqlDefinitions(context.Background())
	s.Require().NoError(err)
	s.checkSchemas(schemasOf(rows), false)
}

func (s *TrinoSystemSchemasSuite) TestQueryTableConstraints() {
	rows, err := s.scrapper.QueryTableConstraints(context.Background())
	s.Require().NoError(err)
	s.checkSchemas(schemasOf(rows), false)
}
