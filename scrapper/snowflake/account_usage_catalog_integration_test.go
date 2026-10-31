package snowflake

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/suite"
)

// AccountUsageIntegrationSuite reads the dwhtesting database twice and expects
// the same catalog: as the dwhtesting user from INFORMATION_SCHEMA, and with
// CatalogFromAccountUsage as SNOWFLAKE_CATALOG_USER with SNOWFLAKE_CATALOG_ROLE,
// a role with IMPORTED PRIVILEGES on SNOWFLAKE and no grant on the database.
// ACCOUNT_USAGE lags behind by up to three hours, so an object created or
// altered in that time is left out of the comparison, and so are streams,
// which only SHOW STREAMS lists.
type AccountUsageIntegrationSuite struct {
	suite.Suite
	database     string
	infoSchema   *SnowflakeScrapper
	accountUsage *SnowflakeScrapper
	recent       map[string]bool
	streams      map[string]bool
}

func TestAccountUsageIntegrationSuite(t *testing.T) {
	skipWithoutSnowflake(t)
	if os.Getenv("SNOWFLAKE_CATALOG_USER") == "" || os.Getenv("SNOWFLAKE_CATALOG_ROLE") == "" {
		t.Skip("SNOWFLAKE_CATALOG_USER / SNOWFLAKE_CATALOG_ROLE not set")
	}
	suite.Run(t, new(AccountUsageIntegrationSuite))
}

const accountUsageLag = 3 * time.Hour

func (s *AccountUsageIntegrationSuite) connect(conf *SnowflakeScrapperConf) *SnowflakeScrapper {
	sc, err := NewSnowflakeScrapper(context.Background(), conf)
	if err != nil {
		s.T().Skipf("Could not connect to Snowflake: %v", err)
	}
	s.T().Cleanup(func() { _ = sc.Close() })
	return sc
}

func (s *AccountUsageIntegrationSuite) SetupSuite() {
	ctx := context.Background()
	s.database = snowflakeTestConf().Databases[0]
	s.infoSchema = s.connect(&SnowflakeScrapperConf{SnowflakeConf: snowflakeTestConf()})
	catalogConf := snowflakeTestConf()
	catalogConf.User = os.Getenv("SNOWFLAKE_CATALOG_USER")
	catalogConf.Role = os.Getenv("SNOWFLAKE_CATALOG_ROLE")
	s.accountUsage = s.connect(&SnowflakeScrapperConf{
		SnowflakeConf:                catalogConf,
		CatalogFromAccountUsage:      true,
		TableMetricsFromAccountUsage: true,
	})

	metrics, err := s.infoSchema.QueryTableMetrics(ctx, time.Time{})
	s.Require().NoError(err)
	tables, err := s.infoSchema.QueryTables(ctx)
	s.Require().NoError(err)
	s.recent = map[string]bool{}
	s.streams = map[string]bool{}
	for _, m := range metrics {
		if m.UpdatedAt != nil && time.Since(*m.UpdatedAt) < accountUsageLag {
			s.recent[tableKey(m.Database, m.Schema, m.Table)] = true
		}
	}
	for _, t := range tables {
		if t.CreatedAt != nil && time.Since(*t.CreatedAt) < accountUsageLag {
			s.recent[tableKey(t.Database, t.Schema, t.Table)] = true
		}
		if t.TableType == "STREAM" {
			s.streams[tableKey(t.Database, t.Schema, t.Table)] = true
		}
	}
}

func tableKey(database, schema, table string) string {
	return fmt.Sprintf("%s.%s.%s", database, schema, table)
}

// settled drops the rows of streams and of the objects ACCOUNT_USAGE may not
// show yet, and keys the others.
func settled[T any](s *AccountUsageIntegrationSuite, rows []*T, table func(*T) string, key func(*T) string) map[string]*T {
	out := map[string]*T{}
	for _, row := range rows {
		if t := table(row); s.streams[t] || s.recent[t] {
			continue
		}
		out[key(row)] = row
	}
	return out
}

func (s *AccountUsageIntegrationSuite) TestTheRoleCannotUseTheDatabase() {
	ctx := context.Background()
	databases, err := s.accountUsage.QueryDatabases(ctx)
	s.Require().NoError(err)
	s.Contains(lo.Map(databases, func(d *scrapper.DatabaseRow, _ int) string { return d.Database }), s.database)

	warnings, err := s.accountUsage.ValidateConfiguration(ctx)
	s.Require().NoError(err)
	s.Require().Len(warnings, 1)
	s.Contains(warnings[0], "The role has no privileges on databases "+s.database+".")

	constraints, err := s.accountUsage.QueryTableConstraints(ctx)
	s.Require().NoError(err)
	s.Empty(constraints)
}

func (s *AccountUsageIntegrationSuite) TestSchemas() {
	ctx := context.Background()
	expected, err := s.infoSchema.QuerySchemas(ctx)
	s.Require().NoError(err)
	actual, err := s.accountUsage.QuerySchemas(ctx)
	s.Require().NoError(err)
	key := func(r *scrapper.SchemaRow) string { return r.Database + "." + r.Schema }
	s.NotEmpty(actual)
	s.Equal(settled(s, expected, key, key), settled(s, actual, key, key))
}

func (s *AccountUsageIntegrationSuite) TestTables() {
	ctx := context.Background()
	expected, err := s.infoSchema.QueryTables(ctx)
	s.Require().NoError(err)
	actual, err := s.accountUsage.QueryTables(ctx)
	s.Require().NoError(err)
	key := func(r *scrapper.TableRow) string { return tableKey(r.Database, r.Schema, r.Table) }
	s.NotEmpty(actual)
	s.Equal(settled(s, expected, key, key), settled(s, actual, key, key))
}

func (s *AccountUsageIntegrationSuite) TestCatalog() {
	ctx := context.Background()
	expected, err := s.infoSchema.QueryCatalog(ctx)
	s.Require().NoError(err)
	actual, err := s.accountUsage.QueryCatalog(ctx)
	s.Require().NoError(err)
	table := func(r *scrapper.CatalogColumnRow) string { return tableKey(r.Database, r.Schema, r.Table) }
	key := func(r *scrapper.CatalogColumnRow) string { return table(r) + "." + r.Column }
	s.NotEmpty(actual)
	s.Equal(settled(s, expected, table, key), settled(s, actual, table, key))
}

func (s *AccountUsageIntegrationSuite) TestTableMetrics() {
	ctx := context.Background()
	expected, err := s.infoSchema.QueryTableMetrics(ctx, time.Time{})
	s.Require().NoError(err)
	actual, err := s.accountUsage.QueryTableMetrics(ctx, time.Time{})
	s.Require().NoError(err)
	key := func(r *scrapper.TableMetricsRow) string { return tableKey(r.Database, r.Schema, r.Table) }
	s.NotEmpty(actual)
	s.Equal(settled(s, expected, key, key), settled(s, actual, key, key))
}

// TestSqlDefinitions expects the same objects and every view with SQL. The
// text of a view differs: INFORMATION_SCHEMA shows it only to the role owning
// the view, and the dwhtesting user then gets GET_DDL, while ACCOUNT_USAGE has
// the statement as it was written. ACCOUNT_USAGE also has the SQL of a secure
// view, which INFORMATION_SCHEMA and GET_DDL hide from a role not owning it.
func (s *AccountUsageIntegrationSuite) TestSqlDefinitions() {
	ctx := context.Background()
	expected, err := s.infoSchema.QuerySqlDefinitions(ctx)
	s.Require().NoError(err)
	actual, err := s.accountUsage.QuerySqlDefinitions(ctx)
	s.Require().NoError(err)
	key := func(r *scrapper.SqlDefinitionRow) string { return tableKey(r.Database, r.Schema, r.Table) }
	s.NotEmpty(actual)

	expectedRows := settled(s, expected, key, key)
	actualRows := settled(s, actual, key, key)
	s.ElementsMatch(lo.Keys(expectedRows), lo.Keys(actualRows))
	for k, row := range expectedRows {
		if !row.IsView || actualRows[k] == nil {
			continue
		}
		s.True(actualRows[k].IsView, k)
		s.NotEmpty(actualRows[k].Sql, k)
	}
}

// TestRoleWithUsageOnSomeSchemas reads the catalog as SNOWFLAKE_CATALOG_PARTIAL_USER,
// whose role holds USAGE on the database and on SCHEMA_A only. GET_DDL
// fails for every other schema, so the scraper asks only for SCHEMA_A, and its
// tables get their DDL.
func (s *AccountUsageIntegrationSuite) TestRoleWithUsageOnSomeSchemas() {
	user := os.Getenv("SNOWFLAKE_CATALOG_PARTIAL_USER")
	if user == "" {
		s.T().Skip("SNOWFLAKE_CATALOG_PARTIAL_USER not set")
	}
	conf := snowflakeTestConf()
	conf.User = user
	conf.Role = ""
	partial := s.connect(&SnowflakeScrapperConf{SnowflakeConf: conf, CatalogFromAccountUsage: true})

	logger, hook := logrustest.NewNullLogger()
	ctx := logging.WithLogger(context.Background(), logger)
	rows, err := partial.QuerySqlDefinitions(ctx)
	s.Require().NoError(err)

	for _, entry := range hook.AllEntries() {
		s.NotEqual("failed to get ddl for schema", entry.Message, entry.Data)
	}
	withDdl := lo.Filter(rows, func(row *scrapper.SqlDefinitionRow, _ int) bool { return !row.IsView && row.Sql != "" })
	s.NotEmpty(withDdl)
	for _, row := range withDdl {
		s.Equal("SCHEMA_A", row.Schema, tableKey(row.Database, row.Schema, row.Table))
	}
}
