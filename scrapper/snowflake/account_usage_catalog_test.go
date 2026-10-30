package snowflake

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/stretchr/testify/suite"
)

// AccountUsageCatalogSuite puts a CatalogFromAccountUsage scrapper in front of
// an account whose ACCOUNT_USAGE lists the databases SALES, MARKETING and the
// share PARTNER_SHARE, while the role holds a privilege on MARKETING only, so
// SHOW DATABASES lists only that one.
type AccountUsageCatalogSuite struct {
	suite.Suite
}

func TestAccountUsageCatalogSuite(t *testing.T) {
	suite.Run(t, new(AccountUsageCatalogSuite))
}

func (s *AccountUsageCatalogSuite) fake() *fakeSnowflake {
	return &fakeSnowflake{t: s.T(), routes: []fakeRoute{
		{
			match:   sqlContains("ACCOUNT_USAGE.DATABASES"),
			columns: []string{"name", "origin", "owner", "comment", "kind"},
			rows: [][]driver.Value{
				{"SALES", "", "SYSADMIN", "", "STANDARD"},
				{"MARKETING", "", "SYSADMIN", "", "STANDARD"},
				{"PARTNER_SHARE", "", "", "", "IMPORTED DATABASE"},
				{"SNOWFLAKE", "", "", "", "APPLICATION"},
				{"USER$ALICE", "", "", "", "PERSONAL DATABASE"},
			},
		},
		{
			match:   sqlContains("SHOW DATABASES"),
			columns: []string{"name", "origin", "owner", "comment", "kind"},
			rows:    [][]driver.Value{{"MARKETING", "", "SYSADMIN", "", "STANDARD"}},
		},
		{
			match:   sqlContains("SHOW STREAMS IN DATABASE MARKETING"),
			columns: []string{"created_on", "name", "database_name", "schema_name", "table_name"},
		},
		{
			match:   sqlContains("ACCOUNT_USAGE.TABLES"),
			columns: []string{"database", "schema", "table", "table_type", "description", "is_view", "is_table"},
			rows:    [][]driver.Value{{"SALES", "PUBLIC", "ORDERS", "BASE TABLE", "", false, true}},
		},
	}}
}

func (s *AccountUsageCatalogSuite) tablesQuery(fake *fakeSnowflake) string {
	statements := fake.statements(sqlContains("ACCOUNT_USAGE.TABLES"))
	s.Require().Len(statements, 1)
	return statements[0]
}

func (s *AccountUsageCatalogSuite) TestReadsAllDatabasesInOneQuery() {
	fake := s.fake()
	sc := newFakeSnowflakeScrapper(context.Background(), fake, &SnowflakeScrapperConf{CatalogFromAccountUsage: true})

	rows, err := sc.QueryTables(context.Background())
	s.Require().NoError(err)
	s.Len(rows, 1)

	query := s.tablesQuery(fake)
	s.Contains(query, "t.table_catalog IN ('SALES', 'MARKETING', 'PARTNER_SHARE')")
	s.Empty(fake.statements(func(sql string) bool { return strings.Contains(strings.ToLower(sql), ".information_schema.") }))
	// SHOW STREAMS goes only to the database the role holds a privilege on.
	s.Empty(fake.statements(sqlContains("SHOW STREAMS IN DATABASE SALES")))
	s.Len(fake.statements(sqlContains("SHOW STREAMS IN DATABASE MARKETING")), 1)
}

func (s *AccountUsageCatalogSuite) TestScopeExcludesGoIntoTheQuery() {
	fake := s.fake()
	sc := newFakeSnowflakeScrapper(context.Background(), fake, &SnowflakeScrapperConf{CatalogFromAccountUsage: true})
	ctx := scope.WithScope(context.Background(), &scope.ScopeFilter{
		Exclude: []scope.ScopeRule{{Database: "PARTNER_SHARE"}, {Database: "SALES", Schema: "STAGING"}},
	})

	_, err := sc.QueryTables(ctx)
	s.Require().NoError(err)

	query := s.tablesQuery(fake)
	s.Contains(query, "t.table_catalog IN ('SALES', 'MARKETING')")
	s.Contains(query, "NOT (LOWER(t.table_catalog) = 'sales' AND LOWER(t.table_schema) = 'staging')")
}

func (s *AccountUsageCatalogSuite) TestConfiguredDatabasesLimitTheRead() {
	fake := s.fake()
	sc := newFakeSnowflakeScrapper(context.Background(), fake, &SnowflakeScrapperConf{
		CatalogFromAccountUsage: true,
	})
	sc.conf.Databases = []string{"SALES"}

	_, err := sc.QueryTables(context.Background())
	s.Require().NoError(err)
	s.Contains(s.tablesQuery(fake), "t.table_catalog IN ('SALES')")
}

func (s *AccountUsageCatalogSuite) TestTableMetricsAreReadWhereTheRoleCanUseTheDatabase() {
	fake := s.fake()
	fake.routes = append(fake.routes, fakeRoute{
		match:   sqlContains("MARKETING.information_schema.tables", `as "row_count"`),
		columns: []string{"database", "schema", "table", "row_count", "size_bytes", "updated_at"},
	})
	sc := newFakeSnowflakeScrapper(context.Background(), fake, &SnowflakeScrapperConf{CatalogFromAccountUsage: true})

	_, err := sc.QueryTableMetrics(context.Background(), time.Time{})
	s.Require().NoError(err)
	s.Empty(fake.statements(sqlContains("ACCOUNT_USAGE.TABLES")))
	s.Empty(fake.statements(sqlContains("SALES.information_schema")))
	s.Len(fake.statements(sqlContains("MARKETING.information_schema.tables")), 1)
}

func (s *AccountUsageCatalogSuite) TestTableMetricsFromAccountUsage() {
	fake := s.fake()
	fake.routes = append([]fakeRoute{{
		match:   sqlContains("ACCOUNT_USAGE.TABLES", `AS "row_count"`),
		columns: []string{"database", "schema", "table", "row_count", "size_bytes", "updated_at"},
	}}, fake.routes...)
	sc := newFakeSnowflakeScrapper(context.Background(), fake, &SnowflakeScrapperConf{
		CatalogFromAccountUsage:      true,
		TableMetricsFromAccountUsage: true,
	})

	_, err := sc.QueryTableMetrics(context.Background(), time.Time{})
	s.Require().NoError(err)
	s.Len(fake.statements(sqlContains("ACCOUNT_USAGE.TABLES", `AS "row_count"`)), 1)
	s.Empty(fake.statements(sqlContains("information_schema")))
}

func TestDatabaseIdent(t *testing.T) {
	for name, want := range map[string]string{
		"SALES":          "SALES",
		"_STAGE$1":       "_STAGE$1",
		"Sales":          `"Sales"`,
		"sales data":     `"sales data"`,
		`odd"name`:       `"odd""name"`,
		"1ST":            `"1ST"`,
		"foo - . :: bar": `"foo - . :: bar"`,
	} {
		if got := databaseIdent(name); got != want {
			t.Errorf("databaseIdent(%q) = %s, want %s", name, got, want)
		}
	}
}

func (s *AccountUsageCatalogSuite) TestWarnsAboutDatabasesWithoutPrivileges() {
	fake := s.fake()
	sc := newFakeSnowflakeScrapper(context.Background(), fake, &SnowflakeScrapperConf{CatalogFromAccountUsage: true})

	warnings, err := sc.ValidateConfiguration(context.Background())
	s.Require().NoError(err)
	s.Require().Len(warnings, 1)
	s.Contains(warnings[0], "The role has no privileges on databases SALES, PARTNER_SHARE.")
}
