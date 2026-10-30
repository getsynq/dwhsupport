package snowflake

import (
	"context"
	"database/sql/driver"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	"github.com/samber/lo"
)

var (
	replacedAt = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	liveAt     = time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
)

// withReplacedOrders lists SALES.PUBLIC.ORDERS twice in every large
// ACCOUNT_USAGE view, as Snowflake does for an object replaced under the same
// name whose old row it never marked deleted. Table 1 is the replaced one,
// table 2 the live one.
func withReplacedOrders(fake *fakeSnowflake) {
	fake.routes = append([]fakeRoute{
		{
			match:   sqlContains("SHOW TERSE SCHEMAS IN DATABASE MARKETING"),
			columns: []string{"created_on", "name", "kind", "database_name", "schema_name"},
		},
		{
			match: sqlContains("ACCOUNT_USAGE.COLUMNS"),
			columns: []string{
				"instance", "database", "schema", "table", "is_view", "column", "type", "position", "table_id", "table_created",
			},
			rows: [][]driver.Value{
				{"ORG-ACC", "SALES", "PUBLIC", "ORDERS", false, "OLD_ID", "NUMBER", int64(1), int64(1), replacedAt},
				{"ORG-ACC", "SALES", "PUBLIC", "ORDERS", false, "ID", "NUMBER", int64(1), int64(2), liveAt},
				{"ORG-ACC", "SALES", "PUBLIC", "ORDERS", false, "ID", "NUMBER", int64(1), int64(2), liveAt},
				{"ORG-ACC", "SALES", "PUBLIC", "ORDERS", false, "AMOUNT", "NUMBER", int64(2), int64(2), liveAt},
			},
		},
		{
			match:   sqlContains("ACCOUNT_USAGE.TAG_REFERENCES"),
			columns: []string{"database", "schema", "table", "column", "domain", "tag_database", "tag_schema", "tag_name", "tag_value"},
		},
		{
			match:   sqlContains("ACCOUNT_USAGE.VIEWS"),
			columns: []string{"database", "schema", "table", "is_view", "sql", "created"},
			rows: [][]driver.Value{
				{"SALES", "PUBLIC", "ORDERS", false, "", replacedAt},
				{"SALES", "PUBLIC", "ORDERS", false, "", liveAt},
			},
		},
		{
			match:   sqlContains("ACCOUNT_USAGE.TABLES", `AS "row_count"`),
			columns: []string{"database", "schema", "table", "row_count", "size_bytes", "updated_at", "created"},
			rows: [][]driver.Value{
				{"SALES", "PUBLIC", "ORDERS", int64(1), int64(10), replacedAt, replacedAt},
				{"SALES", "PUBLIC", "ORDERS", int64(2), int64(20), liveAt, liveAt},
			},
		},
		{
			match:   sqlContains("ACCOUNT_USAGE.TABLES"),
			columns: []string{"database", "schema", "table", "table_type", "description", "is_view", "is_table", "created_at"},
			rows: [][]driver.Value{
				{"SALES", "PUBLIC", "ORDERS", "BASE TABLE", "replaced", false, true, replacedAt},
				{"SALES", "PUBLIC", "ORDERS", "BASE TABLE", "live", false, true, liveAt},
			},
		},
	}, fake.routes...)
}

func (s *AccountUsageCatalogSuite) largeReads(fake *fakeSnowflake) []string {
	return fake.statements(func(sql string) bool {
		return strings.Contains(sql, "ACCOUNT_USAGE.TABLES") ||
			strings.Contains(sql, "ACCOUNT_USAGE.VIEWS") ||
			strings.Contains(sql, "ACCOUNT_USAGE.COLUMNS")
	})
}

// A window over a whole ACCOUNT_USAGE view costs minutes on a large account,
// so the large reads keep the newest object of a name in Go.
func (s *AccountUsageCatalogSuite) TestLargeReadsKeepTheNewestObjectWithoutAWindow() {
	fake := s.fake()
	withReplacedOrders(fake)
	sc := newFakeSnowflakeScrapper(context.Background(), fake, &SnowflakeScrapperConf{
		CatalogFromAccountUsage:      true,
		TableMetricsFromAccountUsage: true,
	})
	ctx := context.Background()

	tables, err := sc.QueryTables(ctx)
	s.Require().NoError(err)
	s.Require().Len(tables, 1)
	s.Equal("live", lo.FromPtr(tables[0].Description))

	columns, err := sc.QueryCatalog(ctx)
	s.Require().NoError(err)
	s.Equal(
		[]string{"ID", "AMOUNT"},
		lo.Map(columns, func(row *scrapper.CatalogColumnRow, _ int) string { return row.Column }),
	)

	metrics, err := sc.QueryTableMetrics(ctx, time.Time{})
	s.Require().NoError(err)
	s.Require().Len(metrics, 1)
	s.Equal(int64(2), lo.FromPtr(metrics[0].RowCount))

	definitions, err := sc.QuerySqlDefinitions(ctx)
	s.Require().NoError(err)
	s.Len(definitions, 1)

	reads := s.largeReads(fake)
	s.Len(reads, 4)
	for _, read := range reads {
		s.NotContains(read, "QUALIFY")
	}
}

// A role reading ACCOUNT_USAGE often has USAGE on a database but not on all
// of its schemas, and GET_DDL of such a schema fails. Only the schemas SHOW
// SCHEMAS lists are read.
func (s *AccountUsageCatalogSuite) TestGetDdlOnlyForSchemasTheRoleCanUse() {
	fake := s.fake()
	fake.routes = append([]fakeRoute{
		{
			match:   sqlContains("ACCOUNT_USAGE.VIEWS"),
			columns: []string{"database", "schema", "table", "is_view", "sql", "created"},
			rows: [][]driver.Value{
				{"MARKETING", "PUBLIC", "CAMPAIGNS", false, "", liveAt},
				{"MARKETING", "RESTRICTED", "SPEND", false, "", liveAt},
				{"SALES", "PUBLIC", "ORDERS", false, "", liveAt},
			},
		},
		{
			match:   sqlContains("SHOW TERSE SCHEMAS IN DATABASE MARKETING"),
			columns: []string{"created_on", "name", "kind", "database_name", "schema_name"},
			rows:    [][]driver.Value{{liveAt, "PUBLIC", "", "MARKETING", ""}},
		},
		{
			match:   sqlContains("GET_DDL"),
			columns: []string{"ddl"},
			rows:    [][]driver.Value{{"create or replace schema PUBLIC;\n"}},
		},
	}, fake.routes...)
	sc := newFakeSnowflakeScrapper(context.Background(), fake, &SnowflakeScrapperConf{CatalogFromAccountUsage: true})

	_, err := sc.QuerySqlDefinitions(context.Background())
	s.Require().NoError(err)
	ddls := fake.statements(sqlContains("GET_DDL"))
	s.Require().Len(ddls, 1)
	s.Contains(ddls[0], `"MARKETING"."PUBLIC"`)
}

// A database SHOW SCHEMAS fails on gets no GET_DDL, and the read goes on.
func (s *AccountUsageCatalogSuite) TestGetDdlSkipsADatabaseShowSchemasFailsOn() {
	fake := s.fake()
	fake.routes = append([]fakeRoute{
		{
			match:   sqlContains("ACCOUNT_USAGE.VIEWS"),
			columns: []string{"database", "schema", "table", "is_view", "sql", "created"},
			rows:    [][]driver.Value{{"MARKETING", "PUBLIC", "CAMPAIGNS", false, "", liveAt}},
		},
		{
			match: sqlContains("SHOW TERSE SCHEMAS IN DATABASE MARKETING"),
			err:   errors.New("002003 (02000): SQL compilation error: Database 'MARKETING' does not exist or not authorized."),
		},
	}, fake.routes...)
	sc := newFakeSnowflakeScrapper(context.Background(), fake, &SnowflakeScrapperConf{CatalogFromAccountUsage: true})

	rows, err := sc.QuerySqlDefinitions(context.Background())
	s.Require().NoError(err)
	s.Len(rows, 1)
	s.Empty(fake.statements(sqlContains("GET_DDL")))
}

// Schema names are matched exactly, as Snowflake reports them, so a quoted
// lower-case schema and its upper-case namesake stay apart.
func (s *AccountUsageCatalogSuite) TestGetDdlMatchesSchemaNamesExactly() {
	fake := s.fake()
	fake.routes = append([]fakeRoute{
		{
			match:   sqlContains("ACCOUNT_USAGE.VIEWS"),
			columns: []string{"database", "schema", "table", "is_view", "sql", "created"},
			rows: [][]driver.Value{
				{"MARKETING", "raw", "EVENTS", false, "", liveAt},
				{"MARKETING", "RAW", "EVENTS", false, "", liveAt},
			},
		},
		{
			match:   sqlContains("SHOW TERSE SCHEMAS IN DATABASE MARKETING"),
			columns: []string{"created_on", "name", "kind", "database_name", "schema_name"},
			rows:    [][]driver.Value{{liveAt, "raw", "", "MARKETING", ""}},
		},
		{
			match:   sqlContains("GET_DDL"),
			columns: []string{"ddl"},
			rows:    [][]driver.Value{{"create or replace schema \"raw\";\n"}},
		},
	}, fake.routes...)
	sc := newFakeSnowflakeScrapper(context.Background(), fake, &SnowflakeScrapperConf{CatalogFromAccountUsage: true})

	_, err := sc.QuerySqlDefinitions(context.Background())
	s.Require().NoError(err)
	ddls := fake.statements(sqlContains("GET_DDL"))
	s.Require().Len(ddls, 1)
	s.Contains(ddls[0], `"MARKETING"."raw"`)
}

// COLUMNS and TAG_REFERENCES are read at the same time, and a refused
// TAG_REFERENCES still leaves the columns.
func (s *AccountUsageCatalogSuite) TestCatalogWithoutTagReferences() {
	fake := s.fake()
	withReplacedOrders(fake)
	fake.routes = append([]fakeRoute{{
		match: sqlContains("ACCOUNT_USAGE.TAG_REFERENCES"),
		err:   errors.New("003001 (42501): SQL access control error: Insufficient privileges to operate on view 'TAG_REFERENCES'"),
	}}, fake.routes...)
	sc := newFakeSnowflakeScrapper(context.Background(), fake, &SnowflakeScrapperConf{CatalogFromAccountUsage: true})

	columns, err := sc.QueryCatalog(context.Background())
	s.Require().NoError(err)
	s.Len(columns, 2)
}

// A failed COLUMNS read fails the catalog, also while TAG_REFERENCES is read.
func (s *AccountUsageCatalogSuite) TestCatalogFailsWhenColumnsFail() {
	fake := s.fake()
	withReplacedOrders(fake)
	fake.routes = append([]fakeRoute{{
		match: sqlContains("ACCOUNT_USAGE.COLUMNS"),
		err:   errors.New("000630 (57014): Statement reached its statement or warehouse timeout"),
	}}, fake.routes...)
	sc := newFakeSnowflakeScrapper(context.Background(), fake, &SnowflakeScrapperConf{CatalogFromAccountUsage: true})

	_, err := sc.QueryCatalog(context.Background())
	s.ErrorContains(err, "ACCOUNT_USAGE.COLUMNS")
}
