package snowflake

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"
	"time"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/reject"
	"github.com/getsynq/dwhsupport/scrapper/sanitize"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/samber/lo"
	gosnowflake "github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/suite"
)

// IncompleteSuite puts the scrapper in front of an account with a healthy
// database SALES beside the imported database PARTNER_SHARE, whose publisher
// stopped sharing it: Snowflake still lists it in SHOW DATABASES and fails
// every read of it with 003030. OLD_SHARE is just as unavailable, but no
// longer configured.
type IncompleteSuite struct {
	suite.Suite
}

func TestIncompleteSuite(t *testing.T) {
	suite.Run(t, new(IncompleteSuite))
}

var (
	shareUnavailable = &gosnowflake.SnowflakeError{
		Number:   3030,
		SQLState: "02000",
		Message: "Shared database is no longer available for use. " +
			"It will need to be re-created if and when the publisher makes it available again.",
	}
	notAuthorized = &gosnowflake.SnowflakeError{
		Number:   2003,
		SQLState: "02000",
		Message:  "SQL compilation error:\nDatabase 'PARTNER_SHARE' does not exist or not authorized.",
	}
)

func showDatabases() fakeRoute {
	return fakeRoute{
		match:   sqlContains("SHOW DATABASES"),
		columns: []string{"name", "origin", "owner", "comment", "kind"},
		rows: [][]driver.Value{
			{"SALES", "", "SYSADMIN", "", "STANDARD"},
			{"PARTNER_SHARE", "PARTNER.ACCOUNT.SHARE", "", "", "IMPORTED DATABASE"},
			{"OLD_SHARE", "PARTNER.ACCOUNT.OLD", "", "", "IMPORTED DATABASE"},
		},
	}
}

// healthySales answers every query the scrapper sends the SALES database.
func healthySales() []fakeRoute {
	return []fakeRoute{
		{
			match:   sqlContains("SHOW STREAMS IN DATABASE SALES"),
			columns: []string{"created_on", "name", "database_name", "schema_name", "table_name"},
		},
		{
			match:   sqlContains("SALES.information_schema.tables", `as "table_type"`),
			columns: []string{"database", "schema", "table", "table_type", "description", "is_view", "is_table"},
			rows:    [][]driver.Value{{"SALES", "PUBLIC", "ORDERS", "BASE TABLE", "", false, true}},
		},
		{
			match: sqlContains("SALES.information_schema.columns"),
			columns: []string{
				"instance", "database", "schema", "table", "is_view", "column", "type", "numeric_precision", "numeric_scale", "position",
			},
			rows: [][]driver.Value{{"ORG-ACCOUNT", "SALES", "PUBLIC", "ORDERS", false, "AMOUNT", "NUMBER", int64(12), int64(2), int64(1)}},
		},
		{
			match:   sqlContains("ACCOUNT_USAGE.TAG_REFERENCES"),
			columns: tagReferenceColumns,
		},
		{
			match:   sqlContains("SALES.information_schema.tables", `as "row_count"`),
			columns: []string{"database", "schema", "table", "row_count", "size_bytes", "updated_at"},
			rows:    [][]driver.Value{{"SALES", "PUBLIC", "ORDERS", int64(25), int64(1024), time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}},
		},
		{
			match:   sqlContains("SALES.information_schema.views"),
			columns: []string{"database", "schema", "table", "is_view", "sql"},
			rows:    [][]driver.Value{{"SALES", "PUBLIC", "ORDERS", false, ""}},
		},
		{
			match:   sqlContains("SALES.information_schema.schemata"),
			columns: []string{"database", "schema", "description", "schema_owner"},
			rows:    [][]driver.Value{{"SALES", "PUBLIC", "", "SYSADMIN"}},
		},
		{
			match:   sqlContains("GET_DDL('SCHEMA', '\"SALES\".\"PUBLIC\"'"),
			columns: []string{"GET_DDL"},
			rows:    [][]driver.Value{{"create or replace TABLE ORDERS (AMOUNT NUMBER(12,2)) COMMENT='orders';"}},
		},
	}
}

// partnerShareFailing fails every read of PARTNER_SHARE with err.
func partnerShareFailing(err error) []fakeRoute {
	return []fakeRoute{{
		match: func(sql string) bool {
			return strings.Contains(sql, "PARTNER_SHARE.") || strings.Contains(sql, "IN DATABASE PARTNER_SHARE")
		},
		err: err,
	}}
}

func (s *IncompleteSuite) scrapper(ctx context.Context, databases []string, routes ...[]fakeRoute) (*SnowflakeScrapper, *fakeSnowflake) {
	fake := &fakeSnowflake{t: s.T(), routes: append([]fakeRoute{showDatabases()}, lo.Flatten(routes)...)}
	conf := &SnowflakeScrapperConf{SnowflakeConf: dwhexecsnowflake.SnowflakeConf{Account: "ORG-ACCOUNT", Databases: databases}}
	return newFakeSnowflakeScrapper(ctx, fake, conf), fake
}

func isIncomplete(err error) bool {
	_, incomplete := scrapper.AsIncomplete(err)
	return incomplete
}

func (s *IncompleteSuite) requireOmissions(err error) []scrapper.Omission {
	incomplete, ok := scrapper.AsIncomplete(err)
	s.Require().True(ok, "expected an incomplete result, got %v", err)
	return incomplete.Omissions
}

// listing is one per-database read of the scrapper, with the part an omission
// of a whole database leaves out, and the databases of the rows it returned.
type listing struct {
	name string
	part scrapper.OmittedPart
	read func(ctx context.Context, scr scrapper.Scrapper) ([]string, error)
}

func databasesOf[T any](rows []*T, database func(*T) string, err error) ([]string, error) {
	return lo.Uniq(lo.Map(rows, func(r *T, _ int) string { return database(r) })), err
}

var listings = []listing{
	{"QueryTables", scrapper.OmittedObjects, func(ctx context.Context, scr scrapper.Scrapper) ([]string, error) {
		rows, err := scr.QueryTables(ctx)
		return databasesOf(rows, func(r *scrapper.TableRow) string { return r.Database }, err)
	}},
	{"QueryCatalog", scrapper.OmittedColumns, func(ctx context.Context, scr scrapper.Scrapper) ([]string, error) {
		rows, err := scr.QueryCatalog(ctx)
		return databasesOf(rows, func(r *scrapper.CatalogColumnRow) string { return r.Database }, err)
	}},
	{"QueryTableMetrics", scrapper.OmittedMetrics, func(ctx context.Context, scr scrapper.Scrapper) ([]string, error) {
		rows, err := scr.QueryTableMetrics(ctx, time.Time{})
		return databasesOf(rows, func(r *scrapper.TableMetricsRow) string { return r.Database }, err)
	}},
	{"QuerySqlDefinitions", scrapper.OmittedDefinitions, func(ctx context.Context, scr scrapper.Scrapper) ([]string, error) {
		rows, err := scr.QuerySqlDefinitions(ctx)
		return databasesOf(rows, func(r *scrapper.SqlDefinitionRow) string { return r.Database }, err)
	}},
	{"QuerySchemas", scrapper.OmittedObjects, func(ctx context.Context, scr scrapper.Scrapper) ([]string, error) {
		rows, err := scr.QuerySchemas(ctx)
		return databasesOf(rows, func(r *scrapper.SchemaRow) string { return r.Database }, err)
	}},
}

func (s *IncompleteSuite) TestAnUnavailableShareIsOmitted() {
	for _, l := range listings {
		s.Run(l.name, func() {
			ctx := scrapper.WithIncompleteResults(context.Background())
			scr, _ := s.scrapper(ctx, []string{"SALES", "PARTNER_SHARE"}, healthySales(), partnerShareFailing(shareUnavailable))

			databases, err := l.read(ctx, scr)

			omissions := s.requireOmissions(err)
			s.Require().Len(omissions, 1)
			s.Equal("PARTNER_SHARE", omissions[0].Database)
			s.Empty(omissions[0].Schema)
			s.Equal(l.part, omissions[0].Part)
			s.True(isSharedDatabaseUnavailableError(omissions[0].Err), "the omission carries Snowflake's own error: %v", omissions[0].Err)
			s.Equal([]string{"SALES"}, databases)
		})
	}
}

func (s *IncompleteSuite) TestAnyOtherFailureOfOneDatabaseIsOmitted() {
	for _, l := range listings {
		s.Run(l.name, func() {
			ctx := scrapper.WithIncompleteResults(context.Background())
			scr, _ := s.scrapper(ctx, []string{"SALES", "PARTNER_SHARE"}, healthySales(), partnerShareFailing(notAuthorized))

			databases, err := l.read(ctx, scr)

			omissions := s.requireOmissions(err)
			s.Require().Len(omissions, 1)
			s.Equal("PARTNER_SHARE", omissions[0].Database)
			s.Equal(l.part, omissions[0].Part)
			s.Contains(omissions[0].Err.Error(), "does not exist or not authorized")
			s.Equal([]string{"SALES"}, databases)
		})
	}
}

// A caller that did not ask for incomplete results keeps what it always had:
// an unavailable share is skipped, and any other failure fails the call.
func (s *IncompleteSuite) TestWithoutTheOptInAnUnavailableShareIsSkipped() {
	for _, l := range listings {
		s.Run(l.name, func() {
			ctx := context.Background()
			scr, _ := s.scrapper(ctx, []string{"SALES", "PARTNER_SHARE"}, healthySales(), partnerShareFailing(shareUnavailable))

			databases, err := l.read(ctx, scr)

			s.Require().NoError(err)
			s.Equal([]string{"SALES"}, databases)
		})
	}
}

func (s *IncompleteSuite) TestWithoutTheOptInAnyOtherFailureFailsTheCall() {
	for _, l := range listings {
		s.Run(l.name, func() {
			ctx := context.Background()
			scr, _ := s.scrapper(ctx, []string{"SALES", "PARTNER_SHARE"}, healthySales(), partnerShareFailing(notAuthorized))

			databases, err := l.read(ctx, scr)

			s.Require().Error(err)
			s.False(isIncomplete(err), "a plain error, not an incomplete result")
			s.Contains(err.Error(), "does not exist or not authorized")
			s.Empty(databases)
		})
	}
}

// Reading nothing is not a partial read: when every database failed, the call
// fails with the first failure, so the caller cannot mistake it for an empty
// account with a warning.
func (s *IncompleteSuite) TestEveryDatabaseFailingFailsTheCall() {
	for _, l := range listings {
		s.Run(l.name, func() {
			ctx := scrapper.WithIncompleteResults(context.Background())
			scr, _ := s.scrapper(ctx, []string{"PARTNER_SHARE"}, partnerShareFailing(shareUnavailable))

			databases, err := l.read(ctx, scr)

			s.Require().Error(err)
			s.False(isIncomplete(err), "a plain error, not an incomplete result")
			s.True(isSharedDatabaseUnavailableError(err), "%v", err)
			s.Empty(databases)
		})
	}
}

// A share that is no longer configured, or that the scope leaves out, is not
// read at all, so it goes away from the catalog like any dropped database
// instead of being reported on every refresh.
func (s *IncompleteSuite) TestAShareNotAskedForIsNotRead() {
	for _, l := range listings {
		s.Run(l.name+"/not configured", func() {
			ctx := scrapper.WithIncompleteResults(context.Background())
			scr, fake := s.scrapper(ctx, []string{"SALES"}, healthySales(), partnerShareFailing(shareUnavailable))

			databases, err := l.read(ctx, scr)

			s.Require().NoError(err)
			s.Equal([]string{"SALES"}, databases)
			s.Empty(fake.statements(sqlContains("PARTNER_SHARE")))
			s.Empty(fake.statements(sqlContains("OLD_SHARE")))
		})
		s.Run(l.name+"/out of scope", func() {
			ctx := scope.WithScope(
				scrapper.WithIncompleteResults(context.Background()),
				&scope.ScopeFilter{Exclude: []scope.ScopeRule{{Database: "PARTNER_SHARE"}}},
			)
			scr, fake := s.scrapper(ctx, []string{"SALES", "PARTNER_SHARE"}, healthySales(), partnerShareFailing(shareUnavailable))

			databases, err := l.read(ctx, scr)

			s.Require().NoError(err)
			s.Equal([]string{"SALES"}, databases)
			s.Empty(fake.statements(sqlContains("PARTNER_SHARE")))
		})
	}
}

func (s *IncompleteSuite) TestACancelledCallFailsRatherThanOmitting() {
	for _, l := range listings {
		s.Run(l.name, func() {
			ctx, cancel := context.WithCancel(scrapper.WithIncompleteResults(context.Background()))
			scr, _ := s.scrapper(ctx, []string{"SALES", "PARTNER_SHARE"}, healthySales(), partnerShareFailing(shareUnavailable))
			_, err := scr.GetExistingDbs(ctx)
			s.Require().NoError(err)
			cancel()

			_, err = l.read(ctx, scr)

			s.Require().Error(err)
			s.False(isIncomplete(err), "a cancelled call is a failure, not an incomplete result: %v", err)
		})
	}
}

// The wrappers a caller stacks over a scrapper pass an incomplete result on
// whole, rows and omissions both.
func (s *IncompleteSuite) TestWrappersPassIncompleteResultsThrough() {
	for _, l := range listings {
		s.Run(l.name, func() {
			ctx := scrapper.WithIncompleteResults(context.Background())
			inner, _ := s.scrapper(ctx, []string{"SALES", "PARTNER_SHARE"}, healthySales(), partnerShareFailing(shareUnavailable))
			wrapped := scope.NewScopedScrapper(sanitize.NewSanitizingScrapper(reject.NewRejectingScrapper(inner)), nil)

			databases, err := l.read(ctx, wrapped)

			s.Len(s.requireOmissions(err), 1)
			s.Equal([]string{"SALES"}, databases)
		})
	}
}

// A caller reading incomplete results hears about an unavailable share from
// each read, as an omission naming the database, so the configuration check
// does not say it a second time. One that did not ask is still warned.
func (s *IncompleteSuite) TestValidateConfigurationLeavesAnUnavailableShareToTheReads() {
	s.Run("incomplete results accepted", func() {
		ctx := scrapper.WithIncompleteResults(context.Background())
		scr, fake := s.scrapper(ctx, []string{"SALES", "PARTNER_SHARE", "GONE"}, partnerShareFailing(shareUnavailable))

		warnings, err := scr.ValidateConfiguration(ctx)

		s.Require().NoError(err)
		s.Equal([]string{"Database not found or no permissions to access: GONE"}, warnings)
		s.Empty(fake.statements(sqlContains("SELECT 1 FROM")), "no database is probed")
	})
	s.Run("incomplete results not accepted", func() {
		ctx := context.Background()
		scr, _ := s.scrapper(ctx, []string{"SALES", "PARTNER_SHARE", "GONE"}, partnerShareFailing(shareUnavailable), []fakeRoute{{
			match:   sqlContains("SELECT 1 FROM SALES.information_schema.tables"),
			columns: []string{"1"},
			rows:    [][]driver.Value{{int64(1)}},
		}})

		warnings, err := scr.ValidateConfiguration(ctx)

		s.Require().NoError(err)
		s.Require().Len(warnings, 2)
		s.Equal("Database not found or no permissions to access: GONE", warnings[0])
		s.Contains(warnings[1], "Shared database(s) no longer available and will be skipped during data extraction: PARTNER_SHARE.")
	})
}
