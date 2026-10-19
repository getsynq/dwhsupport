package databricks

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
	"github.com/getsynq/dwhsupport/lazy"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/reject"
	"github.com/getsynq/dwhsupport/scrapper/sanitize"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

// partlyReadableWorkspace is a workspace in which the caller may read some of Unity
// Catalog and not the rest: catalog "sales" holds a readable schema and one whose
// tables are refused, "locked" refuses its schema listing altogether, and "finance"
// is readable. Three tables can be read: sales.orders.a, sales.orders.b and
// finance.ledger.c.
func partlyReadableWorkspace() *fakeWorkspace {
	fake := newFakeWorkspace()
	fake.addCatalog("sales")
	fake.addSchema("sales", "orders")
	fake.addTable("sales", "orders", "a")
	fake.addTable("sales", "orders", "b")
	fake.addUnreadableSchema("sales", "denied")
	fake.addUnreadableCatalog("locked")
	fake.addCatalog("finance")
	fake.addSchema("finance", "ledger")
	fake.addTable("finance", "ledger", "c")
	return fake
}

// requireOmissions asserts err is an incomplete result and returns what it left out.
func requireOmissions(t *testing.T, err error) []scrapper.Omission {
	t.Helper()
	require.Error(t, err)
	incomplete, ok := scrapper.AsIncomplete(err)
	require.True(t, ok, "expected an incomplete result, got %v", err)
	return incomplete.Omissions
}

// omissionScopes is each omission's scope and part, without the error, which
// carries the API's wording.
func omissionScopes(omissions []scrapper.Omission) []string {
	return lo.Map(omissions, func(o scrapper.Omission, _ int) string { return o.Scope() + " " + string(o.Part) })
}

func dotted(fqn scrapper.DwhFqn) string {
	return fqn.DatabaseName + "." + fqn.SchemaName + "." + fqn.ObjectName
}

// everyWalk is each query that walks Unity Catalog, reduced to the scope of each row
// it returned, and the part a listing it could not read is omitted for.
func everyWalk(s *DatabricksScrapper) map[string]struct {
	part  scrapper.OmittedPart
	query func(ctx context.Context) ([]string, error)
} {
	type walk = struct {
		part  scrapper.OmittedPart
		query func(ctx context.Context) ([]string, error)
	}
	return map[string]walk{
		"QuerySchemas": {scrapper.OmittedObjects, func(ctx context.Context) ([]string, error) {
			rows, err := s.QuerySchemas(ctx)
			return lo.Map(rows, func(r *scrapper.SchemaRow, _ int) string { return r.Database + "." + r.Schema }), err
		}},
		"QueryTables": {scrapper.OmittedObjects, func(ctx context.Context) ([]string, error) {
			rows, err := s.QueryTables(ctx)
			return lo.Map(rows, func(r *scrapper.TableRow, _ int) string { return dotted(r.TableFqn()) }), err
		}},
		"QueryCatalog": {scrapper.OmittedColumns, func(ctx context.Context) ([]string, error) {
			rows, err := s.QueryCatalog(ctx)
			return lo.Map(rows, func(r *scrapper.CatalogColumnRow, _ int) string { return dotted(r.TableFqn()) }), err
		}},
		"QuerySqlDefinitions": {scrapper.OmittedDefinitions, func(ctx context.Context) ([]string, error) {
			rows, err := s.QuerySqlDefinitions(ctx)
			return lo.Map(rows, func(r *scrapper.SqlDefinitionRow, _ int) string { return dotted(r.TableFqn()) }), err
		}},
		"QueryTableConstraints": {scrapper.OmittedObjects, func(ctx context.Context) ([]string, error) {
			rows, err := s.QueryTableConstraints(ctx)
			return lo.Map(rows, func(r *scrapper.TableConstraintRow, _ int) string { return dotted(r.TableFqn()) }), err
		}},
		"QueryTableMetrics": {scrapper.OmittedMetrics, func(ctx context.Context) ([]string, error) {
			rows, err := s.QueryTableMetrics(ctx, time.Now())
			return lo.Map(rows, func(r *scrapper.TableMetricsRow, _ int) string { return dotted(r.TableFqn()) }), err
		}},
	}
}

// A caller that accepts incomplete results gets every catalog and schema that could
// be read, and an omission for each listing that could not: the catalog whose schemas
// were refused, and the schema whose tables were.
func TestIncompleteResultsKeepWhatCouldBeRead(t *testing.T) {
	s := partlyReadableWorkspace().start(t)
	ctx := scrapper.WithIncompleteResults(context.Background())

	for name, walk := range everyWalk(s) {
		t.Run(name, func(t *testing.T) {
			rows, err := walk.query(ctx)
			omissions := requireOmissions(t, err)
			for _, o := range omissions {
				require.ErrorContains(t, o.Err, "does not have USE")
			}

			if name == "QuerySchemas" {
				require.ElementsMatch(t, []string{"sales.orders", "sales.denied", "finance.ledger"}, rows)
				require.Equal(t, []string{"locked " + string(walk.part)}, omissionScopes(omissions))
				return
			}
			require.ElementsMatch(t, []string{"sales.orders.a", "sales.orders.b", "finance.ledger.c"}, rows)
			require.Equal(t, []string{
				"sales.denied " + string(walk.part),
				"locked " + string(walk.part),
			}, omissionScopes(omissions))
		})
	}
}

// A caller that did not ask keeps the behaviour it had: the first listing that
// cannot be read fails the call.
func TestWithoutAskingAnUnreadableCatalogFailsTheCall(t *testing.T) {
	fake := newFakeWorkspace()
	fake.addCatalog("sales")
	fake.addSchema("sales", "orders")
	fake.addTable("sales", "orders", "a")
	fake.addUnreadableCatalog("locked")
	s := fake.start(t)

	for name, walk := range everyWalk(s) {
		t.Run(name, func(t *testing.T) {
			rows, err := walk.query(context.Background())
			require.ErrorContains(t, err, "USE CATALOG")
			_, incomplete := scrapper.AsIncomplete(err)
			require.False(t, incomplete)
			require.Empty(t, rows)
		})
	}
}

// A walk that could read no listing at all read nothing, which is a failure, not an
// empty partial result.
func TestIncompleteResultsOfNothingReadIsAnError(t *testing.T) {
	fake := newFakeWorkspace()
	fake.addUnreadableCatalog("locked")
	fake.addUnreadableCatalog("vault")
	s := fake.start(t)
	ctx := scrapper.WithIncompleteResults(context.Background())

	for name, walk := range everyWalk(s) {
		t.Run(name, func(t *testing.T) {
			rows, err := walk.query(ctx)
			require.ErrorContains(t, err, "USE CATALOG on Catalog 'locked'")
			_, incomplete := scrapper.AsIncomplete(err)
			require.False(t, incomplete, "a query that read nothing must fail, not return an empty partial result")
			require.Empty(t, rows)
		})
	}
}

// Nor is a schema whose only tables were refused: its catalog listed, but no table of
// the workspace was read.
func TestIncompleteResultsOfNoTableReadIsAnError(t *testing.T) {
	fake := newFakeWorkspace()
	fake.addCatalog("sales")
	fake.addUnreadableSchema("sales", "denied")
	s := fake.start(t)

	rows, err := s.QueryTables(scrapper.WithIncompleteResults(context.Background()))
	require.ErrorContains(t, err, "USE SCHEMA")
	_, incomplete := scrapper.AsIncomplete(err)
	require.False(t, incomplete)
	require.Empty(t, rows)
}

// A catalog with no schemas, or a schema with no tables, was read: it is empty. It
// keeps a failure elsewhere an incomplete result rather than nothing read.
func TestIncompleteResultsCountAnEmptyCatalogAsRead(t *testing.T) {
	fake := newFakeWorkspace()
	fake.addCatalog("empty")
	fake.addCatalog("sales")
	fake.addSchema("sales", "orders")
	fake.addUnreadableCatalog("locked")
	s := fake.start(t)

	rows, err := s.QueryTables(scrapper.WithIncompleteResults(context.Background()))
	require.Equal(t, []string{"locked objects"}, omissionScopes(requireOmissions(t, err)))
	require.Empty(t, rows)
}

// A catalog the scope leaves out is never listed, so it is never omitted.
func TestIncompleteResultsLeaveOutOfScopeCatalogsUnreported(t *testing.T) {
	s := partlyReadableWorkspace().start(t)
	ctx := scope.WithScope(
		scrapper.WithIncompleteResults(context.Background()),
		&scope.ScopeFilter{Exclude: []scope.ScopeRule{{Database: "locked"}, {Database: "sales", Schema: "denied"}}},
	)

	rows, err := s.QueryTables(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 3)
}

// A dropped catalog or schema is gone, not unreadable: it stays out of the result
// without being reported.
func TestIncompleteResultsDoNotReportVanishedObjects(t *testing.T) {
	fake := newFakeWorkspace()
	fake.addCatalog("sales")
	fake.addSchema("sales", "orders")
	fake.addTable("sales", "orders", "a")
	fake.addVanishingCatalog("dropped_catalog")
	fake.addVanishingSchema("sales", "dropped_schema")
	s := fake.start(t)
	ctx := scrapper.WithIncompleteResults(context.Background())

	for name, walk := range everyWalk(s) {
		t.Run(name, func(t *testing.T) {
			rows, err := walk.query(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, rows)
		})
	}
}

// A workspace over its quota is not a part of the workspace the caller may not read.
// The pacing has spent its attempts by the time a refusal surfaces, so the next
// catalog would only be refused too: the call fails as rate limited.
func TestIncompleteResultsStillFailOnARateLimit(t *testing.T) {
	fake := partlyReadableWorkspace()
	fake.pacing = &fastPacing
	fake.refuse = func(r *http.Request) *refusedRequest {
		if r.URL.Path == "/api/2.1/unity-catalog/schemas" && r.URL.Query().Get("catalog_name") == "finance" {
			return rateLimited()
		}
		return nil
	}
	s := fake.start(t)

	rows, err := s.QueryTables(scrapper.WithIncompleteResults(context.Background()))
	require.Error(t, err)
	_, incomplete := scrapper.AsIncomplete(err)
	require.False(t, incomplete)
	require.True(t, dwhexecdatabricks.IsRateLimitError(err), "%v", err)
	require.Empty(t, rows)
}

// A call whose context ends mid-walk fails, rather than report the rest of the
// workspace as left out.
func TestIncompleteResultsStopWhenTheContextEnds(t *testing.T) {
	fake := partlyReadableWorkspace()
	ctx, cancel := context.WithCancel(scrapper.WithIncompleteResults(context.Background()))
	defer cancel()
	fake.refuse = func(r *http.Request) *refusedRequest {
		if r.URL.Query().Get("catalog_name") == "locked" {
			cancel()
		}
		return nil
	}
	s := fake.start(t)

	rows, err := s.QueryTables(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, rows)
}

// The wrappers a caller stacks over a scrapper pass an incomplete result on whole.
func TestIncompleteResultsPassThroughTheWrappers(t *testing.T) {
	s := partlyReadableWorkspace().start(t)
	wrapped := scope.NewScopedScrapper(
		sanitize.NewSanitizingScrapper(reject.NewRejectingScrapper(s)),
		&scope.ScopeFilter{Exclude: []scope.ScopeRule{{Database: "finance"}}},
	)

	rows, err := wrapped.QueryTables(scrapper.WithIncompleteResults(context.Background()))
	require.Len(t, requireOmissions(t, err), 2)
	require.Len(t, rows, 2)
}

// failingTableReads makes the per-table read a metrics run makes after the listing
// answer with an internal error for the tables named, and with "does not exist" for
// those in gone.
func failingTableReads(fake *fakeWorkspace, failing []string, gone []string) {
	fake.refuse = func(r *http.Request) *refusedRequest {
		fullName, isRead := strings.CutPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/")
		switch {
		case !isRead:
			return nil
		case lo.Contains(failing, fullName):
			return &refusedRequest{status: http.StatusBadRequest, errorCode: "INVALID_STATE", message: "Table metadata is unavailable."}
		case lo.Contains(gone, fullName):
			return &refusedRequest{status: http.StatusNotFound, errorCode: "TABLE_DOES_NOT_EXIST", message: "Table does not exist."}
		}
		return nil
	}
}

// A table whose statistics cannot be read after the listing is left out of the
// metrics and reported, so its listing's numbers, from before the refresh, are not
// passed off as current. A table dropped since the listing is the expected race and
// stays quiet.
func TestIncompleteResultsReportATableWhoseMetricsCannotBeRead(t *testing.T) {
	fake := partlyReadableWorkspace()
	fake.readProperties = map[string]string{"spark.sql.statistics.numRows": "10"}
	failingTableReads(fake, []string{"sales.orders.a"}, []string{"sales.orders.b"})
	s := fake.startWith(t, &DatabricksScrapperConf{RefreshTableMetrics: true})

	rows, err := s.QueryTableMetrics(scrapper.WithIncompleteResults(context.Background()), time.Now().Add(-time.Hour))

	// The per-table reads run beside the walk, so their omissions can come in any order.
	omissions := requireOmissions(t, err)
	require.ElementsMatch(t, []string{
		"sales.denied metrics",
		"locked metrics",
		"sales.orders.a metrics",
	}, omissionScopes(omissions))
	tableOmission, _ := lo.Find(omissions, func(o scrapper.Omission) bool { return o.Table == "a" })
	require.ErrorContains(t, tableOmission.Err, "Table metadata is unavailable")
	require.ElementsMatch(t, []string{"sales.orders.b", "finance.ledger.c"},
		lo.Map(rows, func(r *scrapper.TableMetricsRow, _ int) string { return dotted(r.TableFqn()) }))
	for _, row := range rows {
		if row.Table == "c" {
			require.Equal(t, int64(10), *row.RowCount)
		}
	}
}

// Without asking, a table whose statistics cannot be read keeps the numbers the
// listing carried, as it always did.
func TestWithoutAskingATableWhoseMetricsCannotBeReadKeepsTheListing(t *testing.T) {
	fake := newFakeWorkspace()
	fake.addCatalog("sales")
	fake.addSchema("sales", "orders")
	fake.addTable("sales", "orders", "a")
	fake.addTable("sales", "orders", "b")
	failingTableReads(fake, []string{"sales.orders.a"}, nil)
	s := fake.startWith(t, &DatabricksScrapperConf{RefreshTableMetrics: true})

	rows, err := s.QueryTableMetrics(context.Background(), time.Now().Add(-time.Hour))
	require.NoError(t, err)
	require.Len(t, rows, 2)
}

// sqlWarehouse stands in for the SQL warehouse the scrapper runs statements on. Each
// statement it expects is answered once, in whatever order the scrapper sends them.
func sqlWarehouse(t *testing.T, s *DatabricksScrapper) sqlmock.Sqlmock {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	mock.MatchExpectationsInOrder(false)
	t.Cleanup(func() { _ = db.Close() })
	executor := dwhexecdatabricks.NewDatabricksExecutorOnDb(sqlx.NewDb(db, "sqlmock"))
	s.lazyExecutor = lazy.New(func() (*dwhexecdatabricks.DatabricksExecutor, error) { return executor, nil })
	return mock
}

func showCreate(mock sqlmock.Sqlmock, table string) *sqlmock.ExpectedQuery {
	parts := strings.Split(table, ".")
	return mock.ExpectQuery("SHOW CREATE TABLE `" + strings.Join(parts, "`.`") + "`")
}

func ddl(sql string) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"createtab_stmt"}).AddRow(sql)
}

// A table whose SHOW CREATE TABLE fails is left out of the definitions and reported,
// rather than returned with no definition. A table Databricks cannot show a CREATE
// for by design, or one dropped since the listing, keeps the quiet it had.
func TestIncompleteResultsReportATableWhoseDefinitionCannotBeRead(t *testing.T) {
	fake := newFakeWorkspace()
	fake.addCatalog("sales")
	fake.addSchema("sales", "orders")
	for _, table := range []string{"a", "b", "c", "d"} {
		fake.addTable("sales", "orders", table)
	}
	s := fake.startWith(t, &DatabricksScrapperConf{UseShowCreateTable: true})
	mock := sqlWarehouse(t, s)
	showCreate(mock, "sales.orders.a").WillReturnError(errors.New("[INSUFFICIENT_PERMISSIONS] Insufficient privileges"))
	showCreate(mock, "sales.orders.b").WillReturnError(errors.New(
		"[UNSUPPORTED_SHOW_CREATE_TABLE.ON_DATA_SOURCE_TABLE_WITH_AS_SERDE] Unsupported a SHOW CREATE TABLE command."))
	showCreate(mock, "sales.orders.c").WillReturnError(errors.New(
		"[TABLE_OR_VIEW_NOT_FOUND] The table or view `sales`.`orders`.`c` cannot be found."))
	showCreate(mock, "sales.orders.d").WillReturnRows(ddl("CREATE TABLE sales.orders.d (id BIGINT)"))

	rows, err := s.QuerySqlDefinitions(scrapper.WithIncompleteResults(context.Background()))

	omissions := requireOmissions(t, err)
	require.Equal(t, []string{"sales.orders.a definitions"}, omissionScopes(omissions))
	require.ErrorContains(t, omissions[0].Err, "INSUFFICIENT_PERMISSIONS")
	definitions := lo.SliceToMap(rows, func(r *scrapper.SqlDefinitionRow) (string, string) { return r.Table, r.Sql })
	require.Equal(t, map[string]string{"b": "", "c": "", "d": "CREATE TABLE sales.orders.d (id BIGINT)"}, definitions)
	require.NoError(t, mock.ExpectationsWereMet())
}

// Without asking, a failed SHOW CREATE TABLE leaves the table without a definition, as
// it always did.
func TestWithoutAskingATableWhoseDefinitionCannotBeReadHasNone(t *testing.T) {
	fake := newFakeWorkspace()
	fake.addCatalog("sales")
	fake.addSchema("sales", "orders")
	fake.addTable("sales", "orders", "a")
	s := fake.startWith(t, &DatabricksScrapperConf{UseShowCreateTable: true})
	mock := sqlWarehouse(t, s)
	showCreate(mock, "sales.orders.a").WillReturnError(errors.New("[INSUFFICIENT_PERMISSIONS] Insufficient privileges"))

	rows, err := s.QuerySqlDefinitions(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Empty(t, rows[0].Sql)
}

// ANALYZE TABLE only refreshes the statistics the read after it returns. It needs
// MODIFY on the table, which a read-only role lacks for every table, so a refused one
// is not a metric left out: the table's statistics are still read and returned.
func TestIncompleteResultsDoNotReportAFailedAnalyze(t *testing.T) {
	fake := newFakeWorkspace()
	fake.addCatalog("sales")
	fake.addSchema("sales", "orders")
	fake.addTable("sales", "orders", "a")
	fake.readProperties = map[string]string{"spark.sql.statistics.numRows": "10"}
	s := fake.startWith(t, &DatabricksScrapperConf{RefreshTableMetrics: true})
	mock := sqlWarehouse(t, s)
	mock.ExpectExec("ANALYZE TABLE `sales`.`orders`.`a` COMPUTE STATISTICS NOSCAN").
		WillReturnError(errors.New("[INSUFFICIENT_PERMISSIONS] User does not have MODIFY on Table 'sales.orders.a'."))

	rows, err := s.QueryTableMetrics(scrapper.WithIncompleteResults(context.Background()), time.Now().Add(-time.Hour))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, int64(10), *rows[0].RowCount)
	require.NoError(t, mock.ExpectationsWereMet())
}
