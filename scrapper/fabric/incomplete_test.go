package fabric

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/reject"
	"github.com/getsynq/dwhsupport/scrapper/sanitize"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// IncompleteSuite puts the scrapper in front of a workspace with two databases:
// "lake", whose every catalog read fails, and a healthy "sales".
type IncompleteSuite struct {
	suite.Suite
}

func TestIncompleteSuite(t *testing.T) {
	suite.Run(t, new(IncompleteSuite))
}

// lakeUnavailable is what the endpoint answers for a database it cannot serve.
var lakeUnavailable = mssql.Error{
	Number:  40613,
	Class:   20,
	Message: "Database 'lake' on server 'workspace.datawarehouse.fabric.microsoft.com' is not currently available.",
}

func databasesRoute(names ...string) []fakeRoute {
	rows := make([][]driver.Value, len(names))
	for i, name := range names {
		rows[i] = []driver.Value{name}
	}
	return []fakeRoute{{match: sqlContains("FROM sys.databases"), columns: []string{"name"}, rows: rows}}
}

// failingDatabase fails every statement that reads database.
func failingDatabase(database string, err error) []fakeRoute {
	return []fakeRoute{{match: sqlContains("[" + database + "]."), err: err}}
}

// healthyDatabase answers every statement the scrapper sends database.
func healthyDatabase(database string) []fakeRoute {
	db := "[" + database + "]."
	return []fakeRoute{
		{
			match:   sqlContains(db + "sys.columns c"),
			columns: []string{"schema", "table", "is_view", "table_comment", "column", "position", "type", "comment"},
			rows: [][]driver.Value{
				{"dbo", "orders", int64(0), nil, "id", int64(1), "bigint", nil},
				{"dbo", "orders", int64(0), nil, "amount", int64(2), "decimal(12,2)", nil},
			},
		},
		{
			match:   sqlContains(db + "sys.objects o"),
			columns: []string{"schema", "table", "table_type", "description", "is_view", "is_table"},
			rows:    [][]driver.Value{{"dbo", "orders", "BASE TABLE", "orders placed", int64(0), int64(1)}},
		},
		{
			match:   sqlContains(db + "sys.views v"),
			columns: []string{"schema", "table", "is_view", "is_materialized_view", "sql"},
			rows:    [][]driver.Value{{"dbo", "recent_orders", int64(1), int64(0), "CREATE VIEW dbo.recent_orders AS SELECT 1 AS x"}},
		},
		{
			match:   sqlContains(db + "sys.tables t"),
			columns: []string{"schema", "table"},
			rows:    [][]driver.Value{{"dbo", "orders"}},
		},
		{
			match:   sqlContains(db+"sys.schemas s", "schema_owner"),
			columns: []string{"schema", "schema_owner"},
			rows:    [][]driver.Value{{"dbo", nil}},
		},
		{
			match: sqlContains(db + "INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc"),
			columns: []string{
				"schema",
				"table",
				"constraint_name",
				"column_name",
				"constraint_type",
				"column_position",
				"constraint_expression",
				"is_enforced",
			},
			rows: [][]driver.Value{{"dbo", "orders", "pk_orders", "id", "PRIMARY KEY", int64(1), "", false}},
		},
		{
			match:   sqlContains("COUNT_BIG(*)", db+"[dbo].[orders]"),
			columns: []string{"schema", "table", "row_count"},
			rows:    [][]driver.Value{{"dbo", "orders", int64(42)}},
		},
	}
}

func (s *IncompleteSuite) lakeAndSales() *FabricScrapper {
	sc, _ := newFakeFabricScrapper(s.T(), databasesRoute("lake", "sales"), failingDatabase("lake", lakeUnavailable), healthyDatabase("sales"))
	return sc
}

func (s *IncompleteSuite) requireLakeOmitted(err error, part scrapper.OmittedPart) {
	incomplete, ok := scrapper.AsIncomplete(err)
	s.Require().True(ok, "expected an incomplete result, got %v", err)
	s.Require().Len(incomplete.Omissions, 1)
	omission := incomplete.Omissions[0]
	s.Equal(scrapper.Omission{Database: "lake", Part: part, Err: omission.Err}, omission)
	s.ErrorContains(omission.Err, "is not currently available")
}

func (s *IncompleteSuite) TestTablesOfAFailingDatabaseAreOmitted() {
	rows, err := s.lakeAndSales().QueryTables(scrapper.WithIncompleteResults(context.Background()))

	s.requireLakeOmitted(err, scrapper.OmittedObjects)
	s.Require().Len(rows, 1)
	s.Equal("sales", rows[0].Database)
	s.Equal("orders", rows[0].Table)
	s.Equal("orders placed", lo.FromPtr(rows[0].Description))
}

func (s *IncompleteSuite) TestColumnsOfAFailingDatabaseAreOmitted() {
	rows, err := s.lakeAndSales().QueryCatalog(scrapper.WithIncompleteResults(context.Background()))

	s.requireLakeOmitted(err, scrapper.OmittedColumns)
	s.Require().Len(rows, 2)
	s.Equal("sales", rows[0].Database)
	amount := rows[1]
	s.Equal("amount", amount.Column)
	s.Equal(scrapper.NumericCategoryDecimal, amount.NumericCategory, "numeric columns are still resolved")
	s.Equal(int64(12), lo.FromPtr(amount.Precision))
	s.Equal(int64(2), lo.FromPtr(amount.Scale))
}

func (s *IncompleteSuite) TestDefinitionsOfAFailingDatabaseAreOmitted() {
	rows, err := s.lakeAndSales().QuerySqlDefinitions(scrapper.WithIncompleteResults(context.Background()))

	s.requireLakeOmitted(err, scrapper.OmittedDefinitions)
	s.Require().Len(rows, 1)
	s.Equal("sales", rows[0].Database)
	s.Equal("recent_orders", rows[0].Table)
}

func (s *IncompleteSuite) TestMetricsOfAFailingDatabaseAreOmitted() {
	rows, err := s.lakeAndSales().QueryTableMetrics(scrapper.WithIncompleteResults(context.Background()), time.Time{})

	s.requireLakeOmitted(err, scrapper.OmittedMetrics)
	s.Require().Len(rows, 1)
	s.Equal("sales", rows[0].Database)
	s.Equal(int64(42), lo.FromPtr(rows[0].RowCount))
}

// A database whose tables are listed but whose count fails is omitted whole:
// the metrics it did count are not returned as the database's metrics.
func (s *IncompleteSuite) TestMetricsOfADatabaseWhoseCountFailsAreOmitted() {
	sc, _ := newFakeFabricScrapper(s.T(),
		databasesRoute("lake", "sales"),
		[]fakeRoute{
			{match: sqlContains("[lake].sys.tables t"), columns: []string{"schema", "table"}, rows: [][]driver.Value{{"dbo", "events"}}},
			{match: sqlContains("COUNT_BIG(*)", "[lake].[dbo].[events]"), err: lakeUnavailable},
		},
		healthyDatabase("sales"),
	)

	rows, err := sc.QueryTableMetrics(scrapper.WithIncompleteResults(context.Background()), time.Time{})

	s.requireLakeOmitted(err, scrapper.OmittedMetrics)
	s.Require().Len(rows, 1)
	s.Equal("sales", rows[0].Database)
}

func (s *IncompleteSuite) TestWrappersPassIncompleteResultsThrough() {
	wrapped := scope.NewScopedScrapper(
		sanitize.NewSanitizingScrapper(reject.NewRejectingScrapper(s.lakeAndSales())),
		&scope.ScopeFilter{Include: []scope.ScopeRule{{Database: "lake"}, {Database: "sales"}}},
	)
	ctx := scrapper.WithIncompleteResults(context.Background())

	tables, err := wrapped.QueryTables(ctx)
	s.requireLakeOmitted(err, scrapper.OmittedObjects)
	s.Len(tables, 1)

	columns, err := wrapped.QueryCatalog(ctx)
	s.requireLakeOmitted(err, scrapper.OmittedColumns)
	s.Len(columns, 2)

	definitions, err := wrapped.QuerySqlDefinitions(ctx)
	s.requireLakeOmitted(err, scrapper.OmittedDefinitions)
	s.Len(definitions, 1)

	metrics, err := wrapped.QueryTableMetrics(ctx, time.Time{})
	s.requireLakeOmitted(err, scrapper.OmittedMetrics)
	s.Len(metrics, 1)
}

// Every database is reported, in the order they are listed, whichever read
// finished first.
func (s *IncompleteSuite) TestEveryFailingDatabaseIsReportedInListingOrder() {
	sc, _ := newFakeFabricScrapper(s.T(),
		databasesRoute("archive", "lake", "sales"),
		failingDatabase("archive", lakeUnavailable),
		failingDatabase("lake", lakeUnavailable),
		healthyDatabase("sales"),
	)

	rows, err := sc.QueryTables(scrapper.WithIncompleteResults(context.Background()))

	incomplete, ok := scrapper.AsIncomplete(err)
	s.Require().True(ok, "expected an incomplete result, got %v", err)
	s.Equal([]string{"archive", "lake"}, lo.Map(incomplete.Omissions, func(o scrapper.Omission, _ int) string { return o.Database }))
	s.Require().Len(rows, 1)
	s.Equal("sales", rows[0].Database)
}

func (s *IncompleteSuite) TestNothingReadIsAnError() {
	sc, _ := newFakeFabricScrapper(s.T(), databasesRoute("lake"), failingDatabase("lake", lakeUnavailable))
	ctx := scrapper.WithIncompleteResults(context.Background())

	rows, err := sc.QueryTables(ctx)
	s.Require().Error(err)
	_, incomplete := scrapper.AsIncomplete(err)
	s.False(incomplete, "a call that read no database is a failure, not a partial result")
	s.ErrorContains(err, "is not currently available")
	s.Nil(rows)

	metrics, err := sc.QueryTableMetrics(ctx, time.Time{})
	s.Require().Error(err)
	_, incomplete = scrapper.AsIncomplete(err)
	s.False(incomplete)
	s.Nil(metrics)
}

func (s *IncompleteSuite) TestWithoutAskingAFailingDatabaseFailsTheCall() {
	sc := s.lakeAndSales()
	ctx := context.Background()

	tables, err := sc.QueryTables(ctx)
	s.requirePlainFailure(err)
	s.Nil(tables)

	columns, err := sc.QueryCatalog(ctx)
	s.requirePlainFailure(err)
	s.Nil(columns)

	definitions, err := sc.QuerySqlDefinitions(ctx)
	s.requirePlainFailure(err)
	s.Nil(definitions)

	metrics, err := sc.QueryTableMetrics(ctx, time.Time{})
	s.requirePlainFailure(err)
	s.Nil(metrics)
}

// Schemas and constraints keep failing the call: the wrappers do not forward an
// incomplete result for them, so a partial listing would arrive as an error
// with no rows anyway.
func (s *IncompleteSuite) TestSchemasAndConstraintsStillFailTheCall() {
	sc := s.lakeAndSales()
	ctx := scrapper.WithIncompleteResults(context.Background())

	schemas, err := sc.QuerySchemas(ctx)
	s.requirePlainFailure(err)
	s.Nil(schemas)

	constraints, err := sc.QueryTableConstraints(ctx)
	s.requirePlainFailure(err)
	s.Nil(constraints)
}

func (s *IncompleteSuite) TestAHealthyWorkspaceIsComplete() {
	sc, _ := newFakeFabricScrapper(s.T(), databasesRoute("sales", "warehouse"), healthyDatabase("sales"), healthyDatabase("warehouse"))

	rows, err := sc.QueryTables(scrapper.WithIncompleteResults(context.Background()))

	s.Require().NoError(err)
	s.Equal([]string{"sales", "warehouse"}, lo.Map(rows, func(r *scrapper.TableRow, _ int) string { return r.Database }))
}

func (s *IncompleteSuite) TestAWorkspaceWithNoDatabaseInScopeIsEmpty() {
	sc, fake := newFakeFabricScrapper(s.T(), databasesRoute("lake"), failingDatabase("lake", lakeUnavailable))
	ctx := scope.WithScope(scrapper.WithIncompleteResults(context.Background()),
		&scope.ScopeFilter{Exclude: []scope.ScopeRule{{Database: "lake"}}})

	rows, err := sc.QueryTables(ctx)

	s.Require().NoError(err)
	s.Empty(rows)
	s.Len(fake.seen, 1, "only the database listing is read")
}

// A database left out of scope is not read, so its failure is not reported.
func (s *IncompleteSuite) TestAFailingDatabaseOutOfScopeIsNotReported() {
	ctx := scope.WithScope(scrapper.WithIncompleteResults(context.Background()),
		&scope.ScopeFilter{Include: []scope.ScopeRule{{Database: "sales"}}})

	rows, err := s.lakeAndSales().QueryTables(ctx)

	s.Require().NoError(err)
	s.Require().Len(rows, 1)
	s.Equal("sales", rows[0].Database)
}

// A cancelled call fails, even for a caller that accepts incomplete results:
// the databases it did not get to are not failures of theirs.
func (s *IncompleteSuite) TestACancelledCallFails() {
	sc := s.lakeAndSales()
	ctx, cancel := context.WithCancel(scrapper.WithIncompleteResults(context.Background()))
	_, err := sc.GetExistingDbs(ctx)
	s.Require().NoError(err)
	cancel()

	rows, err := sc.QueryTables(ctx)

	s.Require().ErrorIs(err, context.Canceled)
	_, incomplete := scrapper.AsIncomplete(err)
	s.False(incomplete)
	s.Nil(rows)
}

func (s *IncompleteSuite) requirePlainFailure(err error) {
	s.T().Helper()
	s.Require().Error(err)
	_, incomplete := scrapper.AsIncomplete(err)
	s.False(incomplete, "expected a plain failure, got %v", err)
	s.ErrorContains(err, "is not currently available")
}

// A context cancelled while the databases are being read fails the call, though
// one database had already been read: the read that saw the cancellation did
// not fail on the database's account.
func TestReadDatabasesCancelledMidReadFails(t *testing.T) {
	ctx, cancel := context.WithCancel(scrapper.WithIncompleteResults(context.Background()))
	defer cancel()
	sales := make(chan struct{})

	rows, err := readDatabases(ctx, []string{"sales", "lake"}, scrapper.OmittedObjects,
		func(ctx context.Context, database string) ([]*scrapper.TableRow, error) {
			if database == "sales" {
				defer close(sales)
				return []*scrapper.TableRow{{Database: database}}, nil
			}
			<-sales
			cancel()
			return nil, ctx.Err()
		})

	require.ErrorIs(t, err, context.Canceled)
	_, incomplete := scrapper.AsIncomplete(err)
	require.False(t, incomplete)
	require.Nil(t, rows)
}
