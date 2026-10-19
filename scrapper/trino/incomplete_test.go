package trino

import (
	"context"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/reject"
	"github.com/getsynq/dwhsupport/scrapper/sanitize"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
)

// IncompleteSuite puts the scrapper in front of a Trino whose Iceberg catalog
// "lake" lists its tables but cannot open their metadata, which is what Trino
// answers when the catalog's object storage is unreachable, beside a healthy
// "tpch" catalog.
type IncompleteSuite struct {
	suite.Suite
}

func TestIncompleteSuite(t *testing.T) {
	suite.Run(t, new(IncompleteSuite))
}

const lakeStorageError = "Failed to open input stream for file: s3://lake/warehouse/sales/orders/metadata/00000.metadata.json"

var (
	tableColumns = []fakeColumn{
		{"database", "varchar"}, {"schema", "varchar"}, {"table", "varchar"}, {"table_type", "varchar"},
		{"description", "varchar"}, {"is_table", "boolean"}, {"is_view", "boolean"},
	}
	catalogColumns = []fakeColumn{
		{"database", "varchar"}, {"schema", "varchar"}, {"table", "varchar"}, {"column", "varchar"},
		{"position", "bigint"}, {"type", "varchar"}, {"comment", "varchar"}, {"table_comment", "varchar"},
	}
	sqlDefinitionColumns = []fakeColumn{
		{"database", "varchar"}, {"schema", "varchar"}, {"table", "varchar"},
		{"is_view", "boolean"}, {"is_materialized_view", "boolean"}, {"sql", "varchar"},
	}
	showStatsColumns = []fakeColumn{
		{"column_name", "varchar"}, {"data_size", "double"}, {"distinct_values_count", "double"},
		{"nulls_fraction", "double"}, {"row_count", "double"}, {"low_value", "varchar"}, {"high_value", "varchar"},
	}
)

func catalogsRoute(catalogs ...[]any) fakeRoute {
	return fakeRoute{
		match:   sqlContains("system.metadata.catalogs"),
		columns: []fakeColumn{{"catalog_name", "varchar"}, {"connector_id", "varchar"}, {"connector_name", "varchar"}},
		rows:    catalogs,
	}
}

func failing(match func(string) bool, errorType, message string) fakeRoute {
	return fakeRoute{match: match, err: &fakeTrinoError{errorType: errorType, message: message}}
}

// healthyTpch answers every query the scrapper sends the tpch catalog.
func healthyTpch() []fakeRoute {
	return []fakeRoute{
		{
			match:   sqlContains("FROM tpch.information_schema.tables t\nJOIN tpch.information_schema.columns"),
			columns: catalogColumns,
			rows: [][]any{
				{"tpch", "tiny", "nation", "nationkey", 1, "bigint", nil, "nations"},
				{"tpch", "tiny", "nation", "balance", 2, "decimal(12,2)", nil, "nations"},
			},
		},
		{
			match:   sqlContains("from tpch.information_schema.tables", "information_schema.views"),
			columns: sqlDefinitionColumns,
		},
		{
			match:   sqlContains("FROM tpch.information_schema.tables t"),
			columns: tableColumns,
			rows: [][]any{
				{"tpch", "tiny", "nation", "BASE TABLE", "nations", true, false},
				{"tpch", "information_schema_like", "no_stats", "BASE TABLE", "", true, false},
			},
		},
		failing(sqlContains(`SHOW STATS FOR "tpch"."information_schema_like"."no_stats"`), "USER_ERROR",
			"Table 'information_schema_like.no_stats' has no supported columns (all 0 columns are not supported)"),
		{
			match:   sqlContains(`SHOW STATS FOR "tpch"."tiny"."nation"`),
			columns: showStatsColumns,
			rows:    [][]any{{nil, nil, nil, nil, 25.0, nil, nil}},
		},
	}
}

// lakeWithUnreachableStorage answers like an Iceberg catalog whose storage
// cannot be read: the listing alone works, anything that loads a table's
// metadata fails.
func lakeWithUnreachableStorage() []fakeRoute {
	return []fakeRoute{
		failing(sqlContains("FROM lake.information_schema.tables t", "table_comments"), "INTERNAL_ERROR", lakeStorageError),
		failing(sqlContains("lake.information_schema.columns"), "INTERNAL_ERROR", "Error listing table columns for catalog lake: "+lakeStorageError),
		failing(sqlContains(`SHOW STATS FOR "lake"`), "INTERNAL_ERROR", lakeStorageError),
		{
			match:   sqlContains("from lake.information_schema.tables", "information_schema.views"),
			columns: sqlDefinitionColumns,
		},
		{
			match:   sqlLacks(sqlContains("FROM lake.information_schema.tables t"), "table_comments"),
			columns: tableColumns,
			rows:    [][]any{{"lake", "sales", "orders", "BASE TABLE", "", true, false}},
		},
	}
}

func (s *IncompleteSuite) scrapper(ctx context.Context, routes ...[]fakeRoute) *TrinoScrapper {
	fake := &fakeTrino{t: s.T(), routes: lo.Flatten(routes)}
	return newFakeTrinoScrapper(s.T(), ctx, fake, TrinoScrapperConf{
		Catalogs:           []string{"lake", "tpch"},
		FetchTableComments: true,
	})
}

func lakeAndTpch() []fakeRoute {
	return []fakeRoute{catalogsRoute([]any{"lake", "lake", "iceberg"}, []any{"tpch", "tpch", "tpch"})}
}

func (s *IncompleteSuite) requireOmissions(err error) []scrapper.Omission {
	incomplete, ok := scrapper.AsIncomplete(err)
	s.Require().True(ok, "expected an incomplete result, got %v", err)
	return incomplete.Omissions
}

func (s *IncompleteSuite) TestTablesOfAnUnreadableCatalogAreListedWithoutDescriptions() {
	ctx := scrapper.WithIncompleteResults(context.Background())
	scr := s.scrapper(ctx, lakeAndTpch(), lakeWithUnreachableStorage(), healthyTpch())

	rows, err := scr.QueryTables(ctx)

	omissions := s.requireOmissions(err)
	s.Require().Len(omissions, 1)
	s.Equal("lake", omissions[0].Database)
	s.Empty(omissions[0].Schema)
	s.Equal(scrapper.OmittedDescriptions, omissions[0].Part)
	s.Contains(omissions[0].Err.Error(), lakeStorageError)

	byTable := lo.KeyBy(rows, func(r *scrapper.TableRow) string { return r.Table })
	s.Require().Len(rows, 3)
	s.Equal("nations", lo.FromPtr(byTable["nation"].Description))
	lake := byTable["orders"]
	s.Require().NotNil(lake)
	s.Equal("orders", lake.Table)
	s.True(lake.IsTable)
}

func (s *IncompleteSuite) TestColumnsOfAnUnreadableCatalogAreOmitted() {
	ctx := scrapper.WithIncompleteResults(context.Background())
	scr := s.scrapper(ctx, lakeAndTpch(), lakeWithUnreachableStorage(), healthyTpch())

	rows, err := scr.QueryCatalog(ctx)

	omissions := s.requireOmissions(err)
	s.Require().Len(omissions, 1)
	s.Equal(scrapper.Omission{Database: "lake", Part: scrapper.OmittedColumns, Err: omissions[0].Err}, omissions[0])
	s.Require().Len(rows, 2)
	s.Equal("tpch", rows[0].Database)
	s.Equal("nationkey", rows[0].Column)
	balance := rows[1]
	s.Equal(scrapper.NumericCategoryDecimal, balance.NumericCategory, "numeric columns are still resolved")
	s.Equal(int64(12), lo.FromPtr(balance.Precision))
	s.Equal(int64(2), lo.FromPtr(balance.Scale))
}

func (s *IncompleteSuite) TestMetricsOfUnreadableTablesAreOmitted() {
	ctx := scrapper.WithIncompleteResults(context.Background())
	scr := s.scrapper(ctx, lakeAndTpch(), lakeWithUnreachableStorage(), healthyTpch())

	rows, err := scr.QueryTableMetrics(ctx, time.Time{})

	omissions := s.requireOmissions(err)
	parts := lo.Map(omissions, func(o scrapper.Omission, _ int) string { return string(o.Part) + " " + o.Scope() })
	s.Equal([]string{"metrics lake.sales.orders"}, parts,
		"a table Trino refuses statistics for by design (USER_ERROR) is not reported, only one it failed to read")
	s.Require().Len(rows, 1)
	s.Equal("nation", rows[0].Table)
	s.Equal(int64(25), lo.FromPtr(rows[0].RowCount))
}

// The wrappers a caller stacks over a scrapper pass an incomplete result on
// whole, rows and omissions both.
func (s *IncompleteSuite) TestWrappersPassIncompleteResultsThrough() {
	ctx := scrapper.WithIncompleteResults(context.Background())
	inner := s.scrapper(ctx, lakeAndTpch(), lakeWithUnreachableStorage(), healthyTpch())
	wrapped := scope.NewScopedScrapper(
		sanitize.NewSanitizingScrapper(reject.NewRejectingScrapper(inner)),
		&scope.ScopeFilter{Exclude: []scope.ScopeRule{{Database: "tpch", Schema: "sf1"}}},
	)

	tables, err := wrapped.QueryTables(ctx)
	s.Len(s.requireOmissions(err), 1)
	s.Len(tables, 3)

	columns, err := wrapped.QueryCatalog(ctx)
	s.Len(s.requireOmissions(err), 1)
	s.Len(columns, 2)

	metrics, err := wrapped.QueryTableMetrics(ctx, time.Time{})
	s.Len(s.requireOmissions(err), 1)
	s.Len(metrics, 1)
}

func (s *IncompleteSuite) TestSqlDefinitionsAreUnaffected() {
	ctx := scrapper.WithIncompleteResults(context.Background())
	scr := s.scrapper(ctx, lakeAndTpch(), lakeWithUnreachableStorage(), healthyTpch())

	_, err := scr.QuerySqlDefinitions(ctx)
	s.Require().NoError(err)
}

func (s *IncompleteSuite) TestAnUnavailableCatalogIsReportedWhenAsked() {
	ctx := scrapper.WithIncompleteResults(context.Background())
	unavailable := failing(sqlContains("lake.information_schema"), "EXTERNAL",
		"Error listing tables for catalog lake: The connection attempt failed.")
	scr := s.scrapper(ctx, lakeAndTpch(), []fakeRoute{unavailable}, healthyTpch())

	rows, err := scr.QueryTables(ctx)

	omissions := s.requireOmissions(err)
	s.Require().Len(omissions, 1)
	s.Equal("lake", omissions[0].Database)
	s.Equal(scrapper.OmittedObjects, omissions[0].Part)
	s.Require().Len(rows, 2)
	s.Equal("tpch", rows[0].Database)
}

func (s *IncompleteSuite) TestNothingReadIsAnError() {
	ctx := scrapper.WithIncompleteResults(context.Background())
	everything := failing(sqlContains("information_schema"), "INTERNAL_ERROR", lakeStorageError)
	scr := s.scrapper(ctx, lakeAndTpch(), []fakeRoute{everything})

	rows, err := scr.QueryTables(ctx)

	s.Require().Error(err)
	_, incomplete := scrapper.AsIncomplete(err)
	s.False(incomplete, "a query that read nothing must fail, not return an empty partial result")
	s.Contains(err.Error(), lakeStorageError)
	s.Empty(rows)
}

// A caller that did not ask for incomplete results keeps the behaviour it had:
// a catalog Trino fails a query on fails the call, and an unavailable catalog is
// skipped.
func (s *IncompleteSuite) TestWithoutAskingAFailingCatalogFailsTheCall() {
	ctx := context.Background()
	scr := s.scrapper(ctx, lakeAndTpch(), lakeWithUnreachableStorage(), healthyTpch())

	_, err := scr.QueryTables(ctx)
	s.Require().Error(err)
	_, incomplete := scrapper.AsIncomplete(err)
	s.False(incomplete)

	_, err = scr.QueryCatalog(ctx)
	s.Require().Error(err)
	_, incomplete = scrapper.AsIncomplete(err)
	s.False(incomplete)
}

func (s *IncompleteSuite) TestWithoutAskingAnUnavailableCatalogIsSkipped() {
	ctx := context.Background()
	unavailable := failing(sqlContains("lake.information_schema"), "EXTERNAL",
		"Error listing tables for catalog lake: The connection attempt failed.")
	scr := s.scrapper(ctx, lakeAndTpch(), []fakeRoute{unavailable}, healthyTpch())

	rows, err := scr.QueryTables(ctx)
	s.Require().NoError(err)
	s.Require().Len(rows, 2)
	s.Equal("tpch", rows[0].Database)
}

// Without table comments the column listing is what loads each table's
// metadata, so a catalog whose storage cannot be read fails it with Trino's
// "Error listing table columns for catalog lake: …". That is a failed read, not
// a catalog that is gone, and skipping it would publish the catalog's tables
// without their columns.
func (s *IncompleteSuite) TestWithoutAskingAFailedColumnListingFailsTheCall() {
	ctx := context.Background()
	fake := &fakeTrino{t: s.T(), routes: lo.Flatten([][]fakeRoute{lakeAndTpch(), lakeWithUnreachableStorage(), healthyTpch()})}
	scr := newFakeTrinoScrapper(s.T(), ctx, fake, TrinoScrapperConf{Catalogs: []string{"lake", "tpch"}})

	rows, err := scr.QueryCatalog(ctx)

	s.Require().Error(err)
	s.Contains(err.Error(), "Error listing table columns for catalog lake")
	s.Empty(rows)
}
