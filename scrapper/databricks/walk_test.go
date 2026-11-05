package databricks

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// pagedSchemaWorkspace is a workspace with catalog "sales": schema "orders" holds
// tables tables, and schema "returns" holds one table.
func pagedSchemaWorkspace(tables int) *fakeWorkspace {
	fake := newFakeWorkspace()
	fake.addCatalog("sales")
	fake.addSchema("sales", "orders")
	for tbl := 0; tbl < tables; tbl++ {
		fake.addTable("sales", "orders", fmt.Sprintf("table_%03d", tbl))
	}
	fake.addSchema("sales", "returns")
	fake.addTable("sales", "returns", "r")
	return fake
}

// refuseTablePagesAfterTheFirst answers every table listing of sales.orders after its
// first page with the error the test gives.
func refuseTablePagesAfterTheFirst(status int, errorCode, message string) func(r *http.Request) *refusedRequest {
	return func(r *http.Request) *refusedRequest {
		query := r.URL.Query()
		if r.URL.Path == "/api/2.1/unity-catalog/tables" && query.Get("schema_name") == "orders" && query.Get("page_token") != "" {
			return &refusedRequest{status: status, errorCode: errorCode, message: message}
		}
		return nil
	}
}

type visitedPage struct {
	schema string
	tables int
}

// visitPages walks the workspace with request and returns each visit in order.
func visitPages(
	t *testing.T,
	ctx context.Context,
	s *DatabricksScrapper,
	request servicecatalog.ListTablesRequest,
) ([]visitedPage, *unityCatalogWalk, error) {
	t.Helper()
	var pages []visitedPage
	walk := s.walkUnityCatalog(ctx, scrapper.OmittedColumns)
	err := walk.forEachTablePage(ctx, request, func(schema servicecatalog.SchemaInfo, tables []servicecatalog.TableInfo) error {
		for _, table := range tables {
			require.Equal(t, schema.Name, table.SchemaName)
		}
		pages = append(pages, visitedPage{schema.Name, len(tables)})
		return nil
	})
	return pages, walk, err
}

// The walk hands a visit one page of tables at a time, never the whole schema, and the
// pages of a listing with columns are tablesWithColumnsPerPage tables long.
func TestWalkVisitsOneTablePageAtATime(t *testing.T) {
	s := pagedSchemaWorkspace(2*tablesWithColumnsPerPage + 50).start(t)

	t.Run("with columns", func(t *testing.T) {
		pages, walk, err := visitPages(t, context.Background(), s, servicecatalog.ListTablesRequest{})
		require.NoError(t, err)
		require.NoError(t, walk.result())
		require.Equal(t, []visitedPage{
			{"orders", tablesWithColumnsPerPage},
			{"orders", tablesWithColumnsPerPage},
			{"orders", 50},
			{"returns", 1},
		}, pages)
	})

	t.Run("without columns", func(t *testing.T) {
		pages, walk, err := visitPages(t, context.Background(), s, servicecatalog.ListTablesRequest{OmitColumns: true})
		require.NoError(t, err)
		require.NoError(t, walk.result())
		require.Equal(t, []visitedPage{
			{"orders", 2*tablesWithColumnsPerPage + 50},
			{"returns", 1},
		}, pages)
	})
}

// A server can serve fewer tables a page than the request asked for. The walk still
// visits full pages of the requested size, as it counts tables and not responses.
func TestWalkVisitsFullPagesWhenTheServerServesSmallerOnes(t *testing.T) {
	fake := pagedSchemaWorkspace(tablesWithColumnsPerPage + 1)
	fake.pageSize = 30
	s := fake.start(t)

	pages, _, err := visitPages(t, context.Background(), s, servicecatalog.ListTablesRequest{})
	require.NoError(t, err)
	require.Equal(t, []visitedPage{{"orders", tablesWithColumnsPerPage}, {"orders", 1}, {"returns", 1}}, pages)
}

// A schema whose listing fails after its first page is an omission of the whole schema
// for a caller that accepts incomplete results. The tables of the first page stay in
// the result, and the rest of the workspace is read.
func TestIncompleteResultsOmitASchemaWhoseLaterPageFails(t *testing.T) {
	fake := pagedSchemaWorkspace(tablesWithColumnsPerPage + 10)
	fake.refuse = refuseTablePagesAfterTheFirst(http.StatusForbidden, "PERMISSION_DENIED", "User does not have USE SCHEMA on Schema 'sales.orders'.")
	s := fake.start(t)

	rows, err := s.QueryCatalog(scrapper.WithIncompleteResults(context.Background()))
	require.Equal(t, []string{"sales.orders columns"}, omissionScopes(requireOmissions(t, err)))
	tablesBySchema := map[string]int{}
	for _, row := range rows {
		tablesBySchema[row.Schema]++
	}
	require.Equal(t, map[string]int{"orders": tablesWithColumnsPerPage, "returns": 1}, tablesBySchema)
}

// A caller that did not ask for incomplete results gets the error, and no rows.
func TestWithoutAskingASchemaWhoseLaterPageFailsFailsTheCall(t *testing.T) {
	fake := pagedSchemaWorkspace(tablesWithColumnsPerPage + 10)
	fake.refuse = refuseTablePagesAfterTheFirst(http.StatusForbidden, "PERMISSION_DENIED", "User does not have USE SCHEMA on Schema 'sales.orders'.")
	s := fake.start(t)

	rows, err := s.QueryCatalog(context.Background())
	require.ErrorContains(t, err, "USE SCHEMA")
	_, incomplete := scrapper.AsIncomplete(err)
	require.False(t, incomplete)
	require.Empty(t, rows)
}

// A schema dropped while its pages are read ends its listing quietly, like a schema
// dropped before its first page. The tables already read stay in the result.
func TestWalkEndsASchemaDroppedBetweenPagesQuietly(t *testing.T) {
	fake := pagedSchemaWorkspace(tablesWithColumnsPerPage + 10)
	fake.refuse = refuseTablePagesAfterTheFirst(http.StatusNotFound, "SCHEMA_DOES_NOT_EXIST", "Schema 'sales.orders' does not exist.")
	s := fake.start(t)

	for _, ctx := range []context.Context{context.Background(), scrapper.WithIncompleteResults(context.Background())} {
		pages, walk, err := visitPages(t, ctx, s, servicecatalog.ListTablesRequest{})
		require.NoError(t, err)
		require.NoError(t, walk.result())
		require.Equal(t, []visitedPage{{"orders", tablesWithColumnsPerPage}, {"returns", 1}}, pages)
	}
}

// A visit that fails stops the walk with its own error. It is not an omission of the
// schema, also for a caller that accepts incomplete results, and the walk asks for no
// more pages.
func TestWalkStopsOnAFailedVisit(t *testing.T) {
	fake := pagedSchemaWorkspace(2 * tablesWithColumnsPerPage)
	s := fake.start(t)
	ctx := scrapper.WithIncompleteResults(context.Background())

	visitFailed := errors.New("visit failed")
	visits := 0
	walk := s.walkUnityCatalog(ctx, scrapper.OmittedColumns)
	err := walk.forEachTablePage(ctx, servicecatalog.ListTablesRequest{}, func(servicecatalog.SchemaInfo, []servicecatalog.TableInfo) error {
		visits++
		return visitFailed
	})
	require.ErrorIs(t, err, visitFailed)
	require.Equal(t, 1, visits)
	require.NoError(t, walk.result(), "a failed visit must not be recorded as an omission")

	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.Equal(t, []int{tablesWithColumnsPerPage}, fake.tablePageSizes[false], "the walk asked for a page after the visit failed")
}
