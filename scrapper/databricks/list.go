package databricks

import (
	"context"

	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
)

// Every Unity Catalog listing in this package goes through the three helpers below.
//
// A listing request that names no page size is refused once its result set outgrows
// what the API returns at once ("Please adopt the paginated version of this API by
// setting max_results and paging through results with page_token", #UC-PGRQD), which
// takes down the whole scrape for a workspace that has grown past the limit. The SDK
// does not supply the page size on the caller's behalf: it appends MaxResults to the
// request's ForceSendFields, but that only governs JSON bodies, while these are query
// parameters encoded from the url struct tags — where an unset MaxResults is dropped
// by omitempty. So the page size has to be set here, and once it is, ListAll follows
// the page tokens to the end of the results.

// listCatalogs returns every catalog in the workspace.
func (e *DatabricksScrapper) listCatalogs(ctx context.Context) ([]servicecatalog.CatalogInfo, error) {
	return e.client.Catalogs.ListAll(ctx, servicecatalog.ListCatalogsRequest{MaxResults: maxResultsPerPage})
}

// listSchemas returns every schema of one catalog.
func (e *DatabricksScrapper) listSchemas(ctx context.Context, catalogName string) ([]servicecatalog.SchemaInfo, error) {
	return e.client.Schemas.ListAll(ctx, servicecatalog.ListSchemasRequest{
		CatalogName: catalogName,
		MaxResults:  maxResultsPerPage,
	})
}

// listTables returns every table the request selects. The caller owns the rest of
// the request — which schema, and how much of each table's metadata it needs.
func (e *DatabricksScrapper) listTables(
	ctx context.Context,
	request servicecatalog.ListTablesRequest,
) ([]servicecatalog.TableInfo, error) {
	request.MaxResults = maxResultsPerPage
	return e.client.Tables.ListAll(ctx, request)
}
