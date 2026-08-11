package databricks

import (
	"context"

	"github.com/databricks/databricks-sdk-go/apierr"
	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/getsynq/dwhsupport/logging"
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

// listSchemas returns every schema of one catalog. A catalog dropped since it was
// listed has no schemas rather than failing the scrape — see vanished.
func (e *DatabricksScrapper) listSchemas(ctx context.Context, catalogName string) ([]servicecatalog.SchemaInfo, error) {
	schemas, err := e.client.Schemas.ListAll(ctx, servicecatalog.ListSchemasRequest{
		CatalogName: catalogName,
		MaxResults:  maxResultsPerPage,
	})
	if vanished(ctx, err, "catalog", catalogName) {
		return nil, nil
	}
	return schemas, err
}

// listTables returns every table the request selects. The caller owns the rest of
// the request — which schema, and how much of each table's metadata it needs. A
// schema dropped since it was listed has no tables rather than failing the scrape —
// see vanished.
func (e *DatabricksScrapper) listTables(
	ctx context.Context,
	request servicecatalog.ListTablesRequest,
) ([]servicecatalog.TableInfo, error) {
	request.MaxResults = maxResultsPerPage
	tables, err := e.client.Tables.ListAll(ctx, request)
	if vanished(ctx, err, "schema", request.CatalogName+"."+request.SchemaName) {
		return nil, nil
	}
	return tables, err
}

// vanished reports whether err says the object a listing named is gone, in which case
// the listing is empty rather than failed.
//
// Every walk of Unity Catalog reads a catalog's schemas, and only then each schema's
// tables, so whatever is dropped between those steps answers "does not exist" — an
// ordinary race, not a broken workspace. Returning it as the scrape's own error
// discards every catalog already walked over one object that no longer matters, and
// where schemas are created and dropped continuously (a workspace whose CI does this
// per test run) it can leave a workspace with no scrape that ever succeeds.
//
// Only a missing object is tolerated: the 404 the API answers with, carrying
// CATALOG_DOES_NOT_EXIST or SCHEMA_DOES_NOT_EXIST. Anything else — denied permission,
// a throttled or unavailable workspace — stays fatal, because silently reading no
// tables for those would report a catalog as empty when it is merely unreadable.
func vanished(ctx context.Context, err error, kind, name string) bool {
	if err == nil || !apierr.IsMissing(err) {
		return false
	}
	logging.GetLogger(ctx).WithField(kind, name).
		Warnf("%s no longer exists, skipping: %v", kind, err)
	return true
}
