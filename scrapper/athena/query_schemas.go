package athena

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_schemas.sql
var querySchemasSQL string

// QuerySchemas lists the Glue databases (mapped to schemas) visible to the
// connection. ScopeRule.database is the Glue Data Catalog name and
// ScopeRule.schema is the Glue database, so we populate SchemaRow.Database with
// the catalog and SchemaRow.Schema with the Glue database — mirroring QueryTables.
func (e *AthenaScrapper) QuerySchemas(ctx context.Context) ([]*scrapper.SchemaRow, error) {
	db := e.executor.GetDb()
	query := scope.AppendSchemaScopeConditions(ctx, querySchemasSQL, "catalog_name", "schema_name")
	return stdsql.QueryMany(ctx, db, query,
		dwhexec.WithPostProcessors(func(row *scrapper.SchemaRow) (*scrapper.SchemaRow, error) {
			row.Instance = e.executor.Instance()
			return row, nil
		}),
	)
}
