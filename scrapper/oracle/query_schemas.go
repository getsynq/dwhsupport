package oracle

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecoracle "github.com/getsynq/dwhsupport/exec/oracle"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_schemas.sql
var querySchemasSql string

// QuerySchemas lists Oracle users as schemas. In Oracle a schema is owned by
// (and named after) a user, so the schema set equals the non-system user set —
// the same source QueryDatabases uses, but mapped to the schema level with the
// service name as the database container.
func (e *OracleScrapper) QuerySchemas(ctx context.Context) ([]*scrapper.SchemaRow, error) {
	sql := scope.AppendSchemaScopeConditions(ctx, querySchemasSql, "", "USERNAME")
	return dwhexecoracle.NewQuerier[scrapper.SchemaRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors(func(row *scrapper.SchemaRow) (*scrapper.SchemaRow, error) {
			row.Database = e.conf.ServiceName
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
