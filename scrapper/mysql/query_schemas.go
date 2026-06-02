package mysql

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecmysql "github.com/getsynq/dwhsupport/exec/mysql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_schemas.sql
var querySchemasSql string

// QuerySchemas lists MySQL databases as schemas. MySQL has no separate
// schema level (a "schema" is a synonym for a database), so we map each
// MySQL database to a SchemaRow and use the host as the DatabaseRow.Database
// container — consistent with QueryTables.
func (e *MySQLScrapper) QuerySchemas(ctx context.Context) ([]*scrapper.SchemaRow, error) {
	sql := scope.AppendSchemaScopeConditions(ctx, querySchemasSql, "", "schema_name")
	return dwhexecmysql.NewQuerier[scrapper.SchemaRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors(func(row *scrapper.SchemaRow) (*scrapper.SchemaRow, error) {
			row.Database = e.conf.Host
			return row, nil
		}),
	)
}
