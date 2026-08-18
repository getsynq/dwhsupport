package clickhouse

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_schemas.sql
var querySchemasSql string

// QuerySchemas lists ClickHouse databases as schemas. ClickHouse has no
// catalog/database level above its databases, so each ClickHouse database is
// mapped to a SchemaRow under the connection's own identity — see rowIdentity,
// which every scrapped row shares.
func (e *ClickhouseScrapper) QuerySchemas(ctx context.Context) ([]*scrapper.SchemaRow, error) {
	sql := scope.AppendSchemaScopeConditions(ctx, e.systemTablesSql(querySchemasSql), "", "name")
	return dwhexecclickhouse.NewQuerier[scrapper.SchemaRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors[scrapper.SchemaRow](func(row *scrapper.SchemaRow) (*scrapper.SchemaRow, error) {
			row.Instance, row.Database = e.rowIdentity()
			return row, nil
		}),
	)
}
