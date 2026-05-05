package athena

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_sql_definitions.sql
var querySqlDefinitionsSQL string

// QuerySqlDefinitions returns view bodies. Athena's information_schema.views
// exposes view_definition as the rewritten Presto SQL (not the original DDL),
// which is what we want for lineage parsing.
//
// Iceberg/Hive table DDL (CTAS) is not currently emitted — those would need
// SHOW CREATE TABLE per object, which is a per-row API call.
func (e *AthenaScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	db := e.executor.GetDb()
	query := scope.AppendScopeConditions(ctx, querySqlDefinitionsSQL, "v.table_catalog", "v.table_schema", "v.table_name")
	rows, err := stdsql.QueryMany(ctx, db, query,
		dwhexec.WithPostProcessors(func(row *scrapper.SqlDefinitionRow) (*scrapper.SqlDefinitionRow, error) {
			row.Instance = e.executor.Instance()
			return row, nil
		}),
	)
	if err != nil {
		return nil, err
	}
	// Drop rows with empty SQL — same convention as the other scrappers.
	out := rows[:0]
	for _, r := range rows {
		if r.Sql != "" {
			out = append(out, r)
		}
	}
	return out, nil
}
