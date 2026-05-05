package athena

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_databases.sql
var queryDatabasesSQL string

// QueryDatabases lists Glue databases visible to the connection.
//
// Note on terminology: ScopeRule.database is the Glue Data Catalog name
// (almost always 'AwsDataCatalog'); ScopeRule.schema is the Glue database.
// We populate DatabaseRow.Database with the *Glue database* name here so
// downstream code that treats DatabaseRow as the top-level container sees
// what the user actually filters on. The real catalog lives on the row's
// Instance (and the catalog string is captured via the executor's Catalog()).
func (e *AthenaScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	db := e.executor.GetDb()
	return stdsql.QueryMany(ctx, db, queryDatabasesSQL,
		dwhexec.WithPostProcessors(func(row *scrapper.DatabaseRow) (*scrapper.DatabaseRow, error) {
			row.Instance = e.executor.Instance()
			return row, nil
		}),
	)
}
