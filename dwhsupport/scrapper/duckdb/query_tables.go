package duckdb

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecduckdb "github.com/getsynq/dwhsupport/exec/duckdb"
	"github.com/getsynq/dwhsupport/scrapper"
	_ "github.com/marcboeker/go-duckdb"
)

//go:embed query_tables.sql
var queryTablesSql string

func (e *DuckDBScrapper) QueryTables(ctx context.Context) ([]*scrapper.TableRow, error) {
	return dwhexecduckdb.NewQuerier[scrapper.TableRow](e.executor).QueryMany(ctx, queryTablesSql,
		dwhexec.WithPostProcessors(func(row *scrapper.TableRow) (*scrapper.TableRow, error) {
			row.Instance = e.conf.MotherduckAccount
			return row, nil
		}))
}
