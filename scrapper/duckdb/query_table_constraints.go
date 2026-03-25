package duckdb

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecduckdb "github.com/getsynq/dwhsupport/exec/duckdb"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_table_constraints.sql
var queryTableConstraintsSql string

func (e *DuckDBScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	sql := scope.AppendScopeConditions(ctx, queryTableConstraintsSql, "", "\"schema\"", "\"table\"")
	return dwhexecduckdb.NewQuerier[scrapper.TableConstraintRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors(func(row *scrapper.TableConstraintRow) (*scrapper.TableConstraintRow, error) {
			row.Instance = e.conf.MotherduckAccount
			if row.ConstraintType == "UNIQUE" {
				row.ConstraintType = scrapper.ConstraintTypeUniqueIndex
			}
			return row, nil
		}),
	)
}
