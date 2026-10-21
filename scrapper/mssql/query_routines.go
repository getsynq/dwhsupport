package mssql

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecmssql "github.com/getsynq/dwhsupport/exec/mssql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_routines.sql
var queryRoutinesSql string

func (e *MSSQLScrapper) QueryRoutines(ctx context.Context) ([]*scrapper.RoutineRow, error) {
	sql := scope.AppendScopeConditions(ctx, queryRoutinesSql, "", "s.name", "o.name")
	return dwhexecmssql.NewQuerier[scrapper.RoutineRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors(func(row *scrapper.RoutineRow) (*scrapper.RoutineRow, error) {
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
