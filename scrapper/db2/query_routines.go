package db2

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecdb2 "github.com/getsynq/dwhsupport/exec/db2"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_routines.sql
var queryRoutinesSql string

func (e *Db2Scrapper) QueryRoutines(ctx context.Context) ([]*scrapper.RoutineRow, error) {
	sql := scope.AppendScopeConditions(ctx, queryRoutinesSql, "", "r.ROUTINESCHEMA", "r.ROUTINENAME")
	return dwhexecdb2.NewQuerier[scrapper.RoutineRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors(func(row *scrapper.RoutineRow) (*scrapper.RoutineRow, error) {
			row.Instance = e.conf.Hostname
			return row, nil
		}),
	)
}
