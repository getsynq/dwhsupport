package postgres

import (
	"context"

	"github.com/getsynq/dwhsupport/exec"
	execstdsql "github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *PostgresScrapper) QuerySegments(ctx context.Context, sql string, args ...any) ([]*scrapper.SegmentRow, error) {
	execer := execstdsql.NewQuerier[scrapper.SegmentRow](e.executor.GetDb())
	queryManyOpts := []exec.QueryManyOpt[scrapper.SegmentRow]{
		exec.WithArgs[scrapper.SegmentRow](args...),
	}
	return execer.QueryMany(ctx, sql, queryManyOpts...)
}
