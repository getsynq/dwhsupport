package db2

import (
	"context"

	"github.com/getsynq/dwhsupport/exec"
	execstdsql "github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *Db2Scrapper) QuerySegments(ctx context.Context, sql string, args ...any) ([]*scrapper.SegmentRow, error) {
	execer := execstdsql.NewQuerier[scrapper.SegmentRow](e.executor.GetDb())
	return execer.QueryMany(ctx, sql, exec.WithArgs[scrapper.SegmentRow](args...))
}
