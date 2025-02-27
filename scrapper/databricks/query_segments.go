package databricks

import (
	"context"

	"github.com/getsynq/dwhsupport/exec"
	execstdsql "github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *DatabricksScrapper) QuerySegments(ctx context.Context, sql string, args ...any) ([]*scrapper.SegmentRow, error) {
	executor, err := e.lazyExecutor.Get()
	if err != nil {
		return nil, err
	}
	execer := execstdsql.NewQuerier[scrapper.SegmentRow](executor.GetDb())
	queryManyOpts := []exec.QueryManyOpt[scrapper.SegmentRow]{
		exec.WithArgs[scrapper.SegmentRow](args...),
	}
	return execer.QueryMany(ctx, sql, queryManyOpts...)
}
