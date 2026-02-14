package clickhouse

import (
	"context"

	"github.com/getsynq/dwhsupport/exec"
	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *ClickhouseScrapper) QuerySegments(ctx context.Context, sql string, args ...any) ([]*scrapper.SegmentRow, error) {
	execer := dwhexecclickhouse.NewQuerier[scrapper.SegmentRow](e.executor)
	queryManyOpts := []exec.QueryManyOpt[scrapper.SegmentRow]{
		exec.WithArgs[scrapper.SegmentRow](args...),
	}
	return execer.QueryMany(ctx, sql, queryManyOpts...)
}
