package snowflake

import (
	"context"

	"github.com/getsynq/dwhsupport/exec"
	execsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *SnowflakeScrapper) QuerySegments(ctx context.Context, sql string, args ...any) ([]*scrapper.SegmentRow, error) {
	execer := execsnowflake.NewQuerier[scrapper.SegmentRow](e.executor)
	queryManyOpts := []exec.QueryManyOpt[scrapper.SegmentRow]{
		exec.WithArgs[scrapper.SegmentRow](args...),
	}
	return execer.QueryMany(ctx, sql, queryManyOpts...)
}
