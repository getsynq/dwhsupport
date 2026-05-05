package athena

import (
	"context"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
	scrapperstdsql "github.com/getsynq/dwhsupport/scrapper/stdsql"
)

func (e *AthenaScrapper) QuerySegments(ctx context.Context, sql string, args ...any) ([]*scrapper.SegmentRow, error) {
	q := stdsql.NewQuerier[scrapper.SegmentRow](e.executor.GetDb())
	return q.QueryMany(ctx, sql, exec.WithArgs[scrapper.SegmentRow](args...))
}

func (e *AthenaScrapper) QueryCustomMetrics(ctx context.Context, sql string, args ...any) ([]*scrapper.CustomMetricsRow, error) {
	return scrapperstdsql.QueryCustomMetrics(ctx, e.executor, sql, args...)
}

func (e *AthenaScrapper) QueryShape(ctx context.Context, sql string) ([]*scrapper.QueryShapeColumn, error) {
	return scrapperstdsql.QueryShape(ctx, e.executor, sql)
}

func (e *AthenaScrapper) RunRawQuery(ctx context.Context, sql string) (scrapper.RawQueryRowIterator, error) {
	return scrapperstdsql.RunRawQuery(ctx, e.executor, sql)
}
