package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/exec"
	execbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
)

type dwhBigquerySegmentResponse struct {
	Segment bigquery.NullString `ch:"segment" bigquery:"segment" db:"segment"`
	Count   bigquery.NullInt64  `ch:"count" bigquery:"count" db:"count"`
}

func (r *dwhBigquerySegmentResponse) GetSegment() string {
	if r.Segment.Valid {
		return r.Segment.String()
	}
	return ""
}

func (r *dwhBigquerySegmentResponse) GetCount() *int64 {
	if r.Count.Valid {
		return lo.ToPtr(r.Count.Int64)
	}
	return nil
}

func (e *BigQueryScrapper) QuerySegments(ctx context.Context, sql string, args ...any) ([]*scrapper.SegmentRow, error) {
	var rows []*scrapper.SegmentRow
	querier := execbigquery.NewQuerier[dwhBigquerySegmentResponse](e.executor)
	var opts []exec.QueryManyOpt[dwhBigquerySegmentResponse]
	opts = append(opts, exec.WithArgs[dwhBigquerySegmentResponse](args...))
	tmpRows, err := querier.QueryMany(ctx, sql, opts...)
	if err != nil {
		return nil, err
	}
	for _, row := range tmpRows {
		rows = append(rows, &scrapper.SegmentRow{
			Segment: row.GetSegment(),
			Count:   row.GetCount(),
		})
	}

	return rows, nil
}
