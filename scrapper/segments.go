package scrapper

type SegmentRow struct {
	Segment string `db:"segment"`
	Count   *int64 `db:"count"`
}
