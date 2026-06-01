package bigquery

import (
	"context"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// scrapeError records a fatal metadata-scrape error so it is usable in both logs
// and traces, then returns it wrapped with the failing object's identity.
//
// The fan-out scrappers deliberately return (never swallow) the result: a
// partial catalog would make the un-fetched tables look deleted downstream, so a
// stalled or failing metadata call must fail the whole scrape rather than drop
// tables silently. Permission/not-found errors are filtered out by the callers
// before reaching here — those are expected and skipped.
//
// dataset/table may be empty for dataset- or project-level operations.
func (e *BigQueryScrapper) scrapeError(ctx context.Context, op, dataset, table string, err error) error {
	if err == nil {
		return nil
	}

	wrapped := errors.Wrapf(err, "bigquery %s failed (project=%s dataset=%s table=%s)", op, e.conf.ProjectId, dataset, table)

	logging.GetLogger(ctx).
		WithField("executor", "bigquery").
		WithField("bq_op", op).
		WithField("bq_project", e.conf.ProjectId).
		WithField("bq_dataset", dataset).
		WithField("bq_table", table).
		WithError(err).
		Error("BigQuery metadata scrape failed")

	// Annotate the active span so the failing object is queryable in traces.
	// Until the per-request HTTP timeout landed a stalled call emitted no span
	// at all (spans only export on completion), leaving a silent gap; now the
	// timeout surfaces an error we can attach here.
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		span.RecordError(wrapped, trace.WithAttributes(
			attribute.String("synq.bq.op", op),
			attribute.String("synq.bq.project", e.conf.ProjectId),
			attribute.String("synq.bq.dataset", dataset),
			attribute.String("synq.bq.table", table),
		))
		span.SetStatus(codes.Error, wrapped.Error())
	}

	return wrapped
}
