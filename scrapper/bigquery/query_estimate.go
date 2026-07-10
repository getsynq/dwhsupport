package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
)

// EstimateQuery uses a BigQuery dry-run job to obtain an authoritative estimate
// of the bytes the query would process. A dry run does not execute the query
// and is not billed; BigQuery bills on TotalBytesProcessed, so this doubles as a
// cost estimate.
//
// The dry-run job must be created with DryRun=true and read via query.Run only —
// job.Read errors on a dry-run job, so this cannot reuse RunRawQuery/QueryShape.
func (e *BigQueryScrapper) EstimateQuery(ctx context.Context, sql string) (*scrapper.QueryEstimate, error) {
	query := e.executor.GetBigQueryClient().Query(sql)
	query.DryRun = true
	// A dry run reports scan bytes independent of cache; disabling the cache
	// keeps the estimate stable and avoids a 0-byte "cache hit" result.
	query.DisableQueryCache = true

	job, err := query.Run(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "bigquery dry-run failed")
	}

	status := job.LastStatus()
	if status == nil || status.Statistics == nil {
		return nil, errors.New("bigquery dry-run returned no statistics")
	}

	estimate := &scrapper.QueryEstimate{
		BytesScanned: int64Ptr(status.Statistics.TotalBytesProcessed),
		Exact:        true,
	}

	// TotalBytesProcessedAccuracy is "PRECISE" for the common case and only
	// downgrades to a "LOWER_BOUND"/"UPPER_BOUND" estimate for queries the
	// planner cannot cost exactly (e.g. some BI Engine / wildcard scans). Treat
	// anything other than a precise value as a planner estimate.
	if qs, ok := status.Statistics.Details.(*bigquery.QueryStatistics); ok {
		if qs.TotalBytesProcessedAccuracy != "" && qs.TotalBytesProcessedAccuracy != "PRECISE" {
			estimate.Exact = false
		}
	}

	return estimate, nil
}

func int64Ptr(v int64) *int64 { return &v }
