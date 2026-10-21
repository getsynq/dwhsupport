package bigquery

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
)

// datasetReads collects what a fan-out over a project's datasets and their
// tables could not read, so one dataset or table BigQuery refuses costs only
// itself.
//
// A caller that asked with scrapper.WithIncompleteResults gets the rows of
// everything else and an *IncompleteError naming each dataset whose metadata or
// table listing failed, and each table whose metadata failed, as omitted for
// part. A table that answers not found is not an omission: it was dropped
// between the listing and the read, and leaving it out is right.
//
// A caller that did not ask keeps the behaviour it had: a failed dataset or
// table fails the call, except a table that answers not found or access denied,
// which is skipped. Either way, a call that read nothing returns the first
// failure, since there is nothing partial about reading nothing.
type datasetReads struct {
	e      *BigQueryScrapper
	part   scrapper.OmittedPart
	accept bool

	mu        sync.Mutex
	omissions []scrapper.Omission
	read      bool
}

func (e *BigQueryScrapper) newDatasetReads(ctx context.Context, part scrapper.OmittedPart) *datasetReads {
	return &datasetReads{e: e, part: part, accept: scrapper.IncompleteResultsAccepted(ctx)}
}

// datasetFailed handles a dataset whose metadata or table listing failed. It
// returns the error that fails the call, or nil when the dataset is left out.
func (r *datasetReads) datasetFailed(ctx context.Context, op, dataset string, err error) error {
	if !r.accept || ctx.Err() != nil {
		return r.e.scrapeError(ctx, op, dataset, "", err)
	}
	r.omit(ctx, op, dataset, "", "", err)
	return nil
}

// tableFailed handles a table whose metadata failed. table is the name the
// result reports it under, which for a sharded table is not its id. It returns
// the error that fails the call, or nil when the table is left out.
func (r *datasetReads) tableFailed(ctx context.Context, dataset, tableId, table string, err error) error {
	if errIsNotFound(err) {
		return nil
	}
	if !r.accept {
		if errIsAccessDenied(err) {
			return nil
		}
		return r.e.scrapeError(ctx, "table.metadata", dataset, tableId, err)
	}
	if ctx.Err() != nil {
		return r.e.scrapeError(ctx, "table.metadata", dataset, tableId, err)
	}
	r.omit(ctx, "table.metadata", dataset, tableId, table, err)
	return nil
}

// degraded handles a table read through a fallback that leaves part of it
// out. table is the name the result reports it under. A caller that accepts
// incomplete results is told the table misses part, so it does not read what
// is missing as removed; any other caller keeps the rows, and the fallback is
// only logged.
func (r *datasetReads) degraded(ctx context.Context, op, dataset, tableId, table string, err error) {
	if !r.accept {
		logging.GetLogger(ctx).
			WithField("executor", "bigquery").
			WithField("bq_op", op).
			WithField("bq_project", r.e.conf.ProjectId).
			WithField("bq_dataset", dataset).
			WithField("bq_table", tableId).
			WithError(err).
			Warn("BigQuery metadata read through a fallback that leaves part of it out")
		return
	}
	r.omit(ctx, op, dataset, tableId, table, err)
}

// markRead records that something was read: a table's metadata, or a dataset
// listed with no table to read.
func (r *datasetReads) markRead() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.read = true
}

func (r *datasetReads) omit(ctx context.Context, op, dataset, tableId, table string, err error) {
	wrapped := errors.Wrapf(err, "bigquery %s failed (project=%s dataset=%s table=%s)", op, r.e.conf.ProjectId, dataset, tableId)
	logging.GetLogger(ctx).
		WithField("executor", "bigquery").
		WithField("bq_op", op).
		WithField("bq_project", r.e.conf.ProjectId).
		WithField("bq_dataset", dataset).
		WithField("bq_table", tableId).
		WithError(err).
		Warn("BigQuery metadata could not be read, leaving it out")

	r.mu.Lock()
	defer r.mu.Unlock()
	r.omissions = append(r.omissions, scrapper.Omission{
		Database: r.e.conf.ProjectId,
		Schema:   dataset,
		Table:    table,
		Part:     r.part,
		Err:      wrapped,
	})
}

// result is the error the call returns beside its rows, once every read has
// finished: nil, an *IncompleteError, or the first failure when nothing was read.
func (r *datasetReads) result() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	sort.SliceStable(r.omissions, func(i, j int) bool {
		return strings.Compare(r.omissions[i].Scope(), r.omissions[j].Scope()) < 0
	})
	if len(r.omissions) > 0 && !r.read {
		return r.omissions[0].Err
	}
	return scrapper.Incomplete(r.omissions)
}

// metricsTableName is the name collapseShardedMetrics reports a table's metrics
// under: a shard's metrics are summed into its sharded table.
func metricsTableName(tableId string) string {
	lower, upper := getValidShardDateRange()
	if name, sharded := shardedTableName(tableId, lower, upper); sharded {
		return strings.TrimRight(name, "_")
	}
	return tableId
}
