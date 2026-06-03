package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
)

// QuerySchemas lists BigQuery datasets as schemas. BigQuery has no instance
// concept: the GCP project is the database and a dataset is a schema, so
// SchemaRow.Instance stays empty, Database is the project and Schema is the
// dataset — consistent with QueryTables.
func (e *BigQueryScrapper) QuerySchemas(ctx context.Context) ([]*scrapper.SchemaRow, error) {
	log := logging.
		GetLogger(ctx).
		WithField("executor", "bigquery").
		WithField("project_id", e.conf.ProjectId)

	allDatasets, err := e.listDatasets(ctx)
	if err != nil {
		return nil, err
	}

	var rows []*scrapper.SchemaRow
	for _, dataset := range allDatasets {
		if isPrivateDataset(dataset.DatasetID) {
			continue
		}
		if !e.scope.IsSchemaAccepted(e.conf.ProjectId, dataset.DatasetID) {
			log.Infof("dataset %s excluded by scope filter", dataset.DatasetID)
			continue
		}

		row := &scrapper.SchemaRow{
			Database: e.conf.ProjectId,
			Schema:   dataset.DatasetID,
		}

		meta, err := withRateLimitRetry(ctx, e.rateLimitCfg, func(ctx context.Context) (*bigquery.DatasetMetadata, error) {
			return e.executor.GetBigQueryClient().Dataset(dataset.DatasetID).Metadata(ctx)
		})
		if err != nil {
			if errIsNotFound(err) || errIsAccessDenied(err) {
				rows = append(rows, row)
				continue
			}
			return nil, e.scrapeError(ctx, "dataset.metadata", dataset.DatasetID, "", err)
		}
		row.Description = lo.EmptyableToPtr(meta.Description)

		rows = append(rows, row)
	}

	return rows, nil
}
