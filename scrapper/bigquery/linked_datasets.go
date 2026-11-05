package bigquery

import (
	"context"
	"strconv"
	"sync"

	dwhexecbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	bqv2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
)

const (
	datasetTypeLinked = "LINKED"
	linkStateUnlinked = "UNLINKED"
)

// bigQueryAPIFor builds the BigQuery REST client a scan reads what the
// cloud.google.com/go/bigquery client leaves out with, acting as the same
// identity as that client.
func bigQueryAPIFor(conf *BigQueryScrapperConf) func(ctx context.Context) (*bqv2.Service, error) {
	return func(ctx context.Context) (*bqv2.Service, error) {
		httpClient, err := dwhexecbigquery.NewHTTPClient(ctx, &conf.BigQueryConf, bqv2.BigqueryScope)
		if err != nil {
			return nil, err
		}
		return bqv2.NewService(ctx, option.WithHTTPClient(httpClient))
	}
}

// linkedDatasets names the source dataset of each linked dataset (an Analytics
// Hub subscription) a scan reads. The cloud.google.com/go/bigquery client does
// not carry linkedDatasetSource, so each dataset is read once more through the
// REST API, asking for that field only.
//
// BigQuery names the source project by its number. The number is turned into
// the project id through BigQuery's own project listing, read once per scan and
// only when a linked dataset names a number. That listing holds every project
// the caller has a role on and needs no permission of its own, so a source
// project the caller has no role on cannot be named, and its linked dataset
// names no source.
//
// Nothing here fails the scan: a read that fails is logged, and the linked
// dataset then names no source.
type linkedDatasets struct {
	newAPI       func(ctx context.Context) (*bqv2.Service, error)
	rateLimitCfg RateLimitConfig

	apiOnce sync.Once
	api     *bqv2.Service
	apiErr  error

	projectsOnce sync.Once
	projectIds   map[string]string
	projectsErr  error
}

func newLinkedDatasets(newAPI func(ctx context.Context) (*bqv2.Service, error), rateLimitCfg RateLimitConfig) *linkedDatasets {
	return &linkedDatasets{newAPI: newAPI, rateLimitCfg: rateLimitCfg}
}

// source returns the source dataset of a linked dataset, its project named by
// id, or nil when the dataset is not linked, is no longer linked, or its source
// cannot be named.
func (l *linkedDatasets) source(ctx context.Context, project, dataset string) *bqv2.DatasetReference {
	log := logging.GetLogger(ctx).WithField("executor", "bigquery").WithField("project_id", project).WithField("dataset", dataset)

	api, err := l.client(ctx)
	if err != nil {
		return nil
	}
	ds, err := withRateLimitRetry(ctx, l.rateLimitCfg, func(ctx context.Context) (*bqv2.Dataset, error) {
		return api.Datasets.Get(project, dataset).Fields("type", "linkedDatasetSource", "linkedDatasetMetadata").Context(ctx).Do()
	})
	if err != nil {
		log.WithError(err).Warn("cannot read whether the dataset is linked, its tables name no source")
		return nil
	}
	if ds.Type != datasetTypeLinked || ds.LinkedDatasetSource == nil || ds.LinkedDatasetSource.SourceDataset == nil {
		return nil
	}
	if ds.LinkedDatasetMetadata != nil && ds.LinkedDatasetMetadata.LinkState == linkStateUnlinked {
		return nil
	}
	src := ds.LinkedDatasetSource.SourceDataset
	if src.ProjectId == "" || src.DatasetId == "" {
		return nil
	}
	if !isProjectNumber(src.ProjectId) {
		return &bqv2.DatasetReference{ProjectId: src.ProjectId, DatasetId: src.DatasetId}
	}
	projectId, ok := l.projectId(ctx, src.ProjectId)
	if !ok {
		log.WithField("source_project_number", src.ProjectId).
			Info("linked dataset source project is not one the caller has a role on, its tables name no source")
		return nil
	}
	return &bqv2.DatasetReference{ProjectId: projectId, DatasetId: src.DatasetId}
}

func (l *linkedDatasets) client(ctx context.Context) (*bqv2.Service, error) {
	l.apiOnce.Do(func() {
		l.api, l.apiErr = l.newAPI(ctx)
		if l.apiErr != nil {
			logging.GetLogger(ctx).WithField("executor", "bigquery").WithError(l.apiErr).
				Warn("cannot create the BigQuery REST client, linked datasets name no source")
		}
	})
	return l.api, l.apiErr
}

// projectId turns a project number into the project's id, through the
// projects the caller has a role on.
func (l *linkedDatasets) projectId(ctx context.Context, number string) (string, bool) {
	l.projectsOnce.Do(func() {
		l.projectIds, l.projectsErr = l.readProjectIds(ctx)
		if l.projectsErr != nil {
			logging.GetLogger(ctx).WithField("executor", "bigquery").WithError(l.projectsErr).
				Warn("cannot list projects, linked datasets naming a project number name no source")
		}
	})
	id, ok := l.projectIds[number]
	return id, ok
}

func (l *linkedDatasets) readProjectIds(ctx context.Context) (map[string]string, error) {
	var ids map[string]string
	_, err := withRateLimitRetry(ctx, l.rateLimitCfg, func(ctx context.Context) (struct{}, error) {
		ids = map[string]string{}
		return struct{}{}, l.api.Projects.List().Context(ctx).Pages(ctx, func(page *bqv2.ProjectList) error {
			for _, p := range page.Projects {
				if p.NumericId == 0 || p.ProjectReference == nil || p.ProjectReference.ProjectId == "" {
					continue
				}
				ids[strconv.FormatUint(p.NumericId, 10)] = p.ProjectReference.ProjectId
			}
			return nil
		})
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list projects")
	}
	return ids, nil
}

func isProjectNumber(project string) bool {
	_, err := strconv.ParseUint(project, 10, 64)
	return err == nil
}

// linkedFrom names the source table of a table in a linked dataset: the table
// of the same name in the source dataset.
func linkedFrom(source *bqv2.DatasetReference, table string) *scrapper.TableLinkedFrom {
	if source == nil {
		return nil
	}
	return &scrapper.TableLinkedFrom{Database: source.ProjectId, Schema: source.DatasetId, Table: table}
}
