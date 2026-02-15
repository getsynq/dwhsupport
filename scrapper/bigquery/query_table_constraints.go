package bigquery

import (
	"context"
	"fmt"
	"sync"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

func (e *BigQueryScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	log := logging.
		GetLogger(ctx).
		WithField("executor", "bigquery").
		WithField("project_id", e.conf.ProjectId)

	datasets := e.executor.GetBigQueryClient().Datasets(ctx)
	datasets.ListHidden = true

	var results []*scrapper.TableConstraintRow
	var mutex sync.Mutex

	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(50)

	for {
		dataset, err := datasets.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			if errIsNotFound(err) || errIsAccessDenied(err) {
				continue
			}
			return nil, err
		}

		if isPrivateDataset(dataset.DatasetID) {
			continue
		}

		if e.blocklist.IsBlocked(dataset.DatasetID) {
			log.Infof("dataset %s blacklisted by config", dataset.DatasetID)
			continue
		}

		tables := e.executor.GetBigQueryClient().Dataset(dataset.DatasetID).Tables(groupCtx)
		for {
			table, err := tables.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				if errIsNotFound(err) || errIsAccessDenied(err) {
					continue
				}
				return nil, err
			}

			datasetID := dataset.DatasetID
			tableID := table.TableID

			select {
			case <-groupCtx.Done():
				return nil, groupCtx.Err()
			default:
			}

			g.Go(func() error {
				tableMeta, err := e.executor.GetBigQueryClient().Dataset(datasetID).Table(tableID).Metadata(groupCtx)
				if err != nil {
					if errIsNotFound(err) || errIsAccessDenied(err) {
						return nil
					}
					return err
				}

				var rows []*scrapper.TableConstraintRow

				if tableMeta.TimePartitioning != nil {
					if tableMeta.TimePartitioning.Field != "" {
						rows = append(rows, &scrapper.TableConstraintRow{
							Database:       e.conf.ProjectId,
							Schema:         datasetID,
							Table:          tableID,
							ConstraintName: fmt.Sprintf("time_partitioning_%s", tableMeta.TimePartitioning.Type),
							ColumnName:     tableMeta.TimePartitioning.Field,
							ConstraintType: scrapper.ConstraintTypePartitionBy,
							ColumnPosition: 1,
						})
					}
				}

				if tableMeta.RangePartitioning != nil {
					rows = append(rows, &scrapper.TableConstraintRow{
						Database:       e.conf.ProjectId,
						Schema:         datasetID,
						Table:          tableID,
						ConstraintName: "range_partitioning",
						ColumnName:     tableMeta.RangePartitioning.Field,
						ConstraintType: scrapper.ConstraintTypePartitionBy,
						ColumnPosition: 1,
					})
				}

				if tableMeta.Clustering != nil {
					for i, col := range tableMeta.Clustering.Fields {
						rows = append(rows, &scrapper.TableConstraintRow{
							Database:       e.conf.ProjectId,
							Schema:         datasetID,
							Table:          tableID,
							ConstraintName: "clustering",
							ColumnName:     col,
							ConstraintType: scrapper.ConstraintTypeClusterBy,
							ColumnPosition: int32(i + 1),
						})
					}
				}

				if len(rows) > 0 {
					mutex.Lock()
					defer mutex.Unlock()
					results = append(results, rows...)
				}

				return nil
			})
		}
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return results, nil
}
