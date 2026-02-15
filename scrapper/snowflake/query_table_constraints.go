package snowflake

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type showPrimaryKeyRow struct {
	CreatedOn       string `db:"created_on"`
	DatabaseName    string `db:"database_name"`
	SchemaName      string `db:"schema_name"`
	TableName       string `db:"table_name"`
	ColumnName      string `db:"column_name"`
	KeySequence     int32  `db:"key_sequence"`
	ConstraintName  string `db:"constraint_name"`
	Comment         string `db:"comment"`
	SchemaEvolution string `db:"schema_evolution_record"`
}

type clusteringKeyRow struct {
	TableSchema string `db:"table_schema"`
	TableName   string `db:"table_name"`
	ClusterBy   string `db:"cluster_by"`
}

func (e *SnowflakeScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	var finalResults []*scrapper.TableConstraintRow
	var m sync.Mutex

	allDatabases, err := e.GetExistingDbs(ctx)
	if err != nil {
		return nil, err
	}

	existingDbs := map[string]bool{}
	for _, database := range allDatabases {
		existingDbs[database.Name] = true
	}

	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(4)

	for _, database := range e.conf.Databases {
		if !existingDbs[database] {
			continue
		}

		select {
		case <-groupCtx.Done():
			return nil, groupCtx.Err()
		default:
		}

		g.Go(func() error {
			var results []*scrapper.TableConstraintRow

			// Query primary keys for the database
			pkRows, err := e.queryPrimaryKeysInDatabase(groupCtx, database)
			if err != nil {
				if isSharedDatabaseUnavailableError(err) {
					logging.GetLogger(groupCtx).WithField("database", database).WithError(err).
						Warn("Shared database is no longer available, skipping")
					return nil
				}
				return errors.Wrapf(err, "failed to query primary keys for database %s", database)
			}
			results = append(results, pkRows...)

			// Query clustering keys for the database
			ckRows, err := e.queryClusteringKeysInDatabase(groupCtx, database)
			if err != nil {
				if isSharedDatabaseUnavailableError(err) {
					logging.GetLogger(groupCtx).WithField("database", database).WithError(err).
						Warn("Shared database is no longer available, skipping")
					return nil
				}
				return errors.Wrapf(err, "failed to query clustering keys for database %s", database)
			}
			results = append(results, ckRows...)

			m.Lock()
			defer m.Unlock()
			finalResults = append(finalResults, results...)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return finalResults, nil
}

func (e *SnowflakeScrapper) queryPrimaryKeysInDatabase(ctx context.Context, database string) ([]*scrapper.TableConstraintRow, error) {
	var results []*scrapper.TableConstraintRow

	rows, err := e.executor.GetDb().QueryxContext(ctx, fmt.Sprintf("SHOW PRIMARY KEYS IN DATABASE %s", database))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		row := &showPrimaryKeyRow{}
		if err := rows.StructScan(row); err != nil {
			return nil, err
		}
		results = append(results, &scrapper.TableConstraintRow{
			Instance:       e.conf.Account,
			Database:       row.DatabaseName,
			Schema:         row.SchemaName,
			Table:          row.TableName,
			ConstraintName: row.ConstraintName,
			ColumnName:     row.ColumnName,
			ConstraintType: scrapper.ConstraintTypePrimaryKey,
			ColumnPosition: row.KeySequence,
		})
	}

	return results, nil
}

func (e *SnowflakeScrapper) queryClusteringKeysInDatabase(ctx context.Context, database string) ([]*scrapper.TableConstraintRow, error) {
	var results []*scrapper.TableConstraintRow

	query := fmt.Sprintf(
		`SELECT table_schema AS "table_schema", table_name AS "table_name", cluster_by AS "cluster_by" FROM %s.information_schema.tables WHERE cluster_by IS NOT NULL AND cluster_by != ''`,
		database,
	)

	rows, err := e.executor.GetDb().QueryxContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		row := &clusteringKeyRow{}
		if err := rows.StructScan(row); err != nil {
			return nil, err
		}
		if row.ClusterBy == "" {
			continue
		}
		// Parse clustering key expression like "LINEAR(col1, col2)" or "col1, col2"
		clusterExpr := row.ClusterBy
		clusterExpr = strings.TrimPrefix(clusterExpr, "LINEAR(")
		clusterExpr = strings.TrimSuffix(clusterExpr, ")")
		columns := strings.Split(clusterExpr, ",")
		for i, col := range columns {
			col = strings.TrimSpace(col)
			if col == "" {
				continue
			}
			results = append(results, &scrapper.TableConstraintRow{
				Instance:       e.conf.Account,
				Database:       database,
				Schema:         row.TableSchema,
				Table:          row.TableName,
				ConstraintName: "clustering_key",
				ColumnName:     col,
				ConstraintType: scrapper.ConstraintTypeClusterBy,
				ColumnPosition: int32(i + 1),
			})
		}
	}

	return results, nil
}
