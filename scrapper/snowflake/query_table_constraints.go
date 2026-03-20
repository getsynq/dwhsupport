package snowflake

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
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

	databasesToQuery, err := e.GetDatabasesToQuery(ctx)
	if err != nil {
		return nil, err
	}

	// Check if CLUSTER_BY column is available using the first database.
	// On Snowflake Standard edition, information_schema.tables does not expose CLUSTER_BY.
	clusterByAvailable := false
	if len(databasesToQuery) > 0 {
		available, err := e.hasClusterByColumn(ctx, databasesToQuery[0])
		if err != nil {
			logging.GetLogger(ctx).WithError(err).Debug("Failed to check CLUSTER_BY column availability, skipping clustering keys")
		} else {
			clusterByAvailable = available
		}
		if !clusterByAvailable {
			logging.GetLogger(ctx).
				Debug("CLUSTER_BY column not available in information_schema.tables (Snowflake Standard edition), skipping clustering keys")
		}
	}

	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(4)

	for _, database := range databasesToQuery {
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

			// Query clustering keys for the database (only if CLUSTER_BY column is available)
			if clusterByAvailable {
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
			}

			m.Lock()
			defer m.Unlock()
			finalResults = append(finalResults, results...)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Post-filter results for SHOW PRIMARY KEYS which doesn't support WHERE clauses.
	return scope.FilterRows(finalResults, scope.GetScope(ctx)), nil
}

func (e *SnowflakeScrapper) queryPrimaryKeysInDatabase(ctx context.Context, database string) ([]*scrapper.TableConstraintRow, error) {
	var results []*scrapper.TableConstraintRow

	rows, err := e.executor.QueryRows(ctx, fmt.Sprintf("SHOW PRIMARY KEYS IN DATABASE %s", database))
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

// hasClusterByColumn checks whether the CLUSTER_BY column exists in information_schema.tables
// by describing the table. This avoids failing queries on Snowflake Standard edition.
func (e *SnowflakeScrapper) hasClusterByColumn(ctx context.Context, database string) (bool, error) {
	tableName := fmt.Sprintf("%s.information_schema.tables", database)
	columns, err := e.findTableColumns(ctx, tableName)
	if err != nil {
		return false, err
	}
	for _, col := range columns {
		if strings.EqualFold(col, "CLUSTER_BY") {
			return true, nil
		}
	}
	return false, nil
}

func (e *SnowflakeScrapper) queryClusteringKeysInDatabase(ctx context.Context, database string) ([]*scrapper.TableConstraintRow, error) {
	var results []*scrapper.TableConstraintRow

	query := scope.AppendScopeConditions(
		ctx,
		fmt.Sprintf(
			`SELECT table_schema AS "table_schema", table_name AS "table_name", cluster_by AS "cluster_by" FROM %s.information_schema.tables WHERE cluster_by IS NOT NULL AND cluster_by != '' /* SYNQ_SCOPE_FILTER */`,
			database,
		),
		"", "table_schema", "table_name",
	)

	rows, err := e.executor.QueryRows(ctx, query)
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
