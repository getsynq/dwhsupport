package snowflake

import (
	"context"
	"fmt"
	"strings"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
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

func (e *SnowflakeScrapper) QueryTableConstraints(
	ctx context.Context,
	database string,
	schema string,
	table string,
) ([]*scrapper.TableConstraintRow, error) {
	var results []*scrapper.TableConstraintRow

	// Query primary keys
	pkRows, err := e.queryPrimaryKeys(ctx, database, schema, table)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query primary keys")
	}
	results = append(results, pkRows...)

	// Query clustering keys
	ckRows, err := e.queryClusteringKeys(ctx, database, schema, table)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query clustering keys")
	}
	results = append(results, ckRows...)

	return results, nil
}

func (e *SnowflakeScrapper) queryPrimaryKeys(
	ctx context.Context,
	database string,
	schema string,
	table string,
) ([]*scrapper.TableConstraintRow, error) {
	var results []*scrapper.TableConstraintRow

	rows, err := e.executor.GetDb().QueryxContext(ctx, fmt.Sprintf("SHOW PRIMARY KEYS IN %s.%s.%s", database, schema, table))
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

type clusteringKeyRow struct {
	ClusterBy string `db:"cluster_by"`
}

func (e *SnowflakeScrapper) queryClusteringKeys(
	ctx context.Context,
	database string,
	schema string,
	table string,
) ([]*scrapper.TableConstraintRow, error) {
	var results []*scrapper.TableConstraintRow

	query := fmt.Sprintf(
		`SELECT cluster_by AS "cluster_by" FROM %s.information_schema.tables WHERE table_schema = '%s' AND table_name = '%s' AND cluster_by IS NOT NULL AND cluster_by != ''`,
		database,
		strings.ToUpper(schema),
		strings.ToUpper(table),
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
				Schema:         schema,
				Table:          table,
				ConstraintName: "clustering_key",
				ColumnName:     col,
				ConstraintType: scrapper.ConstraintTypeClusterBy,
				ColumnPosition: int32(i + 1),
			})
		}
	}

	return results, nil
}
