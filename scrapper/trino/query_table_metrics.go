package trino

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/jmoiron/sqlx"
	"github.com/trinodb/trino-go-client/trino"
)

type MetricsStrategy func(ctx context.Context, db *sqlx.DB, tableRow *scrapper.TableRow, tableMetricsRow *scrapper.TableMetricsRow) error

func (e *TrinoScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {

	db := e.executor.GetDb()
	var out []*scrapper.TableMetricsRow

	availableCatalogs, err := e.allAvailableCatalogs.Get()
	if err != nil {
		return nil, err
	}

	for _, catalog := range availableCatalogs {
		if !catalog.IsAccepted {
			continue
		}

		tables, err := e.queryTables(ctx, catalog.CatalogName)
		if err != nil {
			return nil, err
		}

		var metricsStrategy MetricsStrategy

		switch catalog.ConnectorName {
		case "iceberg", "galaxy_objectstore":
			logging.GetLogger(ctx).Infof("using iceberg metrics strategy for catalog %s", catalog.CatalogName)
			metricsStrategy = e.icebergMetricsStrategy

		default:
			logging.GetLogger(ctx).Warnf(
				"unknown connector %s for catalog %s, falling back to SHOW STATS",
				catalog.ConnectorName, catalog.CatalogName,
			)
			metricsStrategy = e.showStatsMetricsStrategy
		}

		for _, t := range tables {

			if t.TableType == "VIEW" {
				continue
			}

			tableMetricsRow := &scrapper.TableMetricsRow{
				Instance: t.Instance,
				Database: t.Database,
				Schema:   t.Schema,
				Table:    t.Table,
			}

			if metricsStrategy != nil {
				if err := metricsStrategy(ctx, db, t, tableMetricsRow); err != nil {
					logging.GetLogger(ctx).WithField("table_fqn", t.TableFqn()).WithError(err).Warnf("error getting metrics for table")
					continue
				}
			}

			if tableMetricsRow.SizeBytes == nil && tableMetricsRow.RowCount == nil && tableMetricsRow.UpdatedAt == nil {
				logging.GetLogger(ctx).WithField("table_fqn", t.TableFqn()).WithField("table_type", t.TableType).Warnf("no metrics found")
				continue
			}

			out = append(out, tableMetricsRow)
		}
	}
	return out, nil
}

type trinoShowStatsRow struct {
	ColumnName         sql.NullString  `db:"column_name"`
	RowCount           sql.NullFloat64 `db:"row_count"`
	DataSize           sql.NullFloat64 `db:"data_size"`
	DistinctValueCount sql.NullFloat64 `db:"distinct_values_count"`
	NullsFraction      sql.NullFloat64 `db:"nulls_fraction"`
	LowValue           sql.NullString  `db:"low_value"`
	HighValue          sql.NullString  `db:"high_value"`
}

func (e *TrinoScrapper) showStatsMetricsStrategy(ctx context.Context, db *sqlx.DB, tableRow *scrapper.TableRow, tableMetricsRow *scrapper.TableMetricsRow) error {
	if tableRow.TableType == "VIEW" {
		return nil
	}

	fqTable := e.fqn(tableMetricsRow)
	query := fmt.Sprintf("SHOW STATS FOR %s", fqTable)
	rows, err := db.QueryxContext(ctx, query)
	if err != nil {
		return err
	}

	defer rows.Close()
	dataSize := int64(0)
	dataSizePresent := false

	for rows.Next() {
		var stat trinoShowStatsRow
		if err := rows.StructScan(&stat); err != nil {
			return err
		}
		if !stat.ColumnName.Valid { // NULL column_name row
			var rowCount *int64
			if stat.RowCount.Valid {
				v := int64(stat.RowCount.Float64)
				rowCount = &v
			}
			tableMetricsRow.RowCount = rowCount
		}

		if stat.DataSize.Valid {
			dataSize += int64(stat.DataSize.Float64)
			dataSizePresent = true
		}
	}

	if dataSizePresent {
		tableMetricsRow.SizeBytes = &dataSize
	}
	return nil
}

type trinoIcebergSnapshotsRow struct {
	CommittedAt  time.Time      `db:"committed_at"`
	SnapshotId   string         `db:"snapshot_id"`
	ParentId     sql.NullString `db:"parent_id"`
	Operation    string         `db:"operation"`
	ManifestList string         `db:"manifest_list"`
	Summary      trino.NullMap  `db:"summary"`
}

func (e *TrinoScrapper) icebergMetricsStrategy(ctx context.Context, db *sqlx.DB, tableRow *scrapper.TableRow, tableMetricsRow *scrapper.TableMetricsRow) error {

	// Let's reuse SHOW STATS
	if err := e.showStatsMetricsStrategy(ctx, db, tableRow, tableMetricsRow); err != nil {
		return err
	}

	if tableRow.IsView {
		return nil
	}

	fqTable := e.fqn(tableMetricsRow, "snapshots")
	query := fmt.Sprintf("SELECT * FROM %s", fqTable)
	rows, err := db.QueryxContext(ctx, query)
	if err != nil {
		return err
	}

	defer rows.Close()

	var updatedAt *time.Time

	var latestSnapshot *trinoIcebergSnapshotsRow

	for rows.Next() {
		var stat trinoIcebergSnapshotsRow
		if err := rows.StructScan(&stat); err != nil {
			return err
		}

		if updatedAt == nil || stat.CommittedAt.After(*updatedAt) {
			updatedAt = &stat.CommittedAt
			latestSnapshot = &stat
		}
	}
	tableMetricsRow.UpdatedAt = updatedAt

	if latestSnapshot != nil && latestSnapshot.Summary.Valid {
		logging.GetLogger(ctx).WithField("table_fqn", tableRow.TableFqn()).WithField("summary", latestSnapshot.Summary.Map).Info("iceberg summary")
		if v, ok := getInt(latestSnapshot.Summary.Map, "total-files-size"); ok {
			tableMetricsRow.SizeBytes = &v
		}

		if totalRec, ok := getInt(latestSnapshot.Summary.Map, "total-records"); ok {
			tableMetricsRow.RowCount = &totalRec
			if totalDeletes, ok := getInt(latestSnapshot.Summary.Map, "total-position-deletes"); ok {
				sum := totalRec - totalDeletes
				if sum >= 0 {
					tableMetricsRow.RowCount = &sum
				}
			}
		}
	}

	return nil
}

func getInt(m map[string]interface{}, s string) (int64, bool) {
	if v, ok := m[s]; ok {
		switch t := v.(type) {
		case int64:
			return t, true
		case int32:
			return int64(t), true
		case int:
			return int64(t), true
		case float32:
			return int64(t), true
		case float64:
			return int64(t), true
		case string:
			i, err := strconv.ParseInt(t, 10, 64)
			if err == nil {
				return i, true
			}
			return 0, false
		}
	}

	return 0, false
}
