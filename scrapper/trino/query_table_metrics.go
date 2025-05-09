package trino

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
)

type trinoStatsRow struct {
	ColumnName         sql.NullString  `db:"column_name"`
	RowCount           sql.NullFloat64 `db:"row_count"`
	DataSize           sql.NullFloat64 `db:"data_size"`
	DistinctValueCount sql.NullFloat64 `db:"distinct_values_count"`
	NullsFraction      sql.NullFloat64 `db:"nulls_fraction"`
	LowValue           sql.NullString  `db:"low_value"`
	HighValue          sql.NullString  `db:"high_value"`
}

func (e *TrinoScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	tables, err := e.QueryTables(ctx)
	if err != nil {
		return nil, err
	}
	db := e.executor.GetDb()
	var out []*scrapper.TableMetricsRow

	for _, t := range tables {
		fqTable := fmt.Sprintf("%s.%s.%s", t.Database, t.Schema, t.Table)
		query := fmt.Sprintf("SHOW STATS FOR %s", fqTable)
		rows, err := db.QueryxContext(ctx, query)
		if err != nil {
			return nil, err
		}
		// Close rows as soon as possible
		func() {
			defer rows.Close()
			for rows.Next() {
				var stat trinoStatsRow
				if err := rows.StructScan(&stat); err != nil {
					return
				}
				if !stat.ColumnName.Valid { // NULL column_name row
					var rowCount *int64
					if stat.RowCount.Valid {
						v := int64(stat.RowCount.Float64)
						rowCount = &v
					}
					out = append(out, &scrapper.TableMetricsRow{
						Instance:  t.Instance,
						Database:  t.Database,
						Schema:    t.Schema,
						Table:     t.Table,
						RowCount:  rowCount,
						UpdatedAt: nil,
						SizeBytes: nil,
					})
					break
				}
			}
		}()
	}
	return out, nil
}
