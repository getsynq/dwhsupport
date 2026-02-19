package databricks

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
)

type databricksHistoryRow struct {
	Version         int64     `db:"version"`
	Timestamp       time.Time `db:"timestamp"`
	Operation       string    `db:"operation"`
	NumOutputRows   *string   `db:"num_output_rows"`
	NumRowsInserted *string   `db:"num_rows_inserted"`
	NumRowsUpdated  *string   `db:"num_rows_updated"`
	NumRowsDeleted  *string   `db:"num_rows_deleted"`
}

func (e *DatabricksScrapper) FetchTableChangeHistory(
	ctx context.Context,
	fqn scrapper.DwhFqn,
	from, to time.Time,
	limit int,
) ([]*scrapper.TableChangeEvent, error) {
	executor, err := e.lazyExecutor.Get()
	if err != nil {
		return nil, err
	}

	tableFqn := fmt.Sprintf("`%s`.`%s`.`%s`", fqn.DatabaseName, fqn.SchemaName, fqn.ObjectName)

	sql := fmt.Sprintf(`SELECT
    version,
    timestamp,
    operation,
    operationMetrics['numOutputRows'] as num_output_rows,
    operationMetrics['numTargetRowsInserted'] as num_rows_inserted,
    operationMetrics['numTargetRowsUpdated'] as num_rows_updated,
    operationMetrics['numTargetRowsDeleted'] as num_rows_deleted
FROM (DESCRIBE HISTORY %s)
WHERE operation IN ('WRITE', 'MERGE', 'UPDATE', 'DELETE', 'STREAMING UPDATE', 'COPY INTO')
  AND timestamp BETWEEN '%s' AND '%s'
ORDER BY version DESC
LIMIT %d`,
		tableFqn,
		from.UTC().Format("2006-01-02T15:04:05"),
		to.UTC().Format("2006-01-02T15:04:05"),
		limit,
	)

	rows, err := executor.GetDb().QueryxContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*scrapper.TableChangeEvent
	for rows.Next() {
		row := &databricksHistoryRow{}
		if err := rows.StructScan(row); err != nil {
			return nil, err
		}

		event := &scrapper.TableChangeEvent{
			Timestamp: row.Timestamp,
			Version:   strconv.FormatInt(row.Version, 10),
			Operation: normalizeDatabricksOperation(row.Operation),
		}

		if row.NumRowsInserted != nil {
			if n, err := strconv.ParseInt(*row.NumRowsInserted, 10, 64); err == nil {
				event.RowsInserted = &n
			}
		} else if row.NumOutputRows != nil {
			// For WRITE operations, numOutputRows represents rows written
			if n, err := strconv.ParseInt(*row.NumOutputRows, 10, 64); err == nil {
				event.RowsInserted = &n
			}
		}

		if row.NumRowsUpdated != nil {
			if n, err := strconv.ParseInt(*row.NumRowsUpdated, 10, 64); err == nil {
				event.RowsUpdated = &n
			}
		}

		if row.NumRowsDeleted != nil {
			if n, err := strconv.ParseInt(*row.NumRowsDeleted, 10, 64); err == nil {
				event.RowsDeleted = &n
			}
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

func normalizeDatabricksOperation(op string) string {
	switch op {
	case "WRITE", "STREAMING UPDATE":
		return "INSERT"
	case "COPY INTO":
		return "COPY"
	case "MERGE":
		return "MERGE"
	case "UPDATE":
		return "UPDATE"
	case "DELETE":
		return "DELETE"
	default:
		return "OTHER"
	}
}
