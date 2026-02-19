package snowflake

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
)

type snowflakeTableDMLHistoryRow struct {
	Timestamp   time.Time `db:"TIMESTAMP"`
	RowsAdded   *int64    `db:"ROWS_ADDED"`
	RowsRemoved *int64    `db:"ROWS_REMOVED"`
	RowsUpdated *int64    `db:"ROWS_UPDATED"`
}

func (e *SnowflakeScrapper) FetchTableChangeHistory(
	ctx context.Context,
	fqn scrapper.DwhFqn,
	from, to time.Time,
	limit int,
) ([]*scrapper.TableChangeEvent, error) {
	accountUsageDb := "SNOWFLAKE"
	if e.conf.AccountUsageDb != nil && len(*e.conf.AccountUsageDb) > 0 {
		accountUsageDb = *e.conf.AccountUsageDb
	}

	sql := fmt.Sprintf(`SELECT
    end_time AS "TIMESTAMP",
    rows_added AS "ROWS_ADDED",
    rows_removed AS "ROWS_REMOVED",
    rows_updated AS "ROWS_UPDATED"
FROM %s.ACCOUNT_USAGE.TABLE_DML_HISTORY
WHERE UPPER(database_name) = UPPER('%s')
  AND UPPER(schema_name) = UPPER('%s')
  AND UPPER(table_name) = UPPER('%s')
  AND (rows_added > 0 OR rows_removed > 0 OR rows_updated > 0)
  AND start_time >= '%s'
  AND end_time <= '%s'
ORDER BY end_time DESC
LIMIT %d`,
		accountUsageDb,
		strings.ReplaceAll(fqn.DatabaseName, "'", "''"),
		strings.ReplaceAll(fqn.SchemaName, "'", "''"),
		strings.ReplaceAll(fqn.ObjectName, "'", "''"),
		from.UTC().Format("2006-01-02T15:04:05"),
		to.UTC().Format("2006-01-02T15:04:05"),
		limit,
	)

	rows, err := e.executor.GetDb().QueryxContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*scrapper.TableChangeEvent
	for rows.Next() {
		row := &snowflakeTableDMLHistoryRow{}
		if err := rows.StructScan(row); err != nil {
			return nil, err
		}

		event := &scrapper.TableChangeEvent{
			Timestamp: row.Timestamp,
			Version:   "",
			Operation: "OTHER",
		}

		if row.RowsAdded != nil {
			event.RowsInserted = row.RowsAdded
		}
		if row.RowsUpdated != nil {
			event.RowsUpdated = row.RowsUpdated
		}
		if row.RowsRemoved != nil {
			event.RowsDeleted = row.RowsRemoved
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}
