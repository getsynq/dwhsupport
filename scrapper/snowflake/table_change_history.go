package snowflake

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *SnowflakeScrapper) FetchTableChangeHistory(
	ctx context.Context,
	fqn scrapper.DwhFqn,
	from, to time.Time,
	limit int,
) ([]*scrapper.TableChangeEvent, error) {
	if e.conf.UseAccessHistoryForTableChanges {
		return e.fetchTableChangeHistoryFromAccessHistory(ctx, fqn, from, to, limit)
	}
	return e.fetchTableChangeHistoryFromDMLHistory(ctx, fqn, from, to, limit)
}

// fetchTableChangeHistoryFromDMLHistory uses ACCOUNT_USAGE.TABLE_DML_HISTORY.
// Available on all Snowflake editions. Latency: up to ~6h.
// Only returns buckets where rows actually changed (rows_added/removed/updated > 0).
func (e *SnowflakeScrapper) fetchTableChangeHistoryFromDMLHistory(
	ctx context.Context,
	fqn scrapper.DwhFqn,
	from, to time.Time,
	limit int,
) ([]*scrapper.TableChangeEvent, error) {
	accountUsageDb := e.accountUsageDb()

	sql := fmt.Sprintf(`SELECT
    end_time AS "TIMESTAMP"
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

	return e.queryTableChangeEvents(ctx, sql)
}

// fetchTableChangeHistoryFromAccessHistory uses ACCOUNT_USAGE.ACCESS_HISTORY.
// Requires Snowflake Enterprise edition. Latency: up to ~3h.
// Returns all DML events regardless of whether rows changed.
func (e *SnowflakeScrapper) fetchTableChangeHistoryFromAccessHistory(
	ctx context.Context,
	fqn scrapper.DwhFqn,
	from, to time.Time,
	limit int,
) ([]*scrapper.TableChangeEvent, error) {
	accountUsageDb := e.accountUsageDb()

	// objectName in ACCESS_HISTORY is stored as DATABASE.SCHEMA.TABLE
	objectName := fqn.DatabaseName + "." + fqn.SchemaName + "." + fqn.ObjectName

	sql := fmt.Sprintf(`SELECT
    h.query_start_time AS "TIMESTAMP"
FROM %s.ACCOUNT_USAGE.ACCESS_HISTORY h,
    LATERAL FLATTEN(INPUT => h.objects_modified) obj
WHERE obj.value:objectDomain::string = 'Table'
  AND UPPER(obj.value:objectName::string) = UPPER('%s')
  AND h.query_start_time >= '%s'
  AND h.query_start_time <= '%s'
ORDER BY h.query_start_time DESC
LIMIT %d`,
		accountUsageDb,
		strings.ReplaceAll(objectName, "'", "''"),
		from.UTC().Format("2006-01-02T15:04:05"),
		to.UTC().Format("2006-01-02T15:04:05"),
		limit,
	)

	return e.queryTableChangeEvents(ctx, sql)
}

type snowflakeTableChangeRow struct {
	Timestamp time.Time `db:"TIMESTAMP"`
}

func (e *SnowflakeScrapper) queryTableChangeEvents(ctx context.Context, sql string) ([]*scrapper.TableChangeEvent, error) {
	rows, err := e.executor.GetDb().QueryxContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*scrapper.TableChangeEvent
	for rows.Next() {
		row := &snowflakeTableChangeRow{}
		if err := rows.StructScan(row); err != nil {
			return nil, err
		}
		events = append(events, &scrapper.TableChangeEvent{
			Timestamp: row.Timestamp,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

func (e *SnowflakeScrapper) accountUsageDb() string {
	if e.conf.AccountUsageDb != nil && len(*e.conf.AccountUsageDb) > 0 {
		return *e.conf.AccountUsageDb
	}
	return "SNOWFLAKE"
}
