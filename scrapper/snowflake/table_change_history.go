package snowflake

import (
	"context"
	"fmt"
	"time"

	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
)

func (e *SnowflakeScrapper) FetchTableChangeHistory(
	ctx context.Context,
	fqn scrapper.DwhFqn,
	from, to time.Time,
	limit int,
) ([]*scrapper.TableChangeEvent, error) {
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()

	var sql string
	if e.conf.UseAccessHistoryForTableChanges {
		sql = e.buildAccessHistorySQL(fqn, from, to, limit)
	} else {
		sql = e.buildDMLHistorySQL(fqn, from, to, limit)
	}
	events, err := e.queryTableChangeEvents(ctx, sql)
	collector.SetRowsProduced(int64(len(events)))
	return events, err
}

// buildDMLHistorySQL builds the query for ACCOUNT_USAGE.TABLE_DML_HISTORY.
// Available on all Snowflake editions. Latency: up to ~6h.
// Only returns buckets where rows actually changed (rows_added/removed/updated > 0).
func (e *SnowflakeScrapper) buildDMLHistorySQL(fqn scrapper.DwhFqn, from, to time.Time, limit int) string {
	return fmt.Sprintf(`SELECT
    end_time AS "TIMESTAMP"
FROM %s.ACCOUNT_USAGE.TABLE_DML_HISTORY
WHERE UPPER(database_name) = UPPER(%s)
  AND UPPER(schema_name) = UPPER(%s)
  AND UPPER(table_name) = UPPER(%s)
  AND (rows_added > 0 OR rows_removed > 0 OR rows_updated > 0)
  AND start_time >= '%s'
  AND end_time <= '%s'
ORDER BY end_time DESC
LIMIT %d`,
		e.accountUsageDb(),
		sqldialect.StandardSQLStringLiteral(fqn.DatabaseName),
		sqldialect.StandardSQLStringLiteral(fqn.SchemaName),
		sqldialect.StandardSQLStringLiteral(fqn.ObjectName),
		from.UTC().Format("2006-01-02T15:04:05"),
		to.UTC().Format("2006-01-02T15:04:05"),
		limit,
	)
}

// buildAccessHistorySQL builds the query for ACCOUNT_USAGE.ACCESS_HISTORY.
// Requires Snowflake Enterprise edition. Latency: up to ~3h.
// Returns all DML events regardless of whether rows changed.
func (e *SnowflakeScrapper) buildAccessHistorySQL(fqn scrapper.DwhFqn, from, to time.Time, limit int) string {
	// objectName in ACCESS_HISTORY is stored as DATABASE.SCHEMA.TABLE
	objectName := fqn.DatabaseName + "." + fqn.SchemaName + "." + fqn.ObjectName
	return fmt.Sprintf(`SELECT
    h.query_start_time AS "TIMESTAMP"
FROM %s.ACCOUNT_USAGE.ACCESS_HISTORY h,
    LATERAL FLATTEN(INPUT => h.objects_modified) obj
WHERE obj.value:objectDomain::string = 'Table'
  AND UPPER(obj.value:objectName::string) = UPPER(%s)
  AND h.query_start_time >= '%s'
  AND h.query_start_time <= '%s'
ORDER BY h.query_start_time DESC
LIMIT %d`,
		e.accountUsageDb(),
		sqldialect.StandardSQLStringLiteral(objectName),
		from.UTC().Format("2006-01-02T15:04:05"),
		to.UTC().Format("2006-01-02T15:04:05"),
		limit,
	)
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
