package redshift

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed table_change_history.sql
var tableChangeHistorySQL string

var _ scrapper.TableChangeHistoryProvider = &RedshiftScrapper{}

type redshiftChangeRow struct {
	Timestamp time.Time `db:"timestamp"`
	Version   int64     `db:"version"`
	Operation string    `db:"operation"`
}

// FetchTableChangeHistory returns DML events for the given table from SYS_QUERY_HISTORY,
// correlated with SYS_QUERY_DETAIL to filter by schema and table name.
//
// Row counts are not available from Redshift system tables and will be nil.
// The Version field is the query_id, which can be used for correlation but not for
// Redshift time-travel (which uses AS OF TIMESTAMP, not a version number).
//
// Note: SYS_QUERY_HISTORY has a default retention of ~5 days on Serverless
// and ~7 days on provisioned clusters.
func (e *RedshiftScrapper) FetchTableChangeHistory(
	ctx context.Context,
	fqn scrapper.DwhFqn,
	from, to time.Time,
	limit int,
) ([]*scrapper.TableChangeEvent, error) {
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()

	sql := querycontext.AppendSQLComment(ctx, tableChangeHistorySQL)
	rows, err := e.executor.GetDb().QueryxContext(
		ctx,
		sql,
		from.UTC(),
		to.UTC(),
		fqn.SchemaName,
		fqn.ObjectName,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*scrapper.TableChangeEvent
	for rows.Next() {
		row := &redshiftChangeRow{}
		if err := rows.StructScan(row); err != nil {
			return nil, err
		}
		events = append(events, &scrapper.TableChangeEvent{
			Timestamp: row.Timestamp,
			Version:   fmt.Sprintf("%d", row.Version),
			Operation: normalizeRedshiftOperation(row.Operation),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	collector.SetRowsProduced(int64(len(events)))
	return events, nil
}

func normalizeRedshiftOperation(op string) string {
	switch op {
	case "INSERT":
		return "INSERT"
	case "UPDATE":
		return "UPDATE"
	case "DELETE":
		return "DELETE"
	case "COPY":
		return "COPY"
	case "MERGE":
		return "MERGE"
	case "TRUNCATE":
		return "TRUNCATE"
	default:
		return "OTHER"
	}
}
