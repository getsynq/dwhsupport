package bigquery

import (
	"context"
	"fmt"
	"time"

	bq "cloud.google.com/go/bigquery"
	execbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
	"google.golang.org/api/iterator"
)

type bigQueryTableChangeRow struct {
	Timestamp        time.Time     `bigquery:"timestamp"`
	Version          bq.NullString `bigquery:"version"`
	Operation        bq.NullString `bigquery:"operation"`
	InsertedRowCount bq.NullInt64  `bigquery:"inserted_row_count"`
	UpdatedRowCount  bq.NullInt64  `bigquery:"updated_row_count"`
	DeletedRowCount  bq.NullInt64  `bigquery:"deleted_row_count"`
}

func (e *BigQueryScrapper) FetchTableChangeHistory(
	ctx context.Context,
	fqn scrapper.DwhFqn,
	from, to time.Time,
	limit int,
) ([]*scrapper.TableChangeEvent, error) {
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()

	sql := e.buildTableChangeHistorySQL(fqn, from, to, limit)

	query := e.executor.GetBigQueryClient().Query(sql)
	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}
	iter, err := job.Read(ctx)
	if err != nil {
		return nil, err
	}
	execbigquery.CollectBigQueryStats(ctx, job)

	var events []*scrapper.TableChangeEvent
	for {
		var row bigQueryTableChangeRow
		err := iter.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			collector.SetRowsProduced(int64(len(events)))
			return nil, err
		}

		event := &scrapper.TableChangeEvent{
			Timestamp: row.Timestamp,
			Operation: normalizeBigQueryOperation(row.Operation.StringVal),
		}

		if row.Version.Valid {
			event.Version = row.Version.StringVal
		}

		if row.InsertedRowCount.Valid {
			n := row.InsertedRowCount.Int64
			event.RowsInserted = &n
		}

		if row.UpdatedRowCount.Valid {
			n := row.UpdatedRowCount.Int64
			event.RowsUpdated = &n
		}

		if row.DeletedRowCount.Valid {
			n := row.DeletedRowCount.Int64
			event.RowsDeleted = &n
		}

		events = append(events, event)
	}

	collector.SetRowsProduced(int64(len(events)))
	return events, nil
}

func (e *BigQueryScrapper) buildTableChangeHistorySQL(fqn scrapper.DwhFqn, from, to time.Time, limit int) string {
	region := e.conf.Region
	if region == "" {
		region = "us"
	}
	return fmt.Sprintf(`SELECT
    end_time AS timestamp,
    job_id AS version,
    statement_type AS operation,
    dml_statistics.inserted_row_count AS inserted_row_count,
    dml_statistics.updated_row_count AS updated_row_count,
    dml_statistics.deleted_row_count AS deleted_row_count
FROM `+"`%s`.`region-%s`.INFORMATION_SCHEMA.JOBS"+`
WHERE destination_table.project_id = %s
  AND destination_table.dataset_id = %s
  AND destination_table.table_id = %s
  AND statement_type IN ('INSERT', 'UPDATE', 'DELETE', 'MERGE', 'TRUNCATE_TABLE')
  AND state = 'DONE'
  AND error_result IS NULL
  AND creation_time BETWEEN '%s' AND '%s'
ORDER BY end_time DESC
LIMIT %d`,
		e.conf.ProjectId,
		region,
		sqldialect.StandardSQLStringLiteral(fqn.DatabaseName),
		sqldialect.StandardSQLStringLiteral(fqn.SchemaName),
		sqldialect.StandardSQLStringLiteral(fqn.ObjectName),
		from.UTC().Format("2006-01-02T15:04:05"),
		to.UTC().Format("2006-01-02T15:04:05"),
		limit,
	)
}

func normalizeBigQueryOperation(op string) string {
	switch op {
	case "INSERT":
		return "INSERT"
	case "UPDATE":
		return "UPDATE"
	case "DELETE":
		return "DELETE"
	case "MERGE":
		return "MERGE"
	case "TRUNCATE_TABLE":
		return "TRUNCATE"
	default:
		return "OTHER"
	}
}
