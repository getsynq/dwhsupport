package fabric

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/getsynq/dwhsupport/sqldialect"
	"google.golang.org/protobuf/types/known/structpb"
)

// FabricQueryLogSchema maps a row of queryinsights.exec_requests_history —
// Fabric Warehouse's query history surface (there is no Query Store). Each row
// is one request execution.
type FabricQueryLogSchema struct {
	Database      string     `db:"database_name"`
	QueryID       string     `db:"query_id"`
	LoginName     *string    `db:"login_name"`
	SubmitTime    *time.Time `db:"submit_time"`
	StartTime     *time.Time `db:"start_time"`
	EndTime       *time.Time `db:"end_time"`
	ElapsedMs     *int64     `db:"total_elapsed_time_ms"`
	RowCount      *int64     `db:"row_count"`
	Status        string     `db:"status"`
	StatementType *string    `db:"statement_type"`
	Command       *string    `db:"command"`
}

var _ querylogs.QueryLogsProvider = &FabricScrapper{}

// FetchQueryLogs streams query history from queryinsights.exec_requests_history.
//
// Fabric has no Query Store; queryinsights is the query-history source (retained
// ~30 days). The view is per-database, so — consistent with the workspace-centric
// scrapper — this reads it across every in-scope database via three-part names in
// a single UNION ALL, feeding one iterator. The [from, to] window is inlined as
// DATETIME2 literals rather than bound parameters so the same window applies to
// every UNION branch.
func (e *FabricScrapper) FetchQueryLogs(
	ctx context.Context,
	from, to time.Time,
	obfuscator querylogs.QueryObfuscator,
) (querylogs.QueryLogIterator, error) {
	if obfuscator == nil {
		return nil, fmt.Errorf("obfuscator is required")
	}

	databases, err := e.GetDatabasesToQuery(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := e.executor.QueryRows(ctx, buildQueryLogsUnion(databases, from, to))
	if err != nil {
		return nil, err
	}

	host := e.conf.Host
	return querylogs.NewSqlxRowsIterator[FabricQueryLogSchema](
		rows,
		obfuscator,
		e.DialectType(),
		func(row *FabricQueryLogSchema, obfuscator querylogs.QueryObfuscator, sqlDialect string) (*querylogs.QueryLog, error) {
			return convertFabricRowToQueryLog(row, obfuscator, sqlDialect, host)
		},
	), nil
}

// queryLogsSelect is the per-database projection over queryinsights. %[1]s is the
// bracket-quoted database prefix and %[2]s/%[3]s are the DATETIME2 window bounds.
const queryLogsSelect = `SELECT
    database_name,
    CAST(distributed_statement_id AS VARCHAR(64)) AS query_id,
    login_name, submit_time, start_time, end_time,
    total_elapsed_time_ms, row_count, status, statement_type, command
FROM %[1]s.queryinsights.exec_requests_history
WHERE submit_time >= %[2]s AND submit_time < %[3]s`

func buildQueryLogsUnion(databases []string, from, to time.Time) string {
	fromLit := fabricDatetimeLiteral(from)
	toLit := fabricDatetimeLiteral(to)

	// With no in-scope databases, run the connection's own queryinsights but
	// select nothing, so the caller still gets a valid (empty) iterator.
	if len(databases) == 0 {
		return fmt.Sprintf(queryLogsSelect, "", fromLit, toLit) + " AND 1 = 0"
	}

	branches := make([]string, 0, len(databases))
	for _, db := range databases {
		branches = append(branches, fmt.Sprintf(queryLogsSelect, sqldialect.MSSQLQuoteIdentifier(db), fromLit, toLit))
	}
	return strings.Join(branches, "\nUNION ALL\n")
}

func fabricDatetimeLiteral(t time.Time) string {
	return "CAST('" + t.UTC().Format("2006-01-02 15:04:05") + "' AS DATETIME2)"
}

// fabricStatusToStatus maps a queryinsights status to our canonical status.
func fabricStatusToStatus(status string) string {
	switch strings.ToLower(status) {
	case "succeeded":
		return "SUCCESS"
	case "failed":
		return "FAILED"
	case "cancelled", "canceled":
		return "ABORTED"
	case "running":
		return "RUNNING"
	default:
		return "UNKNOWN"
	}
}

func convertFabricRowToQueryLog(
	row *FabricQueryLogSchema,
	obfuscator querylogs.QueryObfuscator,
	sqlDialect string,
	host string,
) (*querylogs.QueryLog, error) {
	var createdAt time.Time
	switch {
	case row.EndTime != nil:
		createdAt = *row.EndTime
	case row.SubmitTime != nil:
		createdAt = *row.SubmitTime
	default:
		createdAt = time.Now()
	}

	queryText := ""
	if row.Command != nil {
		queryText = strings.TrimSpace(strings.ToValidUTF8(*row.Command, ""))
		queryText = obfuscator.Obfuscate(queryText)
	}

	queryType := ""
	if row.StatementType != nil {
		queryType = *row.StatementType
	}

	metadata := map[string]*structpb.Value{
		"login_name":            querylogs.StringPtrValue(row.LoginName),
		"submit_time":           querylogs.TimePtrValue(row.SubmitTime),
		"start_time":            querylogs.TimePtrValue(row.StartTime),
		"end_time":              querylogs.TimePtrValue(row.EndTime),
		"total_elapsed_time_ms": querylogs.IntPtrValue(row.ElapsedMs),
		"row_count":             querylogs.IntPtrValue(row.RowCount),
		"statement_type":        querylogs.StringPtrValue(row.StatementType),
	}

	return &querylogs.QueryLog{
		CreatedAt:                createdAt,
		StartedAt:                row.StartTime,
		FinishedAt:               row.EndTime,
		QueryID:                  row.QueryID,
		SQL:                      queryText,
		SqlDialect:               sqlDialect,
		DwhContext:               &querylogs.DwhContext{Instance: host, Database: row.Database},
		QueryType:                queryType,
		Status:                   fabricStatusToStatus(row.Status),
		Metadata:                 querylogs.NewMetadataStruct(metadata),
		SqlObfuscationMode:       obfuscator.Mode(),
		HasCompleteNativeLineage: false,
		NativeLineage:            nil,
	}, nil
}
