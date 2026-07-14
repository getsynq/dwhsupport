package fabric

import (
	"context"
	"fmt"
	"io"
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
// ~30 days). The schema exists only in Warehouses — a workspace SQL endpoint also
// exposes Lakehouse SQL endpoints, SQL databases and mirrored databases, none of
// which have queryinsights. So rather than one UNION ALL (which fails entirely
// the moment a single non-Warehouse database is in scope), this reads each
// in-scope database independently and skips any that raise the "Invalid object
// name" error for the queryinsights view. The [from, to] window is inlined as
// DATETIME2 literals rather than bound parameters.
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

	return &fabricQueryLogsIterator{
		e:          e,
		databases:  databases,
		from:       from,
		to:         to,
		obfuscator: obfuscator,
		host:       e.conf.Host,
	}, nil
}

// queryInsightsView is the queryinsights object read for query history. It is
// present only in Fabric Warehouses; the "Invalid object name" error naming it
// is how a non-Warehouse database in scope is recognised and skipped.
const queryInsightsView = "queryinsights.exec_requests_history"

// queryLogsSelect is the per-database projection over queryinsights. %[1]s is the
// bracket-quoted database prefix and %[2]s/%[3]s are the DATETIME2 window bounds.
const queryLogsSelect = `SELECT
    database_name,
    CAST(distributed_statement_id AS VARCHAR(64)) AS query_id,
    login_name, submit_time, start_time, end_time,
    total_elapsed_time_ms, row_count, status, statement_type, command
FROM %[1]s.` + queryInsightsView + `
WHERE submit_time >= %[2]s AND submit_time < %[3]s`

func buildQueryLogsSelect(database string, from, to time.Time) string {
	return fmt.Sprintf(queryLogsSelect, sqldialect.MSSQLQuoteIdentifier(database), fabricDatetimeLiteral(from), fabricDatetimeLiteral(to))
}

// isQueryInsightsMissing reports whether err is the "Invalid object name" error
// Fabric raises when a database has no queryinsights schema — i.e. it is a
// Lakehouse SQL endpoint, SQL database or mirrored database rather than a
// Warehouse. Only this exact error skips the database; every other error
// (including permission denials) propagates so real misconfigurations surface.
func isQueryInsightsMissing(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Invalid object name") && strings.Contains(msg, queryInsightsView)
}

// fabricQueryLogsIterator reads queryinsights across the in-scope databases one
// at a time, lazily opening the next only when the current is exhausted, and
// skipping databases whose queryinsights view is absent (non-Warehouses).
type fabricQueryLogsIterator struct {
	e          *FabricScrapper
	databases  []string
	from, to   time.Time
	obfuscator querylogs.QueryObfuscator
	host       string

	idx     int
	current querylogs.QueryLogIterator
	closed  bool
}

func (it *fabricQueryLogsIterator) Next(ctx context.Context) (*querylogs.QueryLog, error) {
	for {
		if it.closed {
			return nil, io.EOF
		}

		if it.current == nil {
			// Advance to the next database that actually exposes queryinsights.
			for it.idx < len(it.databases) {
				select {
				case <-ctx.Done():
					it.Close()
					return nil, ctx.Err()
				default:
				}

				database := it.databases[it.idx]
				it.idx++

				rows, err := it.e.executor.QueryRows(ctx, buildQueryLogsSelect(database, it.from, it.to))
				if err != nil {
					if isQueryInsightsMissing(err) {
						continue // non-Warehouse database: no query history to read
					}
					return nil, err
				}

				host := it.host
				it.current = querylogs.NewSqlxRowsIterator[FabricQueryLogSchema](
					rows,
					it.obfuscator,
					it.e.DialectType(),
					func(row *FabricQueryLogSchema, obfuscator querylogs.QueryObfuscator, sqlDialect string) (*querylogs.QueryLog, error) {
						return convertFabricRowToQueryLog(row, obfuscator, sqlDialect, host)
					},
				)
				break
			}

			if it.current == nil {
				it.Close()
				return nil, io.EOF
			}
		}

		log, err := it.current.Next(ctx)
		if err == io.EOF {
			it.current = nil // exhausted this database; move to the next
			continue
		}
		if err != nil {
			return nil, err
		}
		return log, nil
	}
}

func (it *fabricQueryLogsIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	if it.current != nil {
		return it.current.Close()
	}
	return nil
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
