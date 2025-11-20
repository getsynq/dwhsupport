package clickhouse

import (
	"context"
	_ "embed"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_logs.sql
var queryLogsSql string

type ClickhouseQueryLogSchema struct {
	QueryType                  string    `db:"type"`
	EventTimeMicroseconds      time.Time `db:"event_time_microseconds"`
	QueryStartTimeMicroseconds time.Time `db:"query_start_time_microseconds"`

	// Performance Information
	QueryDurationMs  uint64 `db:"query_duration_ms"`
	PeakThreadsUsage uint64 `db:"peak_threads_usage"`

	// IO Information
	ReadRows  uint64 `db:"read_rows"`
	ReadBytes uint64 `db:"read_bytes"`

	WrittenRows  uint64 `db:"written_rows"`
	WrittenBytes uint64 `db:"written_bytes"`

	ResultRows  uint64 `db:"result_rows"`
	ResultBytes uint64 `db:"result_bytes"`

	// Query Information
	MemoryUsage            uint64   `db:"memory_usage"`
	CurrentDatabase        string   `db:"current_database"`
	Query                  string   `db:"normalized_query"`
	NormalizedQueryHash    uint64   `db:"normalized_query_hash"`
	QueryKind              string   `db:"query_kind"`
	Databases              []string `db:"databases"`
	Tables                 []string `db:"tables"`
	Columns                []string `db:"columns"`
	Partitions             []string `db:"partitions"`
	Projections            []string `db:"projections"`
	Views                  []string `db:"views"`
	UsedFunctions          []string `db:"used_functions"`
	UsedAggregateFunctions []string `db:"used_aggregate_functions"`
	ExceptionCode          int32    `db:"exception_code"`
	Exception              string   `db:"exception"`
	StackTrace             string   `db:"stack_trace"`

	// Server Information
	Hostname string `db:"hostname"`

	// Client Information
	InitialUser                       string    `db:"initial_user"`
	InitialQueryId                    string    `db:"initial_query_id"`
	InitialAddress                    *net.IP   `db:"initial_address"`
	InitialPort                       uint16    `db:"initial_port"`
	InitialQueryStartTimeMicroseconds time.Time `db:"initial_query_start_time_microseconds"`
	OsUser                            string    `db:"os_user"`
	ClientHostname                    string    `db:"client_hostname"`
	ClientName                        string    `db:"client_name"`
	ClientRevision                    uint32    `db:"client_revision"`
	ClientRevisionMajor               uint32    `db:"client_version_major"`
	ClientRevisionMinor               uint32    `db:"client_version_minor"`
	ClientRevisionPatch               uint32    `db:"client_version_patch"`
	DistributedDepth                  uint64    `db:"distributed_depth"`

	// HTTP Client Information
	HttpMethod    uint8  `db:"http_method"`
	HttpUserAgent string `db:"http_user_agent"`
	HttpReferer   string `db:"http_referer"`
	ForwardedFor  string `db:"forwarded_for"`

	// Script Information
	ScriptQueryNumber uint32 `db:"script_query_number"`
	ScriptLineNumber  uint32 `db:"script_line_number"`
}

func (s *ClickhouseScrapper) FetchQueryLogs(
	ctx context.Context,
	from, to time.Time,
	obfuscator querylogs.QueryObfuscator,
) (querylogs.QueryLogIterator, error) {
	// Validate obfuscator is provided
	if obfuscator == nil {
		return nil, fmt.Errorf("obfuscator is required")
	}

	// Build SQL query with conditional normalization based on obfuscation mode
	sql := s.buildQueryLogsSql(obfuscator.Mode())

	// Use native QueryRows - returns sqlx.Rows iterator
	rows, err := s.Executor().QueryRows(ctx, sql, from, to)
	if err != nil {
		return nil, err
	}

	return querylogs.NewSqlxRowsIterator[ClickhouseQueryLogSchema](rows, obfuscator, s.DialectType(), convertClickhouseRowToQueryLog), nil
}

func (s *ClickhouseScrapper) buildQueryLogsSql(mode querylogs.ObfuscationMode) string {
	// Choose query column based on obfuscation mode
	var queryColumn string
	if mode == querylogs.ObfuscationNone {
		// No obfuscation - return raw query with actual literal values
		queryColumn = "query as normalized_query"
	} else {
		// ObfuscationRedactLiterals - use ClickHouse's normalizeQuery to replace literals with ?
		queryColumn = "normalizeQuery(query) as normalized_query"
	}

	return `SELECT type,
       event_time_microseconds,
       query_start_time_microseconds,
       query_duration_ms,
       peak_threads_usage,
       read_rows,
       read_bytes,
       written_rows,
       written_bytes,
       result_rows,
       result_bytes,
       memory_usage,
       current_database,
       query_kind,
       databases,
       tables,
       columns,
       partitions,
       projections,
       views,
       used_functions,
       used_aggregate_functions,
       exception_code,
       exception,
       stack_trace,
       hostname,
       initial_user,
       initial_query_id,
       initial_address,
       initial_port,
       initial_query_start_time_microseconds,
       os_user,
       client_hostname,
       client_name,
       client_revision,
       client_version_major,
       client_version_minor,
       client_version_patch,
       distributed_depth,
       http_method,
       http_user_agent,
       http_referer,
       forwarded_for,
       script_query_number,
       script_line_number,
       ` + queryColumn + `,
       normalized_query_hash

FROM
    clusterAllReplicas(default, system.query_log)

WHERE type in ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing')
  AND is_initial_query = true
  AND event_time between ? and ?
  AND notEmpty(tables)
`
}

func convertClickhouseRowToQueryLog(
	row *ClickhouseQueryLogSchema,
	obfuscator querylogs.QueryObfuscator,
	sqlDialect string,
) (*querylogs.QueryLog, error) {
	// Parse tables array into native lineage
	var nativeLineage *querylogs.NativeLineage
	if len(row.Tables) > 0 {
		inputTables := make([]scrapper.DwhFqn, 0, len(row.Tables))

		for _, t := range row.Tables {
			// Parse "schema.table" format
			var schema, table string
			split := strings.Split(t, ".")
			if len(split) == 1 {
				schema = ""
				table = split[0]
			} else {
				schema = split[0]
				table = strings.Join(split[1:], ".")
			}

			// Filter temporary tables like "`.inner_id.e16b8c51-6afc-4d0f-877c-76e4f75b39d9"
			if strings.HasPrefix(table, "`.") {
				continue
			}

			inputTables = append(inputTables, scrapper.DwhFqn{
				DatabaseName: "",     // ClickHouse doesn't have a separate database/instance concept
				SchemaName:   schema, // ClickHouse "database" is equivalent to "schema"
				ObjectName:   table,
			})
		}

		if len(inputTables) > 0 {
			nativeLineage = &querylogs.NativeLineage{
				InputTables:  inputTables,
				OutputTables: nil, // ClickHouse doesn't provide output tables in query_log
			}
		}
	}

	// Determine status
	status := "SUCCESS"
	if row.Exception != "" {
		status = "FAILED"
	}

	// Build metadata with all ClickHouse-specific fields
	metadata := map[string]any{
		"query_type":               row.QueryType,
		"query_duration_ms":        row.QueryDurationMs,
		"peak_threads_usage":       row.PeakThreadsUsage,
		"read_rows":                row.ReadRows,
		"read_bytes":               row.ReadBytes,
		"written_rows":             row.WrittenRows,
		"written_bytes":            row.WrittenBytes,
		"result_rows":              row.ResultRows,
		"result_bytes":             row.ResultBytes,
		"memory_usage":             row.MemoryUsage,
		"databases":                row.Databases,
		"columns":                  row.Columns,
		"partitions":               row.Partitions,
		"projections":              row.Projections,
		"views":                    row.Views,
		"used_functions":           row.UsedFunctions,
		"used_aggregate_functions": row.UsedAggregateFunctions,
		"exception_code":           row.ExceptionCode,
		"stack_trace":              row.StackTrace,
		"hostname":                 row.Hostname,
		"os_user":                  row.OsUser,
		"client_hostname":          row.ClientHostname,
		"client_name":              row.ClientName,
		"client_revision":          row.ClientRevision,
		"client_version_major":     row.ClientRevisionMajor,
		"client_version_minor":     row.ClientRevisionMinor,
		"client_version_patch":     row.ClientRevisionPatch,
		"distributed_depth":        row.DistributedDepth,
		"http_method":              row.HttpMethod,
		"http_user_agent":          row.HttpUserAgent,
		"http_referer":             row.HttpReferer,
		"forwarded_for":            row.ForwardedFor,
		"script_query_number":      row.ScriptQueryNumber,
		"script_line_number":       row.ScriptLineNumber,
	}

	// Always add initial_port (some queries may not have an address)
	metadata["initial_port"] = row.InitialPort
	if row.InitialAddress != nil {
		metadata["initial_address"] = row.InitialAddress.String()
	}

	// Apply obfuscation (may be no-op if already normalized by ClickHouse)
	queryText := obfuscator.Obfuscate(row.Query)

	// Timing information
	startedAt := row.QueryStartTimeMicroseconds
	finishedAt := row.EventTimeMicroseconds

	// Use native normalized_query_hash from ClickHouse
	normalizedQueryHash := fmt.Sprintf("%d", row.NormalizedQueryHash)

	return &querylogs.QueryLog{
		// Use EventTimeMicroseconds (when query finished) to match old implementation
		CreatedAt:           row.EventTimeMicroseconds,
		StartedAt:           &startedAt,
		FinishedAt:          &finishedAt,
		QueryID:             row.InitialQueryId,
		SQL:                 queryText,
		NormalizedQueryHash: &normalizedQueryHash,
		SqlDialect:          sqlDialect,
		DwhContext: &querylogs.DwhContext{
			Database: "",                  // ClickHouse doesn't have a separate database/instance concept
			Schema:   row.CurrentDatabase, // ClickHouse "database" is equivalent to "schema" in other systems
			User:     row.InitialUser,
		},
		QueryType:                row.QueryKind,
		Status:                   status,
		Metadata:                 metadata,
		SqlObfuscationMode:       obfuscator.Mode(),
		HasCompleteNativeLineage: false, // ClickHouse only provides input tables, not complete lineage
		NativeLineage:            nativeLineage,
	}, nil
}
