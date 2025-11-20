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

	// IO Information
	ReadRows  uint64 `db:"read_rows"`
	ReadBytes uint64 `db:"read_bytes"`

	WrittenRows  uint64 `db:"written_rows"`
	WrittenBytes uint64 `db:"written_bytes"`

	ResultRows  uint64 `db:"result_rows"`
	ResultBytes uint64 `db:"result_bytes"`

	// Query Information
	MemoryUsage     uint64   `db:"memory_usage"`
	CurrentDatabase string   `db:"current_database"`
	Query           string   `db:"normalized_query"`
	QueryKind       string   `db:"query_kind"`
	Databases       []string `db:"databases"`
	Tables          []string `db:"tables"`
	Columns         []string `db:"columns"`
	Projections     []string `db:"projections"`
	Views           []string `db:"views"`
	ExceptionCode   int32    `db:"exception_code"`
	Exception       string   `db:"exception"`
	StackTrace      string   `db:"stack_trace"`

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
}

func (s *ClickhouseScrapper) FetchQueryLogs(ctx context.Context, from, to time.Time, obfuscator querylogs.QueryObfuscator) (querylogs.QueryLogIterator, error) {
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

	return querylogs.NewSqlxRowsIterator[ClickhouseQueryLogSchema](rows, obfuscator, convertClickhouseRowToQueryLog), nil
}

func (s *ClickhouseScrapper) buildQueryLogsSql(mode querylogs.ObfuscationMode) string {
	// Conditionally use normalizeQuery based on obfuscation mode
	// If mode is None, select raw query; otherwise use normalizeQuery()
	queryColumn := "query as normalized_query"
	if mode != querylogs.ObfuscationNone {
		queryColumn = "normalizeQuery(query) as normalized_query"
	}

	return `SELECT type,
       event_time_microseconds,
       query_start_time_microseconds,
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
       projections,
       views,
       exception_code,
       exception,
       stack_trace,
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
       ` + queryColumn + `

FROM
    clusterAllReplicas(default, system.query_log)

WHERE type in ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing')
  AND is_initial_query = true
  AND event_time between ? and ?
  AND notEmpty(tables)
`
}

func convertClickhouseRowToQueryLog(row *ClickhouseQueryLogSchema, obfuscator querylogs.QueryObfuscator) (*querylogs.QueryLog, error) {
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
		"query_type":        row.QueryType,
		"read_rows":         row.ReadRows,
		"read_bytes":        row.ReadBytes,
		"written_rows":      row.WrittenRows,
		"written_bytes":     row.WrittenBytes,
		"result_rows":       row.ResultRows,
		"result_bytes":      row.ResultBytes,
		"memory_usage":      row.MemoryUsage,
		"databases":         row.Databases,
		"columns":           row.Columns,
		"projections":       row.Projections,
		"views":             row.Views,
		"exception_code":    row.ExceptionCode,
		"stack_trace":       row.StackTrace,
		"os_user":           row.OsUser,
		"client_hostname":   row.ClientHostname,
		"client_name":       row.ClientName,
		"client_revision":   row.ClientRevision,
		"distributed_depth": row.DistributedDepth,
	}

	if row.InitialAddress != nil {
		metadata["initial_address"] = row.InitialAddress.String()
		metadata["initial_port"] = row.InitialPort
	}

	// Apply obfuscation (may be no-op if already normalized by ClickHouse)
	queryText := obfuscator.Obfuscate(row.Query)

	return &querylogs.QueryLog{
		CreatedAt: row.QueryStartTimeMicroseconds,
		QueryID:   row.InitialQueryId,
		SQL:       queryText,
		DwhContext: &querylogs.DwhContext{
			Database: "", // ClickHouse doesn't have a separate database/instance concept
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
