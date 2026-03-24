package oracle

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
	"google.golang.org/protobuf/types/known/structpb"
)

//go:embed query_logs.sql
var queryLogsSql string

//go:embed query_logs_diag.sql
var queryLogsDiagSql string

// OracleQueryLogSchema maps columns from V$SQL or DBA_HIST query results.
// Fields shared between both sources use the same column aliases.
type OracleQueryLogSchema struct {
	SqlId             string     `db:"SQL_ID"`
	SqlFulltext       *string    `db:"SQL_FULLTEXT"`
	ParsingSchemaName *string    `db:"PARSING_SCHEMA_NAME"`
	LastActiveTime    *time.Time `db:"LAST_ACTIVE_TIME"`
	IntervalStart     *time.Time `db:"INTERVAL_START"`
	Executions        *int64     `db:"EXECUTIONS"`
	ElapsedTime       *int64     `db:"ELAPSED_TIME"`
	CpuTime           *int64     `db:"CPU_TIME"`
	DiskReads         *int64     `db:"DISK_READS"`
	BufferGets        *int64     `db:"BUFFER_GETS"`
	RowsProcessed     *int64     `db:"ROWS_PROCESSED"`
	CommandType       *int64     `db:"COMMAND_TYPE"`
	Module            *string    `db:"MODULE"`
	Action            *string    `db:"ACTION"`
	OptimizerCost     *int64     `db:"OPTIMIZER_COST"`
	Fetches           *int64     `db:"FETCHES"`
	Sorts             *int64     `db:"SORTS"`
}

var _ querylogs.QueryLogsProvider = &OracleScrapper{}

func (s *OracleScrapper) FetchQueryLogs(
	ctx context.Context,
	from, to time.Time,
	obfuscator querylogs.QueryObfuscator,
) (querylogs.QueryLogIterator, error) {
	if obfuscator == nil {
		return nil, fmt.Errorf("obfuscator is required")
	}

	sql := queryLogsSql
	if s.conf.UseDiagnosticsPack {
		sql = queryLogsDiagSql
	}

	// Pass times as formatted strings with TO_DATE() because go-ora's time.Time
	// binding does not compare correctly with Oracle DATE columns in WHERE clauses.
	// Oracle DATE is timezone-unaware; we use UTC to match server-side SYSDATE.
	const oraDateFmt = "2006-01-02 15:04:05"
	fromStr := from.UTC().Format(oraDateFmt)
	toStr := to.UTC().Format(oraDateFmt)
	rows, err := s.executor.QueryRows(ctx, sql, fromStr, toStr)
	if err != nil {
		return nil, err
	}

	host := s.conf.Host
	serviceName := s.conf.ServiceName
	return querylogs.NewSqlxRowsIterator[OracleQueryLogSchema](
		rows,
		obfuscator,
		s.DialectType(),
		func(row *OracleQueryLogSchema, obfuscator querylogs.QueryObfuscator, sqlDialect string) (*querylogs.QueryLog, error) {
			return convertOracleRowToQueryLog(row, obfuscator, sqlDialect, host, serviceName)
		},
	), nil
}

// oracleCommandTypeToString maps Oracle command_type numbers to human-readable strings.
// See Oracle documentation for V$SQL.COMMAND_TYPE values.
func oracleCommandTypeToString(ct *int64) string {
	if ct == nil {
		return ""
	}
	switch *ct {
	case 1:
		return "CREATE TABLE"
	case 2:
		return "INSERT"
	case 3:
		return "SELECT"
	case 6:
		return "UPDATE"
	case 7:
		return "DELETE"
	case 9:
		return "CREATE INDEX"
	case 11:
		return "ALTER INDEX"
	case 12:
		return "DROP TABLE"
	case 15:
		return "ALTER TABLE"
	case 26:
		return "LOCK TABLE"
	case 44:
		return "COMMIT"
	case 45:
		return "ROLLBACK"
	case 47:
		return "PL/SQL EXECUTE"
	case 48:
		return "SET TRANSACTION"
	case 50:
		return "EXPLAIN"
	case 85:
		return "TRUNCATE TABLE"
	case 170:
		return "CALL"
	case 189:
		return "MERGE"
	default:
		return fmt.Sprintf("COMMAND_%d", *ct)
	}
}

func convertOracleRowToQueryLog(
	row *OracleQueryLogSchema,
	obfuscator querylogs.QueryObfuscator,
	sqlDialect string,
	host string,
	serviceName string,
) (*querylogs.QueryLog, error) {
	var createdAt time.Time
	if row.LastActiveTime != nil {
		createdAt = *row.LastActiveTime
	} else {
		createdAt = time.Now()
	}

	queryText := ""
	if row.SqlFulltext != nil {
		queryText = strings.TrimSpace(strings.ToValidUTF8(*row.SqlFulltext, ""))
		queryText = obfuscator.Obfuscate(queryText)
	}

	queryType := oracleCommandTypeToString(row.CommandType)

	schema := ""
	if row.ParsingSchemaName != nil {
		schema = strings.TrimSpace(*row.ParsingSchemaName)
	}

	metadata := map[string]*structpb.Value{
		"sql_id":              querylogs.StringValue(row.SqlId),
		"parsing_schema_name": querylogs.TrimmedStringPtrValue(row.ParsingSchemaName),
		"last_active_time":    querylogs.TimePtrValue(row.LastActiveTime),
		"interval_start":      querylogs.TimePtrValue(row.IntervalStart),
		"executions":          querylogs.IntPtrValue(row.Executions),
		"elapsed_time_us":     querylogs.IntPtrValue(row.ElapsedTime),
		"cpu_time_us":         querylogs.IntPtrValue(row.CpuTime),
		"disk_reads":          querylogs.IntPtrValue(row.DiskReads),
		"buffer_gets":         querylogs.IntPtrValue(row.BufferGets),
		"rows_processed":      querylogs.IntPtrValue(row.RowsProcessed),
		"command_type":        querylogs.IntPtrValue(row.CommandType),
		"module":              querylogs.TrimmedStringPtrValue(row.Module),
		"action":              querylogs.TrimmedStringPtrValue(row.Action),
		"optimizer_cost":      querylogs.IntPtrValue(row.OptimizerCost),
		"fetches":             querylogs.IntPtrValue(row.Fetches),
		"sorts":               querylogs.IntPtrValue(row.Sorts),
	}

	return &querylogs.QueryLog{
		CreatedAt:  createdAt,
		StartedAt:  nil, // V$SQL/DBA_HIST don't provide per-execution start time
		FinishedAt: row.LastActiveTime,
		QueryID:    row.SqlId,
		SQL:        queryText,
		SqlDialect: sqlDialect,
		DwhContext: &querylogs.DwhContext{
			Instance: host,
			Database: serviceName,
			Schema:   schema,
		},
		QueryType:                queryType,
		Status:                   "SUCCESS", // V$SQL only contains successfully parsed/executed SQL
		Metadata:                 querylogs.NewMetadataStruct(metadata),
		SqlObfuscationMode:       obfuscator.Mode(),
		HasCompleteNativeLineage: false,
		NativeLineage:            nil,
	}, nil
}
