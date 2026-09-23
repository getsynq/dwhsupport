package db2

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/structpb"
)

//go:embed query_logs.sql
var queryLogsSql string

// Db2QueryLogSchema maps a row of MON_GET_PKG_CACHE_STMT (query_logs.sql).
type Db2QueryLogSchema struct {
	ExecutableId       string     `db:"EXECUTABLE_ID"`
	StmtId             *int64     `db:"STMTID"`
	PlanId             *int64     `db:"PLANID"`
	Member             *int64     `db:"MEMBER"`
	SectionType        *string    `db:"SECTION_TYPE"`
	PackageSchema      *string    `db:"PACKAGE_SCHEMA"`
	PackageName        *string    `db:"PACKAGE_NAME"`
	StmtTypeId         *string    `db:"STMT_TYPE_ID"`
	InsertTimestamp    *time.Time `db:"INSERT_TIMESTAMP"`
	LastMetricsUpdate  *time.Time `db:"LAST_METRICS_UPDATE"`
	NumExecutions      *int64     `db:"NUM_EXECUTIONS"`
	NumExecWithMetrics *int64     `db:"NUM_EXEC_WITH_METRICS"`
	TotalActTime       *int64     `db:"TOTAL_ACT_TIME"`
	TotalActWaitTime   *int64     `db:"TOTAL_ACT_WAIT_TIME"`
	TotalCpuTime       *int64     `db:"TOTAL_CPU_TIME"`
	RowsRead           *int64     `db:"ROWS_READ"`
	RowsReturned       *int64     `db:"ROWS_RETURNED"`
	RowsModified       *int64     `db:"ROWS_MODIFIED"`
	TotalSorts         *int64     `db:"TOTAL_SORTS"`
	LogicalReads       *int64     `db:"LOGICAL_READS"`
	PhysicalReads      *int64     `db:"PHYSICAL_READS"`
	QueryCostEstimate  *int64     `db:"QUERY_COST_ESTIMATE"`
	DatabaseName       string     `db:"DATABASE_NAME"`
	StmtText           *string    `db:"STMT_TEXT"`
}

var _ querylogs.QueryLogsProvider = &Db2Scrapper{}

// FetchQueryLogs reads the package cache (MON_GET_PKG_CACHE_STMT): one entry
// per cached statement, with metrics accumulated over its executions, for the
// statements whose metrics were last updated in [from, to). Like Oracle's V$SQL,
// it holds no per-execution history and no executing user, and a statement
// ages out under memory pressure. Needs EXECUTE on MON_GET_PKG_CACHE_STMT.
func (e *Db2Scrapper) FetchQueryLogs(
	ctx context.Context,
	from, to time.Time,
	obfuscator querylogs.QueryObfuscator,
) (querylogs.QueryLogIterator, error) {
	if obfuscator == nil {
		return nil, errors.New("obfuscator is required")
	}

	// The bounds go in as text: the query compares them with the cache's
	// timestamps converted to UTC, and text leaves no time zone to the driver.
	const db2TimestampFmt = "2006-01-02 15:04:05.000000"
	rows, err := e.executor.QueryRows(ctx, queryLogsSql, from.UTC().Format(db2TimestampFmt), to.UTC().Format(db2TimestampFmt))
	if err != nil {
		return nil, err
	}

	hostname := e.conf.Hostname
	return querylogs.NewSqlxRowsIterator[Db2QueryLogSchema](
		rows,
		obfuscator,
		e.DialectType(),
		func(row *Db2QueryLogSchema, obfuscator querylogs.QueryObfuscator, sqlDialect string) (*querylogs.QueryLog, error) {
			return convertDb2RowToQueryLog(row, obfuscator, sqlDialect, hostname)
		},
	), nil
}

func convertDb2RowToQueryLog(
	row *Db2QueryLogSchema,
	obfuscator querylogs.QueryObfuscator,
	sqlDialect string,
	hostname string,
) (*querylogs.QueryLog, error) {
	createdAt := time.Now()
	var finishedAt *time.Time
	if row.LastMetricsUpdate != nil {
		t := row.LastMetricsUpdate.UTC()
		createdAt, finishedAt = t, &t
	}

	queryText := ""
	if row.StmtText != nil {
		queryText = strings.TrimSpace(strings.ToValidUTF8(*row.StmtText, ""))
	}
	queryType := db2QueryType(queryText, row.StmtTypeId)
	queryText = obfuscator.Obfuscate(queryText)

	// EXECUTABLE_ID identifies the cache entry; a statement cached on several
	// members (pureScale, DPF) has one entry per member.
	queryID := row.ExecutableId
	if row.Member != nil && *row.Member != 0 {
		queryID += "-" + strconv.FormatInt(*row.Member, 10)
	}

	// STMTID hashes the normalized statement text: two statements that differ
	// only in a comment share it, so it keys lineage the way other platforms'
	// normalized query hashes do.
	var normalizedHash *string
	if row.StmtId != nil {
		h := strconv.FormatInt(*row.StmtId, 10)
		normalizedHash = &h
	}

	metadata := map[string]*structpb.Value{
		"executable_id":          querylogs.StringValue(row.ExecutableId),
		"stmtid":                 querylogs.IntPtrValue(row.StmtId),
		"planid":                 querylogs.IntPtrValue(row.PlanId),
		"member":                 querylogs.IntPtrValue(row.Member),
		"section_type":           querylogs.TrimmedStringPtrValue(row.SectionType),
		"package_schema":         querylogs.TrimmedStringPtrValue(row.PackageSchema),
		"package_name":           querylogs.TrimmedStringPtrValue(row.PackageName),
		"stmt_type_id":           querylogs.TrimmedStringPtrValue(row.StmtTypeId),
		"insert_timestamp":       querylogs.TimePtrValue(row.InsertTimestamp),
		"last_metrics_update":    querylogs.TimePtrValue(row.LastMetricsUpdate),
		"num_executions":         querylogs.IntPtrValue(row.NumExecutions),
		"num_exec_with_metrics":  querylogs.IntPtrValue(row.NumExecWithMetrics),
		"total_act_time_ms":      querylogs.IntPtrValue(row.TotalActTime),
		"total_act_wait_time_ms": querylogs.IntPtrValue(row.TotalActWaitTime),
		"total_cpu_time_us":      querylogs.IntPtrValue(row.TotalCpuTime),
		"rows_read":              querylogs.IntPtrValue(row.RowsRead),
		"rows_returned":          querylogs.IntPtrValue(row.RowsReturned),
		"rows_modified":          querylogs.IntPtrValue(row.RowsModified),
		"total_sorts":            querylogs.IntPtrValue(row.TotalSorts),
		"logical_reads":          querylogs.IntPtrValue(row.LogicalReads),
		"physical_reads":         querylogs.IntPtrValue(row.PhysicalReads),
		"query_cost_estimate":    querylogs.IntPtrValue(row.QueryCostEstimate),
	}

	return &querylogs.QueryLog{
		CreatedAt:           createdAt,
		StartedAt:           nil, // the cache keeps no per-execution start time
		FinishedAt:          finishedAt,
		QueryID:             queryID,
		SQL:                 queryText,
		NormalizedQueryHash: normalizedHash,
		SqlDialect:          sqlDialect,
		DwhContext: &querylogs.DwhContext{
			Instance: hostname,
			Database: strings.TrimSpace(row.DatabaseName),
		},
		QueryType:                queryType,
		Status:                   "SUCCESS", // the cache keeps no per-execution outcome
		Metadata:                 querylogs.NewMetadataStruct(metadata),
		SqlObfuscationMode:       obfuscator.Mode(),
		HasCompleteNativeLineage: false,
	}, nil
}

// db2QueryType names a statement by its leading keyword, and by the object
// kind for CREATE, ALTER and DROP (CREATE TABLE, CREATE VIEW). STMT_TYPE_ID
// only says "DML, Insert/Update/Delete" or "DDL, (not Set Constraints)", so
// it is the fallback when the text gives nothing.
func db2QueryType(sql string, stmtTypeId *string) string {
	words := strings.Fields(strings.ToUpper(stripLeadingComments(sql)))
	if len(words) == 0 {
		if stmtTypeId != nil {
			return strings.TrimSpace(*stmtTypeId)
		}
		return ""
	}
	first := strings.TrimLeft(words[0], "(")
	switch first {
	case "CREATE", "ALTER", "DROP":
		rest := words[1:]
		if len(rest) >= 2 && rest[0] == "OR" && rest[1] == "REPLACE" {
			rest = rest[2:]
		}
		if len(rest) > 0 {
			return fmt.Sprintf("%s %s", first, rest[0])
		}
	case "WITH", "VALUES":
		return "SELECT"
	}
	return first
}

func stripLeadingComments(sql string) string {
	s := strings.TrimSpace(sql)
	for {
		switch {
		case strings.HasPrefix(s, "--"):
			i := strings.IndexByte(s, '\n')
			if i < 0 {
				return ""
			}
			s = strings.TrimSpace(s[i+1:])
		case strings.HasPrefix(s, "/*"):
			i := strings.Index(s, "*/")
			if i < 0 {
				return ""
			}
			s = strings.TrimSpace(s[i+2:])
		default:
			return s
		}
	}
}
