package redshift

import (
	"context"
	_ "embed"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/getsynq/dwhsupport/rowscan"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/structpb"
)

var (
	//go:embed query_logs_history.sql
	queryLogsHistorySql string

	//go:embed query_logs.sql
	queryLogsTemplate string

	// queryLogsSql reads SYS_QUERY_HISTORY together with each statement's user name and, for a long
	// one, its SYS_QUERY_TEXT chunks.
	queryLogsSql = strings.Replace(queryLogsTemplate, "/* SYS_QUERY_HISTORY */", queryLogsHistorySql, 1)
)

// queryTextChunkMinBytes is the shortest query_text that may be only the start of a statement.
//
// SYS_QUERY_HISTORY.query_text is the first chunk of the statement's text, and SYS_QUERY_TEXT holds
// every chunk, keyed by query_id and sequence. A chunk holds at most 4000 bytes, but Redshift ends one
// short of that often enough that the length of query_text alone cannot tell a cut statement from a
// complete one. Any statement whose query_text is at least this long has its chunks joined, and a
// shorter one is always complete.
const queryTextChunkMinBytes = 3800

type RedshiftQueryLogSchema struct {
	UserId                *int64     `db:"user_id"`
	QueryId               int64      `db:"query_id"`
	QueryLabel            *string    `db:"query_label"`
	TransactionId         *int64     `db:"transaction_id"`
	SessionId             *int64     `db:"session_id"`
	DatabaseName          *string    `db:"database_name"`
	QueryType             *string    `db:"query_type"`
	Status                *string    `db:"status"`
	ResultCacheHit        *bool      `db:"result_cache_hit"`
	StartTime             *time.Time `db:"start_time"`
	EndTime               *time.Time `db:"end_time"`
	ElapsedTime           *int64     `db:"elapsed_time"`
	QueueTime             *int64     `db:"queue_time"`
	ExecutionTime         *int64     `db:"execution_time"`
	ErrorMessage          *string    `db:"error_message"`
	ReturnedRows          *int64     `db:"returned_rows"`
	ReturnedBytes         *int64     `db:"returned_bytes"`
	QueryText             *string    `db:"query_text"`
	RedshiftVersion       *string    `db:"redshift_version"`
	UsageLimit            *string    `db:"usage_limit"`
	ComputeType           *string    `db:"compute_type"`
	CompileTime           *int64     `db:"compile_time"`
	PlanningTime          *int64     `db:"planning_time"`
	LockWaitTime          *int64     `db:"lock_wait_time"`
	ServiceClassId        *int64     `db:"service_class_id"`
	ServiceClassName      *string    `db:"service_class_name"`
	QueryPriority         *string    `db:"query_priority"`
	ShortQueryAccelerated *string    `db:"short_query_accelerated"`
	GenericQueryHash      *string    `db:"generic_query_hash"`
	UserQueryHash         *string    `db:"user_query_hash"`
	UserName              *string    `db:"user_name"`
	TextSequence          *int64     `db:"text_sequence"`
	TextChunk             *string    `db:"text_chunk"`
}

// queryTextChunk is one SYS_QUERY_TEXT row of a statement, still escaped.
type queryTextChunk struct {
	sequence int64
	text     string
}

func (s *RedshiftScrapper) FetchQueryLogs(
	ctx context.Context,
	from, to time.Time,
	obfuscator querylogs.QueryObfuscator,
) (querylogs.QueryLogIterator, error) {
	// Validate obfuscator is provided
	if obfuscator == nil {
		return nil, errors.New("obfuscator is required")
	}

	return fetchQueryLogs(ctx, s.Executor().QueryRows, from, to, obfuscator, s.DialectType(), s.conf.Host, s.conf.Database)
}

type rowsQuerier func(ctx context.Context, sql string, args ...any) (*sqlx.Rows, error)

func fetchQueryLogs(
	ctx context.Context,
	query rowsQuerier,
	from, to time.Time,
	obfuscator querylogs.QueryObfuscator,
	sqlDialect, host, database string,
) (querylogs.QueryLogIterator, error) {
	// end_time is a zone-less timestamp in UTC, and a bound in any other zone would be compared by its
	// wall clock.
	from, to = from.UTC(), to.UTC()
	rows, err := query(ctx, queryLogsSql, from, to, queryTextChunkMinBytes)
	if err != nil && canReadHistoryAlone(err) {
		// A cluster that cannot read SYS_QUERY_TEXT or pg_user still has its query history. Its logs
		// carry no user name, and a statement long enough to have been cut is flagged as truncated.
		logging.GetLogger(ctx).WithError(err).
			Warn("Redshift query logs: reading SYS_QUERY_HISTORY without SYS_QUERY_TEXT and pg_user")
		rows, err = query(ctx, queryLogsHistorySql, from, to)
	}
	if err != nil {
		return nil, err
	}

	return &queryLogIterator{
		rows: rows,
		convert: func(row *RedshiftQueryLogSchema, chunks []queryTextChunk) (*querylogs.QueryLog, error) {
			return convertRedshiftRowToQueryLog(row, chunks, obfuscator, sqlDialect, host, database)
		},
	}, nil
}

// canReadHistoryAlone reports whether err is one the history-only query may not hit: something
// query_logs.sql reads beyond SYS_QUERY_HISTORY is missing, refused, or cannot be joined on this
// cluster.
func canReadHistoryAlone(err error) bool {
	pqError := &pq.Error{}
	if !errors.As(err, &pqError) {
		return false
	}
	switch pqError.Code {
	case "42P01", // undefined_table
		"42703", // undefined_column
		"42883", // undefined_function
		"42501", // insufficient_privilege
		"0A000": // feature_not_supported, which a leader-node-only join raises
		return true
	}
	return false
}

// queryLogIterator turns the rows of query_logs.sql into one QueryLog per statement. A statement long
// enough to have been cut arrives as one row per SYS_QUERY_TEXT chunk, next to each other and in
// sequence order, and the iterator collects them before converting.
type queryLogIterator struct {
	rows    *sqlx.Rows
	convert func(*RedshiftQueryLogSchema, []queryTextChunk) (*querylogs.QueryLog, error)
	scanner *rowscan.Scanner[RedshiftQueryLogSchema]
	// ahead is a row already read that starts the next statement.
	ahead  *RedshiftQueryLogSchema
	closed bool
}

func (it *queryLogIterator) Next(ctx context.Context) (*querylogs.QueryLog, error) {
	for {
		if it.closed {
			return nil, io.EOF
		}

		select {
		case <-ctx.Done():
			it.Close()
			return nil, ctx.Err()
		default:
		}

		first := it.ahead
		it.ahead = nil
		if first == nil {
			row, err := it.scan(ctx)
			if err != nil {
				return nil, err
			}
			if row == nil {
				it.Close()
				return nil, io.EOF
			}
			first = row
		}

		chunks := appendChunk(nil, first)
		for {
			row, err := it.scan(ctx)
			if err != nil {
				return nil, err
			}
			if row == nil {
				break
			}
			if row.QueryId != first.QueryId || row.TextSequence == nil {
				it.ahead = row
				break
			}
			chunks = appendChunk(chunks, row)
		}

		log, err := it.convert(first, chunks)
		if err != nil {
			return nil, err
		}
		if log == nil {
			continue
		}
		return log, nil
	}
}

// scan reads the next row, or returns nil once the rows are exhausted.
func (it *queryLogIterator) scan(ctx context.Context) (*RedshiftQueryLogSchema, error) {
	if !it.rows.Next() {
		return nil, it.rows.Err()
	}
	if it.scanner == nil {
		scanner, err := rowscan.New[RedshiftQueryLogSchema](it.rows)
		if err != nil {
			return nil, err
		}
		scanner.LogColumnDrift(ctx, "query history")
		it.scanner = scanner
	}
	var row RedshiftQueryLogSchema
	if err := it.scanner.Scan(it.rows, &row); err != nil {
		return nil, err
	}
	return &row, nil
}

func (it *queryLogIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	return it.rows.Close()
}

func appendChunk(chunks []queryTextChunk, row *RedshiftQueryLogSchema) []queryTextChunk {
	if row.TextSequence == nil {
		return chunks
	}
	text := ""
	if row.TextChunk != nil {
		text = *row.TextChunk
	}
	return append(chunks, queryTextChunk{sequence: *row.TextSequence, text: text})
}

// statementText returns the text of the statement as it ran, and whether it is incomplete.
//
// The chunks are joined before unescaping, because a chunk can end in the middle of an escape
// sequence, or of a multi-byte character.
func statementText(row *RedshiftQueryLogSchema, chunks []queryTextChunk) (string, bool) {
	historyText := ""
	if row.QueryText != nil {
		historyText = *row.QueryText
	}
	if len(historyText) < queryTextChunkMinBytes {
		return unescapeQueryText(historyText), false
	}

	chunks = slices.Clone(chunks)
	slices.SortStableFunc(chunks, func(a, b queryTextChunk) int { return int(a.sequence - b.sequence) })
	chunks = slices.CompactFunc(chunks, func(a, b queryTextChunk) bool { return a.sequence == b.sequence })

	var text strings.Builder
	contiguous := 0
	for _, chunk := range chunks {
		if chunk.sequence != int64(contiguous) {
			break
		}
		text.WriteString(chunk.text)
		contiguous++
	}
	if contiguous == 0 {
		// SYS_QUERY_TEXT has none of it, so query_text is all there is.
		return unescapeQueryText(historyText), true
	}
	return unescapeQueryText(text.String()), contiguous < len(chunks)
}

// unescapeQueryText reverses the escaping Redshift applies to query text in SYS_QUERY_HISTORY and
// SYS_QUERY_TEXT: a line feed is stored as `\n`, a carriage return as `\r`, and a backslash is doubled.
// A tab is stored as it is. Any other backslash is left alone.
func unescapeQueryText(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case 'n':
				b.WriteByte('\n')
				i++
				continue
			case 'r':
				b.WriteByte('\r')
				i++
				continue
			case '\\':
				b.WriteByte('\\')
				i++
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// trimStringPtr trims whitespace from a string pointer and returns it
// Returns nil if input is nil
func trimStringPtr(s *string) *string {
	if s == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*s)
	return &trimmed
}

func convertRedshiftRowToQueryLog(
	row *RedshiftQueryLogSchema,
	chunks []queryTextChunk,
	obfuscator querylogs.QueryObfuscator,
	sqlDialect string,
	host string,
	configDatabase string,
) (*querylogs.QueryLog, error) {
	// Determine status - Redshift provides it directly
	status := "UNKNOWN"
	if row.Status != nil {
		status = strings.ToUpper(strings.TrimSpace(*row.Status))
	}

	// Timing information - use end_time as CreatedAt (when query finished/logged)
	var createdAt time.Time
	if row.EndTime != nil {
		createdAt = *row.EndTime
	} else if row.StartTime != nil {
		createdAt = *row.StartTime // Fallback to start if end not available
	} else {
		createdAt = time.Now() // Defensive fallback
	}

	// Build metadata with all Redshift-specific fields
	// Include ALL available fields, even those mapped to higher-level QueryLog fields
	// nil values are filtered out by NewMetadataStruct
	metadata := map[string]*structpb.Value{
		// Fields also mapped to higher-level QueryLog fields
		"query_id":      querylogs.IntValue(row.QueryId),
		"database_name": querylogs.TrimmedStringPtrValue(row.DatabaseName),
		"query_type":    querylogs.TrimmedStringPtrValue(row.QueryType),
		"status":        querylogs.TrimmedStringPtrValue(row.Status),
		"start_time":    querylogs.TimePtrValue(row.StartTime),
		"end_time":      querylogs.TimePtrValue(row.EndTime),
		"user_name":     querylogs.TrimmedStringPtrValue(row.UserName),

		// Redshift-specific fields
		"user_id":                 querylogs.IntPtrValue(row.UserId),
		"query_label":             querylogs.TrimmedStringPtrValue(row.QueryLabel),
		"transaction_id":          querylogs.IntPtrValue(row.TransactionId),
		"session_id":              querylogs.IntPtrValue(row.SessionId),
		"result_cache_hit":        querylogs.BoolPtrValue(row.ResultCacheHit),
		"elapsed_time":            querylogs.IntPtrValue(row.ElapsedTime),
		"queue_time":              querylogs.IntPtrValue(row.QueueTime),
		"execution_time":          querylogs.IntPtrValue(row.ExecutionTime),
		"error_message":           querylogs.TrimmedStringPtrValue(row.ErrorMessage),
		"returned_rows":           querylogs.IntPtrValue(row.ReturnedRows),
		"returned_bytes":          querylogs.IntPtrValue(row.ReturnedBytes),
		"redshift_version":        querylogs.TrimmedStringPtrValue(row.RedshiftVersion),
		"usage_limit":             querylogs.TrimmedStringPtrValue(row.UsageLimit),
		"compute_type":            querylogs.TrimmedStringPtrValue(row.ComputeType),
		"compile_time":            querylogs.IntPtrValue(row.CompileTime),
		"planning_time":           querylogs.IntPtrValue(row.PlanningTime),
		"lock_wait_time":          querylogs.IntPtrValue(row.LockWaitTime),
		"service_class_id":        querylogs.IntPtrValue(row.ServiceClassId),
		"service_class_name":      querylogs.TrimmedStringPtrValue(row.ServiceClassName),
		"query_priority":          querylogs.TrimmedStringPtrValue(row.QueryPriority),
		"short_query_accelerated": querylogs.TrimmedStringPtrValue(row.ShortQueryAccelerated),
		"generic_query_hash":      querylogs.TrimmedStringPtrValue(row.GenericQueryHash),
		"user_query_hash":         querylogs.TrimmedStringPtrValue(row.UserQueryHash),
	}

	// Rebuild the statement from its chunks, sanitize and apply obfuscation
	queryText, isTruncated := statementText(row, chunks)
	queryText = strings.TrimSpace(strings.ToValidUTF8(queryText, ""))
	queryText = obfuscator.Obfuscate(queryText)

	// Get query type - trim whitespace
	queryType := ""
	if trimmed := trimStringPtr(row.QueryType); trimmed != nil {
		queryType = *trimmed
	}

	// Build DwhContext
	dwhContext := &querylogs.DwhContext{
		Instance: host,
	}
	if trimmed := trimStringPtr(row.DatabaseName); trimmed != nil && *trimmed != "" {
		dwhContext.Database = *trimmed
	} else {
		// Fall back to config database if not in row data
		dwhContext.Database = configDatabase
	}
	// A user dropped since the query ran has no name left, only its user_id in metadata.
	if trimmed := trimStringPtr(row.UserName); trimmed != nil {
		dwhContext.User = *trimmed
	}

	// generic_query_hash is shared by every statement that differs only in its literals, so it
	// identifies the shape, not the run.
	var normalizedQueryHash *string
	if trimmed := trimStringPtr(row.GenericQueryHash); trimmed != nil && *trimmed != "" {
		normalizedQueryHash = trimmed
	}

	// session_id is the session's process id, which Redshift hands out again once the session ends.
	var sessionID *string
	if row.SessionId != nil {
		id := strconv.FormatInt(*row.SessionId, 10)
		sessionID = &id
	}

	return &querylogs.QueryLog{
		CreatedAt:                createdAt,
		StartedAt:                row.StartTime, // When query execution started
		FinishedAt:               row.EndTime,   // When query execution finished
		QueryID:                  strconv.FormatInt(row.QueryId, 10),
		SQL:                      queryText,
		NormalizedQueryHash:      normalizedQueryHash,
		SessionID:                sessionID,
		SqlDialect:               sqlDialect,
		DwhContext:               dwhContext,
		QueryType:                queryType,
		Status:                   status,
		Metadata:                 querylogs.NewMetadataStruct(metadata),
		SqlObfuscationMode:       obfuscator.Mode(),
		HasCompleteNativeLineage: false, // Redshift doesn't provide lineage in SYS_QUERY_HISTORY
		IsTruncated:              isTruncated,
		NativeLineage:            nil,
	}, nil
}
