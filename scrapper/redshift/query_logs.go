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

	// queryLogsSql reads SYS_QUERY_HISTORY together with each statement's user name and, for one
	// longer than a chunk, its SYS_QUERY_TEXT chunks.
	queryLogsSql = strings.Replace(queryLogsTemplate, "/* SYS_QUERY_HISTORY */", queryLogsHistorySql, 1)
)

// queryTextChunkMinBytes is the shortest query_text, as storedChunkBytes counts it, that may be only
// the start of a statement, for a cluster whose SYS_QUERY_TEXT cannot be read.
//
// SYS_QUERY_HISTORY.query_text is the first chunk of the statement's text, and SYS_QUERY_TEXT holds
// every chunk, keyed by query_id and sequence. A chunk holds at most 4000 bytes as Redshift counts them,
// which is each pipe twice: a synq-recon row hash, three pipes to a column, is cut into chunks of about
// 3660 bytes. Redshift also ends a chunk a few bytes short of that. So the length of query_text cannot
// tell a cut statement from a complete one, and where SYS_QUERY_TEXT can be read a second chunk there
// decides. Where it cannot, a query_text of at least this many bytes is taken as cut, and so is one that
// ends inside a quote (endsInsideQuote).
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
	rows, err := query(ctx, queryLogsSql, from, to)
	textJoined := true
	if err != nil && canReadHistoryAlone(err) {
		// A cluster that cannot read SYS_QUERY_TEXT or pg_user still has its query history. Its logs
		// carry no user name, and a statement that may have been cut is flagged as truncated.
		logging.GetLogger(ctx).WithError(err).
			Warn("Redshift query logs: reading SYS_QUERY_HISTORY without SYS_QUERY_TEXT and pg_user")
		rows, err = query(ctx, queryLogsHistorySql, from, to)
		textJoined = false
	}
	if err != nil {
		return nil, err
	}

	return &queryLogIterator{
		rows: rows,
		convert: func(row *RedshiftQueryLogSchema, chunks []queryTextChunk) (*querylogs.QueryLog, error) {
			return convertRedshiftRowToQueryLog(row, chunks, textJoined, obfuscator, sqlDialect, host, database)
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

// queryLogIterator turns the rows of query_logs.sql into one QueryLog per statement. A statement longer
// than one chunk arrives as one row per SYS_QUERY_TEXT chunk, next to each other and in
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
// textJoined says the rows came from query_logs.sql, which joins the SYS_QUERY_TEXT chunks of every
// statement that has more than one, so a statement without chunks is whole in query_text, unless it
// ends inside a quote: no statement that ran does, so its other chunks were not there to join. Without
// the join (query_logs_history.sql), query_text is taken as complete only when it cannot have been cut
// (queryTextMayBeCut).
//
// The chunks are joined before unescaping, because a chunk can end in the middle of an escape
// sequence, or of a multi-byte character.
func statementText(row *RedshiftQueryLogSchema, chunks []queryTextChunk, textJoined bool) (string, bool) {
	historyText := ""
	if row.QueryText != nil {
		historyText = *row.QueryText
	}
	if len(chunks) == 0 {
		text := unescapeQueryText(historyText)
		if textJoined {
			return text, endsInsideQuote(text)
		}
		return text, queryTextMayBeCut(historyText)
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
		// SYS_QUERY_TEXT has no first chunk, so query_text is all there is.
		return unescapeQueryText(historyText), true
	}
	return unescapeQueryText(text.String()), contiguous < len(chunks)
}

// queryTextMayBeCut reports whether a stored query_text may be only the first chunk of a longer
// statement: it is close to a full chunk as Redshift counts one, or it ends inside a quote, which a
// statement that ran does not.
func queryTextMayBeCut(stored string) bool {
	return storedChunkBytes(stored) >= queryTextChunkMinBytes || endsInsideQuote(unescapeQueryText(stored))
}

// storedChunkBytes is the length of a stored chunk as Redshift counts it towards the 4000 bytes a chunk
// holds: each pipe counts twice. Every full chunk of SYS_QUERY_TEXT measured comes to 3999 or 4000.
func storedChunkBytes(stored string) int {
	return len(stored) + strings.Count(stored, "|")
}

// endsInsideQuote reports whether text stops inside a string literal, a quoted identifier, a
// dollar-quoted string or a block comment.
func endsInsideQuote(text string) bool {
	for i := 0; i < len(text); i++ {
		switch text[i] {
		case '\'':
			// A doubled quote and a backslash escape both stay inside the literal.
			j := i + 1
			for ; j < len(text); j++ {
				if text[j] == '\\' {
					j++
					continue
				}
				if text[j] == '\'' {
					if j+1 < len(text) && text[j+1] == '\'' {
						j++
						continue
					}
					break
				}
			}
			if j >= len(text) {
				return true
			}
			i = j
		case '"':
			// A doubled quote inside an identifier closes it and opens it again.
			end := strings.IndexByte(text[i+1:], '"')
			if end < 0 {
				return true
			}
			i += end + 1
		case '-':
			if strings.HasPrefix(text[i:], "--") {
				end := strings.IndexByte(text[i:], '\n')
				if end < 0 {
					return false
				}
				i += end
			}
		case '/':
			if strings.HasPrefix(text[i:], "/*") {
				end := strings.Index(text[i+2:], "*/")
				if end < 0 {
					return true
				}
				i += end + 3
			}
		case '$':
			if tag := dollarQuoteTag(text[i:]); tag != "" {
				end := strings.Index(text[i+len(tag):], tag)
				if end < 0 {
					return true
				}
				i += 2*len(tag) + end - 1
			}
		}
	}
	return false
}

// dollarQuoteTag returns the tag that opens a dollar-quoted string at the start of s, $$ or $name$, or
// "" when s does not start one: a positional parameter such as $1 is not a tag.
func dollarQuoteTag(s string) string {
	for i := 1; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '$':
			return s[:i+1]
		case c == '_', 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', c >= 0x80:
		case '0' <= c && c <= '9' && i > 1:
		default:
			return ""
		}
	}
	return ""
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
	textJoined bool,
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
	queryText, isTruncated := statementText(row, chunks, textJoined)
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
