package querycontext

import (
	"context"
	"encoding/json"
	"strings"
)

// QueryContext holds key-value metadata to attach to database queries.
// It is used to track query provenance (e.g., originating from SYNQ,
// associated monitor or model) across all supported data warehouses.
type QueryContext map[string]string

type queryContextKey struct{}

// WithQueryContext attaches query context metadata to a Go context.
func WithQueryContext(ctx context.Context, qc QueryContext) context.Context {
	return context.WithValue(ctx, queryContextKey{}, qc)
}

// GetQueryContext retrieves query context from a Go context. Returns nil if not set.
func GetQueryContext(ctx context.Context) QueryContext {
	qc, _ := ctx.Value(queryContextKey{}).(QueryContext)
	return qc
}

// FormatAsJSON returns the query context as a JSON string.
func (qc QueryContext) FormatAsJSON() string {
	if len(qc) == 0 {
		return ""
	}
	b, err := json.Marshal(map[string]string(qc))
	if err != nil {
		return ""
	}
	return string(b)
}

// FormatAsSQLComment returns the query context as a SQL block comment
// suitable for appending to a query string: /* {"key":"value"} */
//
// The JSON payload is sanitized to prevent SQL injection: any occurrence of
// the comment-close sequence "*/" is neutralised by replacing "/" with its
// JSON Unicode escape "\u002f". The result is still valid, parseable JSON
// but cannot prematurely close the SQL block comment.
func (qc QueryContext) FormatAsSQLComment() string {
	j := qc.FormatAsJSON()
	if j == "" {
		return ""
	}
	// Prevent comment-close injection: escape "/" as \u002f inside "*/" sequences.
	j = strings.ReplaceAll(j, "*/", "*\\u002f")
	return " /* " + j + " */"
}

// AppendSQLComment appends a query context SQL comment to the given SQL string
// if query context is present in the context. Returns the original SQL unchanged
// if no query context is set.
func AppendSQLComment(ctx context.Context, sql string) string {
	qc := GetQueryContext(ctx)
	if qc == nil {
		return sql
	}
	comment := qc.FormatAsSQLComment()
	if comment == "" {
		return sql
	}
	return strings.TrimRight(sql, " \t\n\r;") + comment
}
