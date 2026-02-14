package querycontext

import (
	"context"
	"encoding/json"
	"regexp"
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
func (qc QueryContext) FormatAsSQLComment() string {
	j := qc.FormatAsJSON()
	if j == "" {
		return ""
	}
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

// BigQuery label keys must contain only lowercase letters, numeric characters,
// underscores, and dashes. Keys must start with a letter. Max 63 chars for both key and value.
var bigQueryLabelKeyRegexp = regexp.MustCompile(`[^a-z0-9_-]`)

// AsBigQueryLabels converts the query context to BigQuery-compatible job labels.
// Keys are lowercased and sanitized (non-alphanumeric/underscore/dash replaced with underscore).
// Keys and values are truncated to 63 characters.
func (qc QueryContext) AsBigQueryLabels() map[string]string {
	if len(qc) == 0 {
		return nil
	}
	labels := make(map[string]string, len(qc))
	for k, v := range qc {
		key := strings.ToLower(k)
		key = bigQueryLabelKeyRegexp.ReplaceAllString(key, "_")
		if len(key) == 0 {
			continue
		}
		// Keys must start with a letter
		if key[0] < 'a' || key[0] > 'z' {
			key = "l_" + key
		}
		if len(key) > 63 {
			key = key[:63]
		}
		if len(v) > 63 {
			v = v[:63]
		}
		labels[key] = v
	}
	return labels
}
