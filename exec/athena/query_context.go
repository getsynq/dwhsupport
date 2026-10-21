package athena

import (
	"context"
	"strings"
	"unicode"

	"github.com/getsynq/dwhsupport/exec/querycontext"
)

// hiveStatements are the leading keywords of the statements Athena runs on its
// Hive engine (DDL) rather than on Trino. Hive's parser rejects a block comment
// after the statement, "FAILED: ParseException line 1:55 missing EOF at '/'",
// and takes a line comment.
var hiveStatements = map[string]bool{
	"SHOW":     true,
	"DESCRIBE": true,
	"DESC":     true,
	"MSCK":     true,
	"ALTER":    true,
	"DROP":     true,
}

// withQueryContext marks a statement with the query context of ctx: as the
// block comment every other executor appends, or, for a statement Athena runs
// on Hive, as a line comment, which is the only form Hive accepts there.
func withQueryContext(ctx context.Context, sql string) string {
	sql = trimRightSemicolons(sql)
	if hiveStatements[leadingKeyword(sql)] {
		return querycontext.AppendSQLLineComment(ctx, sql)
	}
	return querycontext.AppendSQLComment(ctx, sql)
}

// leadingKeyword returns the first word of sql in upper case, after any
// whitespace and comments in front of it.
func leadingKeyword(sql string) string {
	for {
		sql = strings.TrimLeftFunc(sql, unicode.IsSpace)
		switch {
		case strings.HasPrefix(sql, "--"):
			_, rest, found := strings.Cut(sql, "\n")
			if !found {
				return ""
			}
			sql = rest
		case strings.HasPrefix(sql, "/*"):
			_, rest, found := strings.Cut(sql[len("/*"):], "*/")
			if !found {
				return ""
			}
			sql = rest
		default:
			end := strings.IndexFunc(sql, func(r rune) bool { return !unicode.IsLetter(r) })
			if end < 0 {
				end = len(sql)
			}
			return strings.ToUpper(sql[:end])
		}
	}
}
