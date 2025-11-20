package querylogs

import (
	"strings"
)

// ObfuscatorOption configures the SQL obfuscator behavior.
// These options control what gets replaced during obfuscation.
type ObfuscatorOption func(*obfuscatorConfig)

// WithKeepJsonPath preserves JSON path expressions in SQL (e.g., $.field).
// When true, expressions like `data->>'$.field'` keep the JSON path intact.
// Default: true (keep JSON paths to preserve query structure)
func WithKeepJsonPath(keep bool) ObfuscatorOption {
	return func(c *obfuscatorConfig) {
		c.KeepJsonPath = keep
	}
}

// WithPreserveNumbers controls whether numeric literals (like 123, 45.67) are kept unchanged
// or replaced with placeholders during obfuscation.
//
// When true:  "WHERE id = 123 AND score > 98.5" stays as "WHERE id = 123 AND score > 98.5"
// When false: "WHERE id = 123 AND score > 98.5" becomes "WHERE id = ? AND score > ?"
//
// IMPORTANT: This option ONLY affects numeric literals (NUMBER tokens).
// Digits in identifiers (table1, col2, schema3) are ALWAYS preserved regardless of this setting.
//
// This is useful when you want to see actual numeric values in query logs for debugging,
// while still obfuscating sensitive string data like names, emails, etc.
//
// Default: false (numeric literals are replaced with ?)
func WithPreserveNumbers(preserve bool) ObfuscatorOption {
	return func(c *obfuscatorConfig) {
		c.PreserveNumbers = preserve
	}
}

// WithPreserveLiteralsMatching preserves string and numeric literals whose full content
// matches any of the provided regex patterns. Matched literals are kept unchanged in the
// obfuscated SQL, while non-matching literals are replaced with placeholders.
//
// Patterns are compiled into a single efficient regex at obfuscator creation time.
// This is useful for preserving dates, timestamps, UUIDs, or other structured data that
// doesn't contain sensitive information but is valuable for debugging and analysis.
//
// Example patterns:
//   - Date: `^\d{4}-\d{2}-\d{2}$` (matches '2023-10-01')
//   - Timestamp: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$` (matches '2023-10-01 12:34:56')
//   - UUID: `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$` (matches UUID strings)
//
// Example:
//   SQL: "WHERE created_at > '2023-10-01' AND email = 'user@example.com'"
//   With date pattern: "WHERE created_at > '2023-10-01' AND email = ?"
//
// Default: no patterns (all literals are replaced)
func WithPreserveLiteralsMatching(patterns []string) ObfuscatorOption {
	return func(c *obfuscatorConfig) {
		c.PreserveLiteralPatterns = patterns
	}
}

// queryObfuscatorImpl implements QueryObfuscator using local sqllexer obfuscator
type queryObfuscatorImpl struct {
	mode          ObfuscationMode
	sqlObfuscator *sqlObfuscator
}

// NewQueryObfuscator creates a SQL obfuscator with the specified mode and options.
// Mode is a required parameter that determines the obfuscation behavior.
//
// Returns an error if any regex patterns in WithPreserveLiteralsMatching are invalid.
//
// Modes:
//   - ObfuscationNone: No obfuscation (returns SQL unchanged)
//   - ObfuscationRedactLiterals: Replaces string and numeric literals with placeholders
//   - ObfuscationRemoveQuery: Removes the entire SQL query (future feature)
//
// Default behavior for ObfuscationRedactLiterals:
//   - Replaces string and numeric literals with placeholders (e.g., 'value' -> ?, 123 -> ?)
//   - Preserves digits in identifiers (table1, col2, etc. stay unchanged)
//   - Preserves JSON paths ($.field) by default
//
// Example for cloud service (no obfuscation):
//   obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
//
// Example for on-premise (redact literals):
//   obfuscator, err := querylogs.NewQueryObfuscator(
//       querylogs.ObfuscationRedactLiterals,
//       querylogs.WithKeepJsonPath(true),
//   )
func NewQueryObfuscator(mode ObfuscationMode, opts ...ObfuscatorOption) (QueryObfuscator, error) {
	var sqlObf *sqlObfuscator
	if mode != ObfuscationNone {
		var err error
		sqlObf, err = newSqlObfuscator(opts...)
		if err != nil {
			return nil, err
		}
	}

	return &queryObfuscatorImpl{
		mode:          mode,
		sqlObfuscator: sqlObf,
	}, nil
}

func (o *queryObfuscatorImpl) Mode() ObfuscationMode {
	return o.mode
}

func (o *queryObfuscatorImpl) Obfuscate(sql string) string {
	// Return early for empty or whitespace-only strings
	if len(strings.TrimSpace(sql)) == 0 {
		return sql
	}

	if o.mode == ObfuscationNone {
		return sql
	}

	return o.sqlObfuscator.Obfuscate(sql)
}
