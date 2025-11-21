package sqldialect

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/lib/pq"
)

func PqQuoteIdentifierIfUpper(identifier string) string {
	if identifier == "" || IsLower(identifier) {
		return identifier
	}
	return pq.QuoteIdentifier(identifier)
}

func IsLower(s string) bool {
	for _, r := range s {
		if !unicode.IsLower(r) && unicode.IsLetter(r) {
			return false
		}
	}
	return true
}

func IsUpper(s string) bool {
	for _, r := range s {
		if !unicode.IsUpper(r) && unicode.IsLetter(r) {
			return false
		}
	}
	return true
}

// StandardSQLStringLiteral implements the standard SQL string literal escaping
// where single quotes are escaped by doubling them (”).
// This is used by most SQL dialects: Snowflake, BigQuery, Postgres, Redshift,
// Databricks, DuckDB, Trino, and MySQL.
func StandardSQLStringLiteral(s string) string {
	escaped := strings.ReplaceAll(s, "'", "''")
	return fmt.Sprintf("'%s'", escaped)
}
