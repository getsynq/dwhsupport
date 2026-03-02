package scope

import (
	"context"
	"strings"
)

// ScopeFilterPlaceholder is the marker placed in SQL queries at the exact point
// where scope filter conditions should be injected. It must appear inside an
// existing WHERE clause. When scope filtering is active it gets replaced with
// "AND <conditions>"; when inactive it is replaced with an empty string.
const ScopeFilterPlaceholder = "/* SYNQ_SCOPE_FILTER */"

// AppendScopeConditions replaces the ScopeFilterPlaceholder in a SQL query with
// scope filter conditions derived from the context.
//
// schemaCol and tableCol are the column expressions in the SQL query.
// dbCol can be empty if the query has no database column.
//
// Returns the original SQL unchanged if no scope is set or no conditions apply.
func AppendScopeConditions(ctx context.Context, sql, dbCol, schemaCol, tableCol string) string {
	filter := GetScope(ctx)
	if filter == nil {
		return strings.ReplaceAll(sql, ScopeFilterPlaceholder, "")
	}

	cond := filter.InlineTableSQL(dbCol, schemaCol, tableCol)
	if cond == "" {
		return strings.ReplaceAll(sql, ScopeFilterPlaceholder, "")
	}

	return strings.ReplaceAll(sql, ScopeFilterPlaceholder, "AND "+cond)
}

// AppendSchemaScopeConditions is like AppendScopeConditions but only generates
// schema-level conditions (no table filtering). Useful for queries that don't
// have table-level columns.
func AppendSchemaScopeConditions(ctx context.Context, sql, dbCol, schemaCol string) string {
	filter := GetScope(ctx)
	if filter == nil {
		return strings.ReplaceAll(sql, ScopeFilterPlaceholder, "")
	}

	cond := filter.InlineSchemaSQL(dbCol, schemaCol)
	if cond == "" {
		return strings.ReplaceAll(sql, ScopeFilterPlaceholder, "")
	}

	return strings.ReplaceAll(sql, ScopeFilterPlaceholder, "AND "+cond)
}
