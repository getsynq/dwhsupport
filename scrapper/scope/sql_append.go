package scope

import (
	"context"
	"strings"
)

// AppendScopeConditions appends scope filter conditions to a SQL query.
// It retrieves the scope filter from the context and generates inline WHERE
// conditions using the provided column mappings.
//
// The conditions are appended after the existing WHERE clause using AND.
// If the SQL does not contain WHERE, no conditions are added (to avoid
// breaking queries without WHERE clauses).
//
// schemaCol and tableCol are the column expressions in the SQL query.
// dbCol can be empty if the query has no database column.
//
// Returns the original SQL unchanged if no scope is set or no conditions apply.
func AppendScopeConditions(ctx context.Context, sql, dbCol, schemaCol, tableCol string) string {
	filter := GetScope(ctx)
	if filter == nil {
		return sql
	}

	cond := filter.InlineTableSQL(dbCol, schemaCol, tableCol)
	if cond == "" {
		return sql
	}

	return appendWhereCondition(sql, cond)
}

// AppendSchemaScopeConditions is like AppendScopeConditions but only generates
// schema-level conditions (no table filtering). Useful for queries that don't
// have table-level columns.
func AppendSchemaScopeConditions(ctx context.Context, sql, dbCol, schemaCol string) string {
	filter := GetScope(ctx)
	if filter == nil {
		return sql
	}

	cond := filter.InlineSchemaSQL(dbCol, schemaCol)
	if cond == "" {
		return sql
	}

	return appendWhereCondition(sql, cond)
}

// appendWhereCondition appends an AND condition to an existing SQL query.
// It looks for the last WHERE clause and appends the condition after any
// existing conditions. If no WHERE is found, the condition is not added.
func appendWhereCondition(sql, condition string) string {
	// Find the last occurrence of WHERE (case-insensitive) to handle CTEs and subqueries.
	upperSQL := strings.ToUpper(sql)

	// Look for WHERE in the main query (last occurrence handles CTEs).
	idx := strings.LastIndex(upperSQL, "WHERE")
	if idx == -1 {
		// No WHERE clause — don't modify the query.
		// The ScopedScrapper post-filter will handle filtering.
		return sql
	}

	// Find a good insertion point: before GROUP BY, ORDER BY, LIMIT, UNION, or end of query.
	afterWhere := sql[idx:]
	insertBefore := len(sql)

	for _, keyword := range []string{"GROUP BY", "ORDER BY", "LIMIT", "UNION", "HAVING"} {
		if pos := strings.Index(strings.ToUpper(afterWhere), keyword); pos != -1 {
			absPos := idx + pos
			if absPos < insertBefore {
				insertBefore = absPos
			}
		}
	}

	// Insert before any trailing whitespace/semicolons at the insertion point.
	before := strings.TrimRight(sql[:insertBefore], " \t\n\r")
	after := sql[insertBefore:]

	return before + "\n    AND " + condition + "\n" + after
}
