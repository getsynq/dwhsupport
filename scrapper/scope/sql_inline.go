package scope

import (
	"fmt"
	"strings"
)

// InlineDatabaseSQL generates an inline SQL WHERE fragment for database-level filtering.
// Unlike DatabaseSQL, this produces literal SQL with no placeholders, suitable for
// appending to any SQL dialect without needing parameterized query support.
// Returns empty string if no applicable rules exist.
func (f *ScopeFilter) InlineDatabaseSQL(dbCol string) string {
	if f == nil {
		return ""
	}
	if len(f.children) > 0 {
		return f.childrenInlineSQL(func(child *ScopeFilter) string {
			return child.InlineDatabaseSQL(dbCol)
		})
	}
	rules := f.databaseRules()
	return buildInlineSingleLevelSQL(dbCol, rules)
}

// InlineSchemaSQL generates an inline SQL WHERE fragment for schema-level filtering.
// Returns empty string if no applicable rules exist.
func (f *ScopeFilter) InlineSchemaSQL(dbCol, schemaCol string) string {
	if f == nil {
		return ""
	}
	if len(f.children) > 0 {
		return f.childrenInlineSQL(func(child *ScopeFilter) string {
			return child.InlineSchemaSQL(dbCol, schemaCol)
		})
	}

	var conditions []string

	// Include rules
	if len(f.Include) > 0 {
		if inc := f.inlineIncludeRulesSQL(dbCol, schemaCol, ""); inc != "" {
			conditions = append(conditions, inc)
		}
	}

	// Exclude rules (schema-level: skip rules with table constraints)
	for _, rule := range f.Exclude {
		if rule.Table != "" {
			continue
		}
		if rule.Database == "" && rule.Schema == "" {
			continue
		}
		if exc := inlineExcludeRuleSQL(rule, dbCol, schemaCol, ""); exc != "" {
			conditions = append(conditions, exc)
		}
	}

	return joinConditions(conditions, " AND ")
}

// InlineTableSQL generates an inline SQL WHERE fragment for table-level filtering.
// Returns empty string if no applicable rules exist.
func (f *ScopeFilter) InlineTableSQL(dbCol, schemaCol, tableCol string) string {
	if f == nil {
		return ""
	}
	if len(f.children) > 0 {
		return f.childrenInlineSQL(func(child *ScopeFilter) string {
			return child.InlineTableSQL(dbCol, schemaCol, tableCol)
		})
	}

	var conditions []string

	// Include rules
	if len(f.Include) > 0 {
		if inc := f.inlineIncludeRulesSQL(dbCol, schemaCol, tableCol); inc != "" {
			conditions = append(conditions, inc)
		}
	}

	// Exclude rules
	for _, rule := range f.Exclude {
		if rule.Database == "" && rule.Schema == "" && rule.Table == "" {
			continue
		}
		if exc := inlineExcludeRuleSQL(rule, dbCol, schemaCol, tableCol); exc != "" {
			conditions = append(conditions, exc)
		}
	}

	return joinConditions(conditions, " AND ")
}

func (f *ScopeFilter) childrenInlineSQL(fn func(*ScopeFilter) string) string {
	var conditions []string
	for _, child := range f.children {
		if cond := fn(child); cond != "" {
			conditions = append(conditions, cond)
		}
	}
	return joinConditions(conditions, " AND ")
}

func (f *ScopeFilter) inlineIncludeRulesSQL(dbCol, schemaCol, tableCol string) string {
	var ruleSQLs []string

	for _, rule := range f.Include {
		var parts []string
		if rule.Database != "" && dbCol != "" {
			parts = append(parts, inlinePatternSQL(dbCol, rule.Database, false))
		}
		if rule.Schema != "" && schemaCol != "" {
			parts = append(parts, inlinePatternSQL(schemaCol, rule.Schema, false))
		}
		if rule.Table != "" && tableCol != "" {
			parts = append(parts, inlinePatternSQL(tableCol, rule.Table, false))
		}
		if len(parts) == 0 {
			// Rule matches everything — no SQL needed.
			return ""
		}
		ruleSQLs = append(ruleSQLs, joinConditions(parts, " AND "))
	}

	if len(ruleSQLs) == 0 {
		return ""
	}
	return joinConditions(ruleSQLs, " OR ")
}

func inlineExcludeRuleSQL(rule ScopeRule, dbCol, schemaCol, tableCol string) string {
	var parts []string
	if rule.Database != "" && dbCol != "" {
		parts = append(parts, inlinePatternSQL(dbCol, rule.Database, false))
	}
	if rule.Schema != "" && schemaCol != "" {
		parts = append(parts, inlinePatternSQL(schemaCol, rule.Schema, false))
	}
	if rule.Table != "" && tableCol != "" {
		parts = append(parts, inlinePatternSQL(tableCol, rule.Table, false))
	}
	if len(parts) == 0 {
		return ""
	}
	inner := joinConditions(parts, " AND ")
	return "NOT " + inner
}

func buildInlineSingleLevelSQL(col string, rules singleLevelRules) string {
	var conditions []string

	if len(rules.includePatterns) > 0 {
		cond := inlinePatternsSQL(col, rules.includePatterns, false)
		if cond != "" {
			conditions = append(conditions, cond)
		}
	}

	if len(rules.excludePatterns) > 0 {
		cond := inlinePatternsSQL(col, rules.excludePatterns, true)
		if cond != "" {
			conditions = append(conditions, cond)
		}
	}

	return joinConditions(conditions, " AND ")
}

func inlinePatternsSQL(col string, patterns []string, negate bool) string {
	if len(patterns) == 0 {
		return ""
	}

	var exact []string
	var wildcardParts []string

	for _, p := range patterns {
		if hasWildcard(p) {
			sqlLike := globToSQLLike(p)
			if negate {
				wildcardParts = append(wildcardParts, fmt.Sprintf("LOWER(%s) NOT LIKE %s", col, quoteLiteral(strings.ToLower(sqlLike))))
			} else {
				wildcardParts = append(wildcardParts, fmt.Sprintf("LOWER(%s) LIKE %s", col, quoteLiteral(strings.ToLower(sqlLike))))
			}
		} else {
			exact = append(exact, p)
		}
	}

	var conditions []string

	if len(exact) > 0 {
		quoted := make([]string, len(exact))
		for i, e := range exact {
			quoted[i] = quoteLiteral(strings.ToLower(e))
		}
		if negate {
			conditions = append(conditions, fmt.Sprintf("LOWER(%s) NOT IN (%s)", col, strings.Join(quoted, ", ")))
		} else {
			conditions = append(conditions, fmt.Sprintf("LOWER(%s) IN (%s)", col, strings.Join(quoted, ", ")))
		}
	}

	conditions = append(conditions, wildcardParts...)

	if negate {
		return joinConditions(conditions, " AND ")
	}
	return joinConditions(conditions, " OR ")
}

func inlinePatternSQL(col, pattern string, negate bool) string {
	if hasWildcard(pattern) {
		sqlLike := globToSQLLike(pattern)
		if negate {
			return fmt.Sprintf("LOWER(%s) NOT LIKE %s", col, quoteLiteral(strings.ToLower(sqlLike)))
		}
		return fmt.Sprintf("LOWER(%s) LIKE %s", col, quoteLiteral(strings.ToLower(sqlLike)))
	}
	if negate {
		return fmt.Sprintf("LOWER(%s) != %s", col, quoteLiteral(strings.ToLower(pattern)))
	}
	return fmt.Sprintf("LOWER(%s) = %s", col, quoteLiteral(strings.ToLower(pattern)))
}

// quoteLiteral quotes a string for use in SQL, escaping single quotes.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func joinConditions(conditions []string, sep string) string {
	if len(conditions) == 0 {
		return ""
	}
	if len(conditions) == 1 {
		return conditions[0]
	}
	return "(" + strings.Join(conditions, sep) + ")"
}
