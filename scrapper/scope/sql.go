package scope

import (
	"fmt"
	"strings"
)

// DatabaseSQL generates a SQL WHERE condition fragment for database-level filtering.
// Returns empty string and nil args if no applicable rules exist.
// The condition uses parameterized placeholders (%s for column names, values in args).
func (f *ScopeFilter) DatabaseSQL(dbCol string) (string, []any) {
	if f == nil {
		return "", nil
	}
	if len(f.children) > 0 {
		return f.childrenSQL(func(child *ScopeFilter) (string, []any) {
			return child.DatabaseSQL(dbCol)
		})
	}
	return f.buildSingleLevelSQL(dbCol, f.databaseRules())
}

// SchemaSQL generates a SQL WHERE condition fragment for schema-level filtering.
// Handles cross-level rules that involve both database and schema.
// Returns empty string and nil args if no applicable rules exist.
func (f *ScopeFilter) SchemaSQL(dbCol, schemaCol string) (string, []any) {
	if f == nil {
		return "", nil
	}
	if len(f.children) > 0 {
		return f.childrenSQL(func(child *ScopeFilter) (string, []any) {
			return child.SchemaSQL(dbCol, schemaCol)
		})
	}
	return f.buildMultiLevelSQL(
		[]levelSpec{{col: dbCol}, {col: schemaCol}},
		f.schemaExcludeConditions(dbCol, schemaCol),
	)
}

// TableSQL generates a SQL WHERE condition fragment for table-level filtering.
// Handles cross-level rules that involve database, schema, and table.
// Returns empty string and nil args if no applicable rules exist.
func (f *ScopeFilter) TableSQL(dbCol, schemaCol, tableCol string) (string, []any) {
	if f == nil {
		return "", nil
	}
	if len(f.children) > 0 {
		return f.childrenSQL(func(child *ScopeFilter) (string, []any) {
			return child.TableSQL(dbCol, schemaCol, tableCol)
		})
	}
	return f.buildMultiLevelSQL(
		[]levelSpec{{col: dbCol}, {col: schemaCol}, {col: tableCol}},
		f.tableExcludeConditions(dbCol, schemaCol, tableCol),
	)
}

func (f *ScopeFilter) childrenSQL(fn func(*ScopeFilter) (string, []any)) (string, []any) {
	var conditions []string
	var allArgs []any
	for _, child := range f.children {
		cond, args := fn(child)
		if cond != "" {
			conditions = append(conditions, cond)
			allArgs = append(allArgs, args...)
		}
	}
	if len(conditions) == 0 {
		return "", nil
	}
	if len(conditions) == 1 {
		return conditions[0], allArgs
	}
	return "(" + strings.Join(conditions, " AND ") + ")", allArgs
}

type levelSpec struct {
	col string
}

// databaseRules extracts database patterns from include/exclude rules that only have database constraints.
type singleLevelRules struct {
	includePatterns []string
	excludePatterns []string
}

func (f *ScopeFilter) databaseRules() singleLevelRules {
	var includes, excludes []string
	for _, rule := range f.Include {
		if rule.Database != "" {
			includes = append(includes, rule.Database)
		}
	}
	for _, rule := range f.Exclude {
		if rule.Database != "" && rule.Schema == "" && rule.Table == "" {
			excludes = append(excludes, rule.Database)
		}
	}
	return singleLevelRules{includePatterns: includes, excludePatterns: excludes}
}

func (f *ScopeFilter) buildSingleLevelSQL(col string, rules singleLevelRules) (string, []any) {
	var conditions []string
	var args []any

	if len(rules.includePatterns) > 0 {
		cond, a := patternsToSQL(col, rules.includePatterns, false)
		if cond != "" {
			conditions = append(conditions, cond)
			args = append(args, a...)
		}
	}

	if len(rules.excludePatterns) > 0 {
		cond, a := patternsToSQL(col, rules.excludePatterns, true)
		if cond != "" {
			conditions = append(conditions, cond)
			args = append(args, a...)
		}
	}

	if len(conditions) == 0 {
		return "", nil
	}
	if len(conditions) == 1 {
		return conditions[0], args
	}
	return "(" + strings.Join(conditions, " AND ") + ")", args
}

func (f *ScopeFilter) buildMultiLevelSQL(levels []levelSpec, excludeConditions []excludeCondition) (string, []any) {
	var conditions []string
	var args []any

	// Include: for each level that has patterns, generate an include condition.
	// The include rules use OR semantics within each rule, and the full include
	// is a disjunction of all include rules.
	if len(f.Include) > 0 {
		includeSQL, includeArgs := f.includeRulesSQL(levels)
		if includeSQL != "" {
			conditions = append(conditions, includeSQL)
			args = append(args, includeArgs...)
		}
	}

	// Exclude conditions from cross-level rules.
	for _, ec := range excludeConditions {
		conditions = append(conditions, ec.sql)
		args = append(args, ec.args...)
	}

	if len(conditions) == 0 {
		return "", nil
	}
	if len(conditions) == 1 {
		return conditions[0], args
	}
	return "(" + strings.Join(conditions, " AND ") + ")", args
}

func (f *ScopeFilter) includeRulesSQL(levels []levelSpec) (string, []any) {
	var ruleSQLs []string
	var allArgs []any

	for _, rule := range f.Include {
		rulePatterns := []struct {
			col     string
			pattern string
		}{}

		for i, level := range levels {
			var pattern string
			switch i {
			case 0:
				pattern = rule.Database
			case 1:
				pattern = rule.Schema
			case 2:
				pattern = rule.Table
			}
			if pattern != "" {
				rulePatterns = append(rulePatterns, struct {
					col     string
					pattern string
				}{col: level.col, pattern: pattern})
			}
		}

		if len(rulePatterns) == 0 {
			// Rule matches everything — no SQL needed for this rule.
			return "", nil
		}

		var parts []string
		for _, rp := range rulePatterns {
			part, a := singlePatternSQL(rp.col, rp.pattern, false)
			parts = append(parts, part)
			allArgs = append(allArgs, a...)
		}

		if len(parts) == 1 {
			ruleSQLs = append(ruleSQLs, parts[0])
		} else {
			ruleSQLs = append(ruleSQLs, "("+strings.Join(parts, " AND ")+")")
		}
	}

	if len(ruleSQLs) == 0 {
		return "", nil
	}
	if len(ruleSQLs) == 1 {
		return ruleSQLs[0], allArgs
	}
	return "(" + strings.Join(ruleSQLs, " OR ") + ")", allArgs
}

type excludeCondition struct {
	sql  string
	args []any
}

func (f *ScopeFilter) schemaExcludeConditions(dbCol, schemaCol string) []excludeCondition {
	var result []excludeCondition
	for _, rule := range f.Exclude {
		// Skip rules with table constraints — they can't be pushed down at schema level.
		if rule.Table != "" {
			continue
		}
		if rule.Database == "" && rule.Schema == "" {
			continue
		}

		var parts []string
		var args []any
		if rule.Database != "" {
			part, a := singlePatternSQL(dbCol, rule.Database, false)
			parts = append(parts, part)
			args = append(args, a...)
		}
		if rule.Schema != "" {
			part, a := singlePatternSQL(schemaCol, rule.Schema, false)
			parts = append(parts, part)
			args = append(args, a...)
		}

		cond := strings.Join(parts, " AND ")
		if len(parts) > 1 {
			cond = "(" + cond + ")"
		}
		result = append(result, excludeCondition{sql: "NOT " + cond, args: args})
	}
	return result
}

func (f *ScopeFilter) tableExcludeConditions(dbCol, schemaCol, tableCol string) []excludeCondition {
	var result []excludeCondition
	for _, rule := range f.Exclude {
		if rule.Database == "" && rule.Schema == "" && rule.Table == "" {
			continue
		}

		var parts []string
		var args []any
		if rule.Database != "" {
			part, a := singlePatternSQL(dbCol, rule.Database, false)
			parts = append(parts, part)
			args = append(args, a...)
		}
		if rule.Schema != "" {
			part, a := singlePatternSQL(schemaCol, rule.Schema, false)
			parts = append(parts, part)
			args = append(args, a...)
		}
		if rule.Table != "" {
			part, a := singlePatternSQL(tableCol, rule.Table, false)
			parts = append(parts, part)
			args = append(args, a...)
		}

		cond := strings.Join(parts, " AND ")
		if len(parts) > 1 {
			cond = "(" + cond + ")"
		}
		result = append(result, excludeCondition{sql: "NOT " + cond, args: args})
	}
	return result
}

// patternsToSQL converts a list of patterns for a single column to SQL.
// If negate is true, generates NOT conditions.
func patternsToSQL(col string, patterns []string, negate bool) (string, []any) {
	if len(patterns) == 0 {
		return "", nil
	}

	var exact []string
	var wildcardParts []string
	var args []any

	for _, p := range patterns {
		if hasWildcard(p) {
			sqlLike := globToSQLLike(p)
			if negate {
				wildcardParts = append(wildcardParts, fmt.Sprintf("LOWER(%s) NOT LIKE ?", col))
			} else {
				wildcardParts = append(wildcardParts, fmt.Sprintf("LOWER(%s) LIKE ?", col))
			}
			args = append(args, strings.ToLower(sqlLike))
		} else {
			exact = append(exact, p)
		}
	}

	var conditions []string

	if len(exact) > 0 {
		placeholders := make([]string, len(exact))
		for i := range exact {
			placeholders[i] = "?"
		}
		if negate {
			conditions = append(conditions, fmt.Sprintf("LOWER(%s) NOT IN (%s)", col, strings.Join(placeholders, ", ")))
		} else {
			conditions = append(conditions, fmt.Sprintf("LOWER(%s) IN (%s)", col, strings.Join(placeholders, ", ")))
		}
		for _, e := range exact {
			args = append(args, strings.ToLower(e))
		}
	}

	conditions = append(conditions, wildcardParts...)

	if len(conditions) == 0 {
		return "", nil
	}

	if negate {
		// All exclude conditions must hold (AND).
		if len(conditions) == 1 {
			return conditions[0], args
		}
		return "(" + strings.Join(conditions, " AND ") + ")", args
	}

	// All include conditions use OR.
	if len(conditions) == 1 {
		return conditions[0], args
	}
	return "(" + strings.Join(conditions, " OR ") + ")", args
}

// singlePatternSQL generates SQL for a single pattern against a column.
func singlePatternSQL(col, pattern string, negate bool) (string, []any) {
	if hasWildcard(pattern) {
		sqlLike := globToSQLLike(pattern)
		if negate {
			return fmt.Sprintf("LOWER(%s) NOT LIKE ?", col), []any{strings.ToLower(sqlLike)}
		}
		return fmt.Sprintf("LOWER(%s) LIKE ?", col), []any{strings.ToLower(sqlLike)}
	}
	if negate {
		return fmt.Sprintf("LOWER(%s) != ?", col), []any{strings.ToLower(pattern)}
	}
	return fmt.Sprintf("LOWER(%s) = ?", col), []any{strings.ToLower(pattern)}
}

// globToSQLLike converts a glob pattern to SQL LIKE syntax.
// * → %, and SQL special characters (%, _) are escaped.
func globToSQLLike(pattern string) string {
	// First escape SQL LIKE special chars that aren't our glob wildcard.
	result := strings.ReplaceAll(pattern, "%", "\\%")
	result = strings.ReplaceAll(result, "_", "\\_")
	// Then convert glob * to SQL %.
	result = strings.ReplaceAll(result, "*", "%")
	return result
}
