package scope

import "github.com/getsynq/dwhsupport/scrapper"

// ScopeRule is a multi-level pattern. All non-empty fields must match for the rule to apply.
// Empty field = match anything at that level.
// Patterns support glob syntax where * matches zero or more characters.
type ScopeRule struct {
	Database string `yaml:"database,omitempty" json:"database,omitempty"`
	Schema   string `yaml:"schema,omitempty"   json:"schema,omitempty"`
	Table    string `yaml:"table,omitempty"    json:"table,omitempty"`
}

// ScopeFilter defines include/exclude rules for scoping scrapper queries.
//
// Matching semantics:
//   - If Include is non-empty, the tuple (database, schema, table) must match at least one include rule.
//   - If Exclude is non-empty, the tuple must NOT match any exclude rule.
//   - Exclude takes precedence over include (exclude wins).
//   - Empty field in a rule = wildcard (matches anything at that level).
//   - nil ScopeFilter = accept all (all methods return true on nil receiver).
type ScopeFilter struct {
	Include []ScopeRule `yaml:"include,omitempty" json:"include,omitempty"`
	Exclude []ScopeRule `yaml:"exclude,omitempty" json:"exclude,omitempty"`

	// children is set by Merge to compose multiple filters with AND semantics.
	// When set, Include/Exclude are ignored and all matching is delegated to children.
	children []*ScopeFilter
}

// IsObjectAccepted checks if a (database, schema, table) tuple passes the filter.
// All three levels are evaluated against all rules.
// Returns true on nil receiver.
func (f *ScopeFilter) IsObjectAccepted(database, schema, table string) bool {
	if f == nil {
		return true
	}
	if len(f.children) > 0 {
		for _, child := range f.children {
			if !child.IsObjectAccepted(database, schema, table) {
				return false
			}
		}
		return true
	}
	if f.matchesAnyExclude(database, schema, table) {
		return false
	}
	if len(f.Include) > 0 {
		return f.matchesAnyInclude(database, schema, table)
	}
	return true
}

// IsDatabaseAccepted performs conservative partial evaluation at the database level.
// Returns false only when it can definitively say no tuple (db, *, *) would ever pass.
// Returns true (conservative) when uncertain.
// Returns true on nil receiver.
func (f *ScopeFilter) IsDatabaseAccepted(database string) bool {
	if f == nil {
		return true
	}
	if len(f.children) > 0 {
		for _, child := range f.children {
			if !child.IsDatabaseAccepted(database) {
				return false
			}
		}
		return true
	}

	// Check excludes: only reject if an exclude rule matches this database
	// with no schema/table constraints (excluding everything in that db).
	for _, rule := range f.Exclude {
		if rule.Schema == "" && rule.Table == "" && matchPattern(rule.Database, database) {
			return false
		}
	}

	// Check includes: reject only if all include rules have database patterns
	// and none of them match this database.
	if len(f.Include) > 0 {
		for _, rule := range f.Include {
			if rule.Database == "" || matchPattern(rule.Database, database) {
				return true
			}
		}
		return false
	}

	return true
}

// IsSchemaAccepted performs conservative partial evaluation at the schema level.
// Returns false only when it can definitively say no tuple (db, schema, *) would ever pass.
// Returns true (conservative) when uncertain.
// Returns true on nil receiver.
func (f *ScopeFilter) IsSchemaAccepted(database, schema string) bool {
	if f == nil {
		return true
	}
	if len(f.children) > 0 {
		for _, child := range f.children {
			if !child.IsSchemaAccepted(database, schema) {
				return false
			}
		}
		return true
	}

	// Check excludes: only reject if an exclude rule matches this database+schema
	// with no table constraint.
	for _, rule := range f.Exclude {
		if rule.Table == "" &&
			(rule.Database == "" || matchPattern(rule.Database, database)) &&
			(rule.Schema == "" || matchPattern(rule.Schema, schema)) {
			// Fully matches db+schema with no table constraint — definitively excluded.
			// But only if the rule actually constrains something (not all-empty).
			if rule.Database != "" || rule.Schema != "" {
				return false
			}
		}
	}

	// Check includes: reject only if no include rule can possibly match (db, schema, *).
	if len(f.Include) > 0 {
		for _, rule := range f.Include {
			dbMatch := rule.Database == "" || matchPattern(rule.Database, database)
			schemaMatch := rule.Schema == "" || matchPattern(rule.Schema, schema)
			if dbMatch && schemaMatch {
				return true
			}
		}
		return false
	}

	return true
}

// IsFqnAccepted is a convenience wrapper for IsObjectAccepted using a DwhFqn.
// Returns true on nil receiver.
func (f *ScopeFilter) IsFqnAccepted(fqn scrapper.DwhFqn) bool {
	return f.IsObjectAccepted(fqn.DatabaseName, fqn.SchemaName, fqn.ObjectName)
}

// matchesAnyExclude returns true if the tuple matches any exclude rule.
func (f *ScopeFilter) matchesAnyExclude(database, schema, table string) bool {
	for _, rule := range f.Exclude {
		if ruleMatches(rule, database, schema, table) {
			return true
		}
	}
	return false
}

// matchesAnyInclude returns true if the tuple matches any include rule.
func (f *ScopeFilter) matchesAnyInclude(database, schema, table string) bool {
	for _, rule := range f.Include {
		if ruleMatches(rule, database, schema, table) {
			return true
		}
	}
	return false
}

// ruleMatches checks if all non-empty fields in the rule match the corresponding values.
func ruleMatches(rule ScopeRule, database, schema, table string) bool {
	if rule.Database != "" && !matchPattern(rule.Database, database) {
		return false
	}
	if rule.Schema != "" && !matchPattern(rule.Schema, schema) {
		return false
	}
	if rule.Table != "" && !matchPattern(rule.Table, table) {
		return false
	}
	return true
}
