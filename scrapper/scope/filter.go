package scope

import "github.com/getsynq/dwhsupport/scrapper"

// FilterRows filters rows that implement HasTableFqn using the given ScopeFilter.
// Returns all rows unchanged if filter is nil.
func FilterRows[T scrapper.HasTableFqn](rows []T, filter *ScopeFilter) []T {
	if filter == nil {
		return rows
	}
	result := make([]T, 0, len(rows))
	for _, row := range rows {
		if filter.IsFqnAccepted(row.TableFqn()) {
			result = append(result, row)
		}
	}
	return result
}

// FilterDatabaseRows filters DatabaseRow entries using the given ScopeFilter.
// DatabaseRow doesn't implement HasTableFqn, so this uses IsDatabaseAccepted.
// Returns all rows unchanged if filter is nil.
func FilterDatabaseRows(rows []*scrapper.DatabaseRow, filter *ScopeFilter) []*scrapper.DatabaseRow {
	if filter == nil {
		return rows
	}
	result := make([]*scrapper.DatabaseRow, 0, len(rows))
	for _, row := range rows {
		if filter.IsDatabaseAccepted(row.Database) {
			result = append(result, row)
		}
	}
	return result
}

// FilterSchemaRows filters SchemaRow entries using the given ScopeFilter.
// Schemas are matched with IsSchemaAccepted (conservative partial evaluation at
// the schema level), not IsObjectAccepted, because a SchemaRow has no table.
// Returns all rows unchanged if filter is nil.
func FilterSchemaRows(rows []*scrapper.SchemaRow, filter *ScopeFilter) []*scrapper.SchemaRow {
	if filter == nil {
		return rows
	}
	result := make([]*scrapper.SchemaRow, 0, len(rows))
	for _, row := range rows {
		if filter.IsSchemaAccepted(row.Database, row.Schema) {
			result = append(result, row)
		}
	}
	return result
}
