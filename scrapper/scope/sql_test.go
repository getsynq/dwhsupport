package scope

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatabaseSQL_Nil(t *testing.T) {
	var f *ScopeFilter
	sql, args := f.DatabaseSQL("db")
	assert.Empty(t, sql)
	assert.Nil(t, args)
}

func TestDatabaseSQL_Empty(t *testing.T) {
	f := &ScopeFilter{}
	sql, args := f.DatabaseSQL("db")
	assert.Empty(t, sql)
	assert.Nil(t, args)
}

func TestDatabaseSQL_IncludeExact(t *testing.T) {
	f := &ScopeFilter{
		Include: []ScopeRule{
			{Database: "prod"},
			{Database: "analytics"},
		},
	}
	sql, args := f.DatabaseSQL("database_name")
	assert.Equal(t, "LOWER(database_name) IN (?, ?)", sql)
	assert.Equal(t, []any{"prod", "analytics"}, args)
}

func TestDatabaseSQL_IncludeWildcard(t *testing.T) {
	f := &ScopeFilter{
		Include: []ScopeRule{
			{Database: "prod_*"},
		},
	}
	sql, args := f.DatabaseSQL("db")
	assert.Equal(t, "LOWER(db) LIKE ?", sql)
	assert.Equal(t, []any{"prod\\_%"}, args)
}

func TestDatabaseSQL_ExcludeExact(t *testing.T) {
	f := &ScopeFilter{
		Exclude: []ScopeRule{
			{Database: "dev"},
			{Database: "test"},
		},
	}
	sql, args := f.DatabaseSQL("db")
	assert.Equal(t, "LOWER(db) NOT IN (?, ?)", sql)
	assert.Equal(t, []any{"dev", "test"}, args)
}

func TestDatabaseSQL_ExcludeWildcard(t *testing.T) {
	f := &ScopeFilter{
		Exclude: []ScopeRule{
			{Database: "dev_*"},
		},
	}
	sql, args := f.DatabaseSQL("db")
	assert.Equal(t, "LOWER(db) NOT LIKE ?", sql)
	assert.Equal(t, []any{"dev\\_%"}, args)
}

func TestDatabaseSQL_IncludeAndExclude(t *testing.T) {
	f := &ScopeFilter{
		Include: []ScopeRule{{Database: "prod"}},
		Exclude: []ScopeRule{{Database: "prod_old"}},
	}
	sql, args := f.DatabaseSQL("db")
	assert.Equal(t, "(LOWER(db) IN (?) AND LOWER(db) NOT IN (?))", sql)
	assert.Equal(t, []any{"prod", "prod_old"}, args)
}

func TestDatabaseSQL_ExcludeWithSchemaIgnored(t *testing.T) {
	// Exclude rules with schema constraints should not generate database-only SQL.
	f := &ScopeFilter{
		Exclude: []ScopeRule{
			{Database: "prod", Schema: "internal"},
		},
	}
	sql, args := f.DatabaseSQL("db")
	assert.Empty(t, sql)
	assert.Nil(t, args)
}

func TestSchemaSQL_IncludeExact(t *testing.T) {
	f := &ScopeFilter{
		Include: []ScopeRule{
			{Schema: "public"},
			{Schema: "analytics"},
		},
	}
	sql, args := f.SchemaSQL("db", "schema_name")
	assert.Equal(t, "(LOWER(schema_name) = ? OR LOWER(schema_name) = ?)", sql)
	assert.Equal(t, []any{"public", "analytics"}, args)
}

func TestSchemaSQL_ExcludeCrossLevel(t *testing.T) {
	f := &ScopeFilter{
		Exclude: []ScopeRule{
			{Database: "analytics", Schema: "raw_*"},
		},
	}
	sql, args := f.SchemaSQL("db", "schema_name")
	assert.Equal(t, "NOT (LOWER(db) = ? AND LOWER(schema_name) LIKE ?)", sql)
	assert.Equal(t, []any{"analytics", "raw\\_%"}, args)
}

func TestSchemaSQL_ExcludeSchemaOnly(t *testing.T) {
	f := &ScopeFilter{
		Exclude: []ScopeRule{
			{Schema: "information_schema"},
			{Schema: "pg_catalog"},
		},
	}
	sql, args := f.SchemaSQL("db", "schema_name")
	assert.Equal(t, "(NOT LOWER(schema_name) = ? AND NOT LOWER(schema_name) = ?)", sql)
	assert.Equal(t, []any{"information_schema", "pg_catalog"}, args)
}

func TestTableSQL_IncludeWithCrossLevel(t *testing.T) {
	f := &ScopeFilter{
		Include: []ScopeRule{
			{Database: "prod", Schema: "public", Table: "users"},
		},
	}
	sql, args := f.TableSQL("db", "schema", "tbl")
	assert.Equal(t, "(LOWER(db) = ? AND LOWER(schema) = ? AND LOWER(tbl) = ?)", sql)
	assert.Equal(t, []any{"prod", "public", "users"}, args)
}

func TestTableSQL_ExcludeCrossLevel(t *testing.T) {
	f := &ScopeFilter{
		Exclude: []ScopeRule{
			{Schema: "*_dev", Table: "*_tmp"},
		},
	}
	sql, args := f.TableSQL("db", "schema", "tbl")
	assert.Equal(t, "NOT (LOWER(schema) LIKE ? AND LOWER(tbl) LIKE ?)", sql)
	assert.Equal(t, []any{"%\\_dev", "%\\_tmp"}, args)
}

func TestTableSQL_MixedIncludeExclude(t *testing.T) {
	f := &ScopeFilter{
		Include: []ScopeRule{
			{Database: "prod"},
		},
		Exclude: []ScopeRule{
			{Schema: "information_schema"},
		},
	}
	sql, args := f.TableSQL("db", "schema", "tbl")
	assert.Equal(t, "(LOWER(db) = ? AND NOT LOWER(schema) = ?)", sql)
	assert.Equal(t, []any{"prod", "information_schema"}, args)
}

func TestGlobToSQLLike(t *testing.T) {
	tests := []struct {
		glob string
		want string
	}{
		{"*", "%"},
		{"prefix_*", "prefix\\_%"},
		{"*_suffix", "%\\_suffix"},
		{"pre_*_suf", "pre\\_%\\_suf"},
		{"exact", "exact"},
		{"with%percent", "with\\%percent"},
		{"with_underscore", "with\\_underscore"},
	}
	for _, tt := range tests {
		t.Run(tt.glob, func(t *testing.T) {
			got := globToSQLLike(tt.glob)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDatabaseSQL_Merged(t *testing.T) {
	f1 := &ScopeFilter{
		Include: []ScopeRule{{Database: "prod"}, {Database: "analytics"}},
	}
	f2 := &ScopeFilter{
		Include: []ScopeRule{{Database: "prod"}},
	}
	merged := Merge(f1, f2)
	sql, args := merged.DatabaseSQL("db")
	assert.Equal(t, "(LOWER(db) IN (?, ?) AND LOWER(db) IN (?))", sql)
	assert.Equal(t, []any{"prod", "analytics", "prod"}, args)
}

func TestSchemaSQL_IncludeWithDatabaseAndSchema(t *testing.T) {
	f := &ScopeFilter{
		Include: []ScopeRule{
			{Database: "prod", Schema: "public"},
			{Database: "analytics"},
		},
	}
	sql, args := f.SchemaSQL("db", "schema")
	assert.Equal(t, "((LOWER(db) = ? AND LOWER(schema) = ?) OR LOWER(db) = ?)", sql)
	assert.Equal(t, []any{"prod", "public", "analytics"}, args)
}

func TestSchemaSQL_IncludeAllEmptyFields(t *testing.T) {
	// A rule with no database/schema constraints matches everything — no SQL needed.
	f := &ScopeFilter{
		Include: []ScopeRule{
			{Table: "users"}, // Only table constraint, irrelevant at schema level.
		},
	}
	sql, args := f.SchemaSQL("db", "schema")
	assert.Empty(t, sql)
	assert.Nil(t, args)
}

func TestTableSQL_ExcludeTableWithAllLevels(t *testing.T) {
	f := &ScopeFilter{
		Exclude: []ScopeRule{
			{Database: "dev", Schema: "staging", Table: "tmp_*"},
		},
	}
	sql, args := f.TableSQL("db", "schema", "tbl")
	assert.Equal(t, "NOT (LOWER(db) = ? AND LOWER(schema) = ? AND LOWER(tbl) LIKE ?)", sql)
	assert.Equal(t, []any{"dev", "staging", "tmp\\_%"}, args)
}
