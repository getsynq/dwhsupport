package scope

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInlineDatabaseSQL_Nil(t *testing.T) {
	var f *ScopeFilter
	assert.Empty(t, f.InlineDatabaseSQL("db"))
}

func TestInlineDatabaseSQL_IncludeExact(t *testing.T) {
	f := &ScopeFilter{
		Include: []ScopeRule{
			{Database: "prod"},
			{Database: "analytics"},
		},
	}
	got := f.InlineDatabaseSQL("database_name")
	assert.Equal(t, "LOWER(database_name) IN ('prod', 'analytics')", got)
}

func TestInlineDatabaseSQL_IncludeWildcard(t *testing.T) {
	f := &ScopeFilter{
		Include: []ScopeRule{
			{Database: "prod_*"},
		},
	}
	got := f.InlineDatabaseSQL("db")
	assert.Equal(t, "LOWER(db) LIKE 'prod\\_%'", got)
}

func TestInlineDatabaseSQL_ExcludeExact(t *testing.T) {
	f := &ScopeFilter{
		Exclude: []ScopeRule{
			{Database: "dev"},
			{Database: "test"},
		},
	}
	got := f.InlineDatabaseSQL("db")
	assert.Equal(t, "LOWER(db) NOT IN ('dev', 'test')", got)
}

func TestInlineSchemaSQL_ExcludeSchemaOnly(t *testing.T) {
	f := &ScopeFilter{
		Exclude: []ScopeRule{
			{Schema: "information_schema"},
			{Schema: "pg_catalog"},
		},
	}
	got := f.InlineSchemaSQL("db", "schema_name")
	assert.Equal(t, "(NOT LOWER(schema_name) = 'information_schema' AND NOT LOWER(schema_name) = 'pg_catalog')", got)
}

func TestInlineSchemaSQL_ExcludeCrossLevel(t *testing.T) {
	f := &ScopeFilter{
		Exclude: []ScopeRule{
			{Database: "analytics", Schema: "raw_*"},
		},
	}
	got := f.InlineSchemaSQL("db", "schema_name")
	assert.Equal(t, "NOT (LOWER(db) = 'analytics' AND LOWER(schema_name) LIKE 'raw\\_%')", got)
}

func TestInlineTableSQL_ExcludeCrossLevel(t *testing.T) {
	f := &ScopeFilter{
		Exclude: []ScopeRule{
			{Schema: "*_dev", Table: "*_tmp"},
		},
	}
	got := f.InlineTableSQL("db", "schema", "tbl")
	assert.Equal(t, "NOT (LOWER(schema) LIKE '%\\_dev' AND LOWER(tbl) LIKE '%\\_tmp')", got)
}

func TestInlineTableSQL_IncludeWithDatabase(t *testing.T) {
	f := &ScopeFilter{
		Include: []ScopeRule{
			{Database: "prod", Schema: "public"},
			{Database: "analytics"},
		},
	}
	got := f.InlineTableSQL("db", "schema", "tbl")
	assert.Equal(t, "((LOWER(db) = 'prod' AND LOWER(schema) = 'public') OR LOWER(db) = 'analytics')", got)
}

func TestInlineDatabaseSQL_Merged(t *testing.T) {
	f1 := &ScopeFilter{
		Include: []ScopeRule{{Database: "prod"}, {Database: "analytics"}},
	}
	f2 := &ScopeFilter{
		Include: []ScopeRule{{Database: "prod"}},
	}
	merged := Merge(f1, f2)
	got := merged.InlineDatabaseSQL("db")
	assert.Equal(t, "(LOWER(db) IN ('prod', 'analytics') AND LOWER(db) IN ('prod'))", got)
}

func TestQuoteLiteral(t *testing.T) {
	assert.Equal(t, "'hello'", quoteLiteral("hello"))
	assert.Equal(t, "'it''s'", quoteLiteral("it's"))
	assert.Equal(t, "'a''b''c'", quoteLiteral("a'b'c"))
}

func TestInlineTableSQL_IncludeAllEmpty(t *testing.T) {
	// Include rule with only table — db and schema columns are empty string, should be ignored.
	f := &ScopeFilter{
		Include: []ScopeRule{{Table: "users"}},
	}
	got := f.InlineTableSQL("db", "schema", "tbl")
	assert.Equal(t, "LOWER(tbl) = 'users'", got)
}

func TestInlineSchemaSQL_IncludeAllEmptyFields(t *testing.T) {
	// Rule with no database/schema constraints matches everything at schema level.
	f := &ScopeFilter{
		Include: []ScopeRule{{Table: "users"}},
	}
	got := f.InlineSchemaSQL("db", "schema")
	assert.Empty(t, got)
}
