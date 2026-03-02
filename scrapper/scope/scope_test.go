package scope

import (
	"context"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNilScopeFilter(t *testing.T) {
	var f *ScopeFilter
	assert.True(t, f.IsObjectAccepted("db", "schema", "table"))
	assert.True(t, f.IsDatabaseAccepted("db"))
	assert.True(t, f.IsSchemaAccepted("db", "schema"))
	assert.True(t, f.IsFqnAccepted(scrapper.DwhFqn{DatabaseName: "db", SchemaName: "schema", ObjectName: "table"}))
}

func TestEmptyScopeFilter(t *testing.T) {
	f := &ScopeFilter{}
	assert.True(t, f.IsObjectAccepted("db", "schema", "table"))
	assert.True(t, f.IsDatabaseAccepted("db"))
	assert.True(t, f.IsSchemaAccepted("db", "schema"))
}

func TestIncludeOnly(t *testing.T) {
	f := &ScopeFilter{
		Include: []ScopeRule{
			{Database: "prod"},
			{Database: "analytics"},
		},
	}

	assert.True(t, f.IsObjectAccepted("prod", "public", "users"))
	assert.True(t, f.IsObjectAccepted("analytics", "raw", "events"))
	assert.False(t, f.IsObjectAccepted("dev", "public", "users"))

	assert.True(t, f.IsDatabaseAccepted("prod"))
	assert.True(t, f.IsDatabaseAccepted("analytics"))
	assert.False(t, f.IsDatabaseAccepted("dev"))

	assert.True(t, f.IsSchemaAccepted("prod", "anything"))
	assert.False(t, f.IsSchemaAccepted("dev", "anything"))
}

func TestExcludeOnly(t *testing.T) {
	f := &ScopeFilter{
		Exclude: []ScopeRule{
			{Schema: "information_schema"},
			{Schema: "pg_catalog"},
		},
	}

	assert.True(t, f.IsObjectAccepted("db", "public", "users"))
	assert.False(t, f.IsObjectAccepted("db", "information_schema", "tables"))
	assert.False(t, f.IsObjectAccepted("db", "pg_catalog", "pg_class"))

	// Database-level: can't reject because excludes have schema constraints.
	assert.True(t, f.IsDatabaseAccepted("db"))

	// Schema-level: can reject.
	assert.True(t, f.IsSchemaAccepted("db", "public"))
	assert.False(t, f.IsSchemaAccepted("db", "information_schema"))
}

func TestExcludePrecedenceOverInclude(t *testing.T) {
	f := &ScopeFilter{
		Include: []ScopeRule{
			{Database: "prod"},
		},
		Exclude: []ScopeRule{
			{Database: "prod", Schema: "internal"},
		},
	}

	assert.True(t, f.IsObjectAccepted("prod", "public", "users"))
	assert.False(t, f.IsObjectAccepted("prod", "internal", "secrets"))
	assert.False(t, f.IsObjectAccepted("dev", "public", "users"))
}

func TestGlobPatterns(t *testing.T) {
	f := &ScopeFilter{
		Include: []ScopeRule{
			{Database: "prod_*"},
		},
		Exclude: []ScopeRule{
			{Table: "*_tmp"},
		},
	}

	assert.True(t, f.IsObjectAccepted("prod_us", "public", "users"))
	assert.True(t, f.IsObjectAccepted("prod_eu", "raw", "events"))
	assert.False(t, f.IsObjectAccepted("dev_us", "public", "users"))
	assert.False(t, f.IsObjectAccepted("prod_us", "public", "staging_tmp"))

	// Zero-char match: prod_* should match prod_
	assert.True(t, f.IsObjectAccepted("prod_", "public", "users"))
}

func TestCaseInsensitiveMatching(t *testing.T) {
	f := &ScopeFilter{
		Include: []ScopeRule{
			{Database: "PROD_DB"},
		},
	}

	assert.True(t, f.IsObjectAccepted("prod_db", "public", "users"))
	assert.True(t, f.IsObjectAccepted("PROD_DB", "public", "users"))
	assert.True(t, f.IsObjectAccepted("Prod_Db", "public", "users"))
	assert.False(t, f.IsObjectAccepted("dev_db", "public", "users"))
}

func TestCrossLevelRules(t *testing.T) {
	f := &ScopeFilter{
		Exclude: []ScopeRule{
			{Schema: "*_dev", Table: "*_tmp"},
		},
	}

	// Only excluded when BOTH conditions match.
	assert.False(t, f.IsObjectAccepted("db", "staging_dev", "data_tmp"))
	assert.True(t, f.IsObjectAccepted("db", "staging_dev", "data"))
	assert.True(t, f.IsObjectAccepted("db", "production", "data_tmp"))

	// Schema-level: can't reject because exclude has table constraint.
	assert.True(t, f.IsSchemaAccepted("db", "staging_dev"))
}

func TestDatabaseExcludeWithNoConstraints(t *testing.T) {
	f := &ScopeFilter{
		Exclude: []ScopeRule{
			{Database: "dev_*"},
		},
	}

	assert.False(t, f.IsDatabaseAccepted("dev_test"))
	assert.True(t, f.IsDatabaseAccepted("prod"))

	assert.False(t, f.IsSchemaAccepted("dev_test", "public"))
	assert.True(t, f.IsSchemaAccepted("prod", "public"))
}

func TestIncludeWithSchemaOnly(t *testing.T) {
	f := &ScopeFilter{
		Include: []ScopeRule{
			{Schema: "public"},
			{Schema: "analytics"},
		},
	}

	assert.True(t, f.IsObjectAccepted("any_db", "public", "any_table"))
	assert.True(t, f.IsObjectAccepted("any_db", "analytics", "any_table"))
	assert.False(t, f.IsObjectAccepted("any_db", "internal", "any_table"))

	// Database-level: can't reject because include rules have no database pattern.
	assert.True(t, f.IsDatabaseAccepted("any_db"))

	// Schema-level: can reject.
	assert.True(t, f.IsSchemaAccepted("any_db", "public"))
	assert.False(t, f.IsSchemaAccepted("any_db", "internal"))
}

func TestEmptyFieldMatchesAnything(t *testing.T) {
	// A rule with only table set applies to all databases and schemas.
	f := &ScopeFilter{
		Exclude: []ScopeRule{
			{Table: "tmp_*"},
		},
	}

	assert.False(t, f.IsObjectAccepted("any_db", "any_schema", "tmp_data"))
	assert.True(t, f.IsObjectAccepted("any_db", "any_schema", "real_data"))
}

// Context tests

func TestContextWithScope(t *testing.T) {
	ctx := context.Background()
	assert.Nil(t, GetScope(ctx))

	f := &ScopeFilter{
		Include: []ScopeRule{{Database: "prod"}},
	}
	ctx = WithScope(ctx, f)
	got := GetScope(ctx)
	require.NotNil(t, got)
	assert.True(t, got.IsDatabaseAccepted("prod"))
	assert.False(t, got.IsDatabaseAccepted("dev"))
}

func TestContextNilScope(t *testing.T) {
	ctx := context.Background()
	ctx = WithScope(ctx, nil)
	assert.Nil(t, GetScope(ctx))
}

func TestContextStacking(t *testing.T) {
	ctx := context.Background()

	// Layer 1: include prod and analytics databases.
	base := &ScopeFilter{
		Include: []ScopeRule{
			{Database: "prod"},
			{Database: "analytics"},
		},
	}
	ctx = WithScope(ctx, base)

	// Layer 2: narrow to only prod.
	narrow := &ScopeFilter{
		Include: []ScopeRule{
			{Database: "prod"},
		},
	}
	ctx = WithScope(ctx, narrow)

	got := GetScope(ctx)
	require.NotNil(t, got)

	// Only prod passes both layers.
	assert.True(t, got.IsDatabaseAccepted("prod"))
	assert.False(t, got.IsDatabaseAccepted("analytics"))
	assert.False(t, got.IsDatabaseAccepted("dev"))
}

func TestContextStackingExclude(t *testing.T) {
	ctx := context.Background()

	// Layer 1: exclude system schemas.
	layer1 := &ScopeFilter{
		Exclude: []ScopeRule{
			{Schema: "information_schema"},
		},
	}
	ctx = WithScope(ctx, layer1)

	// Layer 2: also exclude pg_catalog.
	layer2 := &ScopeFilter{
		Exclude: []ScopeRule{
			{Schema: "pg_catalog"},
		},
	}
	ctx = WithScope(ctx, layer2)

	got := GetScope(ctx)
	require.NotNil(t, got)

	assert.True(t, got.IsSchemaAccepted("db", "public"))
	assert.False(t, got.IsSchemaAccepted("db", "information_schema"))
	assert.False(t, got.IsSchemaAccepted("db", "pg_catalog"))
}

// Merge tests

func TestMergeNils(t *testing.T) {
	assert.Nil(t, Merge())
	assert.Nil(t, Merge(nil))
	assert.Nil(t, Merge(nil, nil))
}

func TestMergeSingle(t *testing.T) {
	f := &ScopeFilter{Include: []ScopeRule{{Database: "prod"}}}
	assert.Same(t, f, Merge(f))
	assert.Same(t, f, Merge(nil, f, nil))
}

func TestMergeMultiple(t *testing.T) {
	f1 := &ScopeFilter{
		Include: []ScopeRule{{Database: "prod"}, {Database: "analytics"}},
	}
	f2 := &ScopeFilter{
		Include: []ScopeRule{{Database: "prod"}},
	}

	merged := Merge(f1, f2)
	require.NotNil(t, merged)

	assert.True(t, merged.IsObjectAccepted("prod", "public", "users"))
	assert.False(t, merged.IsObjectAccepted("analytics", "raw", "events"))
	assert.False(t, merged.IsObjectAccepted("dev", "public", "users"))
}

func TestMergeIncludeAndExclude(t *testing.T) {
	include := &ScopeFilter{
		Include: []ScopeRule{{Database: "prod"}},
	}
	exclude := &ScopeFilter{
		Exclude: []ScopeRule{{Schema: "internal"}},
	}

	merged := Merge(include, exclude)
	require.NotNil(t, merged)

	assert.True(t, merged.IsObjectAccepted("prod", "public", "users"))
	assert.False(t, merged.IsObjectAccepted("prod", "internal", "secrets"))
	assert.False(t, merged.IsObjectAccepted("dev", "public", "users"))
}

// FilterRows tests

func TestFilterRows(t *testing.T) {
	rows := []*scrapper.CatalogColumnRow{
		{Database: "prod", Schema: "public", Table: "users", Column: "id"},
		{Database: "prod", Schema: "internal", Table: "secrets", Column: "key"},
		{Database: "dev", Schema: "public", Table: "users", Column: "id"},
	}

	f := &ScopeFilter{
		Include: []ScopeRule{{Database: "prod"}},
		Exclude: []ScopeRule{{Schema: "internal"}},
	}

	filtered := FilterRows(rows, f)
	require.Len(t, filtered, 1)
	assert.Equal(t, "public", filtered[0].Schema)
}

func TestFilterRowsNilFilter(t *testing.T) {
	rows := []*scrapper.TableRow{
		{Database: "db", Schema: "public", Table: "t1"},
	}
	filtered := FilterRows(rows, nil)
	assert.Len(t, filtered, 1)
}

func TestFilterDatabaseRows(t *testing.T) {
	rows := []*scrapper.DatabaseRow{
		{Database: "prod"},
		{Database: "dev"},
		{Database: "analytics"},
	}

	f := &ScopeFilter{
		Include: []ScopeRule{{Database: "prod"}, {Database: "analytics"}},
	}

	filtered := FilterDatabaseRows(rows, f)
	require.Len(t, filtered, 2)
	assert.Equal(t, "prod", filtered[0].Database)
	assert.Equal(t, "analytics", filtered[1].Database)
}

// Pattern matching tests

func TestMatchPattern(t *testing.T) {
	tests := []struct {
		pattern string
		value   string
		want    bool
	}{
		{"", "anything", true},
		{"exact", "exact", true},
		{"exact", "other", false},
		{"UPPER", "upper", true},
		{"lower", "LOWER", true},
		{"*", "", true},
		{"*", "anything", true},
		{"prefix_*", "prefix_foo", true},
		{"prefix_*", "prefix_", true},
		{"*_suffix", "foo_suffix", true},
		{"*_suffix", "_suffix", true},
		{"pre_*_suf", "pre_mid_suf", true},
		{"pre_*_suf", "pre__suf", true},
		{"pre_*_suf", "pre_suf", false},
		{"test.*", "test.foo", true},
		{"test.*", "test_foo", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"/"+tt.value, func(t *testing.T) {
			got := matchPattern(tt.pattern, tt.value)
			assert.Equal(t, tt.want, got, "matchPattern(%q, %q)", tt.pattern, tt.value)
		})
	}
}

// YAML round-trip test

func TestYAMLRoundTrip(t *testing.T) {
	f := &ScopeFilter{
		Include: []ScopeRule{
			{Database: "prod_*"},
			{Database: "analytics"},
		},
		Exclude: []ScopeRule{
			{Schema: "*_dev", Table: "*_tmp"},
			{Schema: "information_schema"},
		},
	}

	// Verify struct fields have correct tags.
	assert.Equal(t, "prod_*", f.Include[0].Database)
	assert.Equal(t, "*_dev", f.Exclude[0].Schema)
	assert.Equal(t, "*_tmp", f.Exclude[0].Table)
}

// Partial evaluation correctness tests

func TestPartialEvalDatabaseWithCrossLevelInclude(t *testing.T) {
	// Include rule has schema constraint — can't definitively reject any database.
	f := &ScopeFilter{
		Include: []ScopeRule{
			{Database: "prod", Schema: "public"},
		},
	}

	// Should accept prod (matches database part).
	assert.True(t, f.IsDatabaseAccepted("prod"))
	// Should reject dev (no include rule can match).
	assert.False(t, f.IsDatabaseAccepted("dev"))
}

func TestPartialEvalDatabaseWithNoDbPattern(t *testing.T) {
	// Include rule has no database pattern — matches any database.
	f := &ScopeFilter{
		Include: []ScopeRule{
			{Schema: "public"},
		},
	}

	assert.True(t, f.IsDatabaseAccepted("any_db"))
}

func TestPartialEvalSchemaWithCrossLevelExclude(t *testing.T) {
	// Exclude has table constraint — can't definitively reject schema.
	f := &ScopeFilter{
		Exclude: []ScopeRule{
			{Schema: "staging", Table: "*_tmp"},
		},
	}

	// Can't reject staging schema because not all tables are excluded.
	assert.True(t, f.IsSchemaAccepted("db", "staging"))
	assert.True(t, f.IsSchemaAccepted("db", "public"))
}

func TestThreeLayerStacking(t *testing.T) {
	ctx := context.Background()

	// Layer 1: Connector config — only PROD_DB and ANALYTICS_DB.
	ctx = WithScope(ctx, &ScopeFilter{
		Include: []ScopeRule{{Database: "PROD_DB"}, {Database: "ANALYTICS_DB"}},
	})

	// Layer 2: Feature narrowing — only PROD_DB.
	ctx = WithScope(ctx, &ScopeFilter{
		Include: []ScopeRule{{Database: "PROD_DB"}},
	})

	// Layer 3: Schema narrowing — only public.
	ctx = WithScope(ctx, &ScopeFilter{
		Include: []ScopeRule{{Schema: "public"}},
	})

	got := GetScope(ctx)
	require.NotNil(t, got)

	assert.True(t, got.IsObjectAccepted("PROD_DB", "public", "users"))
	assert.False(t, got.IsObjectAccepted("PROD_DB", "internal", "users"))
	assert.False(t, got.IsObjectAccepted("ANALYTICS_DB", "public", "users"))
	assert.False(t, got.IsObjectAccepted("dev", "public", "users"))
}
