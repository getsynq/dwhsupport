package databricks

import (
	"context"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/stretchr/testify/require"
)

// TestScopeFromConf pins how the two ways of configuring scope relate: Scope is what
// callers should set, CatalogBlocklist still works for those that have not migrated,
// and a caller that has migrated is never also subject to a stale blocklist.
func TestScopeFromConf(t *testing.T) {
	t.Run("neither configured accepts everything", func(t *testing.T) {
		f := ScopeFromConf(&DatabricksScrapperConf{})
		require.Nil(t, f)
		require.True(t, f.IsDatabaseAccepted("anything"))
	})

	t.Run("blocklist excludes catalogs", func(t *testing.T) {
		f := ScopeFromConf(&DatabricksScrapperConf{CatalogBlocklist: "test_*, dev_one"})
		require.NotNil(t, f)
		require.False(t, f.IsDatabaseAccepted("test_catalog"))
		require.False(t, f.IsDatabaseAccepted("dev_one"))
		require.True(t, f.IsDatabaseAccepted("prod"))
	})

	t.Run("scope supersedes blocklist", func(t *testing.T) {
		f := ScopeFromConf(&DatabricksScrapperConf{
			CatalogBlocklist: "prod",
			Scope:            &scope.ScopeFilter{Include: []scope.ScopeRule{{Database: "prod"}}},
		})
		require.NotNil(t, f)
		// The blocklist would have excluded prod; the Scope that replaces it includes it.
		require.True(t, f.IsDatabaseAccepted("prod"))
		require.False(t, f.IsDatabaseAccepted("other"))
	})

	t.Run("empty scope means no filtering, not fall back to blocklist", func(t *testing.T) {
		f := ScopeFromConf(&DatabricksScrapperConf{
			CatalogBlocklist: "prod",
			Scope:            &scope.ScopeFilter{},
		})
		require.True(t, f.IsDatabaseAccepted("prod"))
	})

	t.Run("scope reaches schemas and tables, which a blocklist cannot", func(t *testing.T) {
		f := ScopeFromConf(&DatabricksScrapperConf{
			Scope: &scope.ScopeFilter{
				Exclude: []scope.ScopeRule{{Database: "prod", Schema: "staging"}},
			},
		})
		require.True(t, f.IsDatabaseAccepted("prod"))
		require.False(t, f.IsSchemaAccepted("prod", "staging"))
		require.True(t, f.IsSchemaAccepted("prod", "marts"))
	})
}

// TestEffectiveScopeMergesContext pins that a per-call scope narrows the configured
// one rather than replacing or being ignored by it.
func TestEffectiveScopeMergesContext(t *testing.T) {
	conf := &DatabricksScrapperConf{CatalogBlocklist: "blocked"}
	e := &DatabricksScrapper{conf: conf, scope: ScopeFromConf(conf)}

	perCall := &scope.ScopeFilter{Include: []scope.ScopeRule{{Database: "wanted"}}}
	merged := e.effectiveScope(scope.WithScope(context.Background(), perCall))

	require.True(t, merged.IsDatabaseAccepted("wanted"), "per-call include should be honoured")
	require.False(t, merged.IsDatabaseAccepted("other"), "per-call include should exclude the rest")
	require.False(t, merged.IsDatabaseAccepted("blocked"), "configured scope should still apply")

	// With no per-call scope the configured one is all that applies.
	configured := e.effectiveScope(context.Background())
	require.False(t, configured.IsDatabaseAccepted("blocked"))
	require.True(t, configured.IsDatabaseAccepted("wanted"))
	require.True(t, configured.IsDatabaseAccepted("other"))
}
