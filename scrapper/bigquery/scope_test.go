package bigquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScopeFromConf_Nil(t *testing.T) {
	assert.Nil(t, ScopeFromConf(nil))
}

func TestScopeFromConf_Empty(t *testing.T) {
	assert.Nil(t, ScopeFromConf(&BigQueryScrapperConf{}))
}

func TestScopeFromConf_BlocklistOnly(t *testing.T) {
	f := ScopeFromConf(&BigQueryScrapperConf{Blocklist: "tmp_*, dbt_pr_*"})
	if assert.NotNil(t, f) {
		assert.Empty(t, f.Include)
		assert.Equal(t, 2, len(f.Exclude))
		assert.True(t, f.IsSchemaAccepted("proj", "analytics"))
		assert.False(t, f.IsSchemaAccepted("proj", "tmp_123"))
		assert.False(t, f.IsSchemaAccepted("proj", "dbt_pr_abc"))
	}
}

func TestScopeFromConf_DatasetsAllowlistOnly(t *testing.T) {
	f := ScopeFromConf(&BigQueryScrapperConf{Datasets: []string{"analytics", "marketing"}})
	if assert.NotNil(t, f) {
		assert.Equal(t, 2, len(f.Include))
		assert.Empty(t, f.Exclude)
		assert.True(t, f.IsSchemaAccepted("proj", "analytics"))
		assert.True(t, f.IsSchemaAccepted("proj", "marketing"))
		assert.False(t, f.IsSchemaAccepted("proj", "finance"))
		assert.False(t, f.IsSchemaAccepted("proj", "random"))
	}
}

func TestScopeFromConf_DatasetsAndBlocklist(t *testing.T) {
	// Allowlist wins as the base set; blocklist can carve out individual
	// allowlisted datasets (exclude takes precedence).
	f := ScopeFromConf(&BigQueryScrapperConf{
		Datasets:  []string{"analytics", "staging_*"},
		Blocklist: "staging_tmp",
	})
	if assert.NotNil(t, f) {
		assert.Equal(t, 2, len(f.Include))
		assert.Equal(t, 1, len(f.Exclude))
		assert.True(t, f.IsSchemaAccepted("proj", "analytics"))
		assert.True(t, f.IsSchemaAccepted("proj", "staging_main"))
		assert.False(t, f.IsSchemaAccepted("proj", "staging_tmp"))
		assert.False(t, f.IsSchemaAccepted("proj", "finance"))
	}
}

func TestScopeFromConf_TrimsAndSkipsEmpty(t *testing.T) {
	f := ScopeFromConf(&BigQueryScrapperConf{
		Datasets:  []string{"  analytics ", "", "   "},
		Blocklist: " tmp_* ,, ,dbt_pr_* ",
	})
	if assert.NotNil(t, f) {
		assert.Equal(t, 1, len(f.Include))
		assert.Equal(t, "analytics", f.Include[0].Schema)
		assert.Equal(t, 2, len(f.Exclude))
		assert.Equal(t, "tmp_*", f.Exclude[0].Schema)
		assert.Equal(t, "dbt_pr_*", f.Exclude[1].Schema)
	}
}
