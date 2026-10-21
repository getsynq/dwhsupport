package connect

import (
	"testing"

	agentdwhv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/agent/dwh/v1"
	"github.com/stretchr/testify/assert"
)

func TestRedshiftScrapperConf(t *testing.T) {
	got := redshiftScrapperConf(&agentdwhv1.RedshiftConf{
		Host:                   "redshift.example",
		Port:                   5439,
		Database:               "analytics",
		Username:               "scraper",
		Password:               "sekret",
		FreshnessFromQueryLogs: true,
		UseShowCreateTable:     true,
	})

	assert.Equal(t, "redshift.example", got.Host)
	assert.Equal(t, 5439, got.Port)
	assert.Equal(t, "analytics", got.Database)
	assert.Equal(t, "scraper", got.User)
	assert.Equal(t, "sekret", got.Password)
	assert.True(t, got.FreshnessFromQueryLogs)
	assert.True(t, got.UseShowCreateTable)
}

// SHOW TABLE runs once per table, so a connection that does not ask for it does not get it.
func TestRedshiftScrapperConf_ShowCreateTableIsOptIn(t *testing.T) {
	got := redshiftScrapperConf(&agentdwhv1.RedshiftConf{Host: "redshift.example"})

	assert.False(t, got.UseShowCreateTable)
	assert.False(t, got.FreshnessFromQueryLogs)
}
