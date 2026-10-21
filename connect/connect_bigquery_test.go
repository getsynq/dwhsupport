package connect

import (
	"testing"

	agentdwhv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/agent/dwh/v1"
	"github.com/stretchr/testify/assert"
)

func TestBigQueryScrapperConf(t *testing.T) {
	got := bigQueryScrapperConf(&agentdwhv1.BigQueryConf{
		ProjectId:             "my-gcp-project",
		Region:                "europe-west2",
		ServiceAccountKey:     `{"type":"service_account"}`,
		ServiceAccountKeyFile: "keys/service-account.json",
		Datasets:              []string{"analytics", "mart_core"},
		FetchPolicyTags:       true,
	})

	assert.Equal(t, "my-gcp-project", got.ProjectId)
	assert.Equal(t, "europe-west2", got.Region)
	assert.Equal(t, `{"type":"service_account"}`, got.CredentialsJson)
	assert.Equal(t, "keys/service-account.json", got.CredentialsFile)
	assert.Equal(t, []string{"analytics", "mart_core"}, got.Datasets)
	assert.True(t, got.FetchPolicyTags)
}

// A Data Catalog call the role is denied shows in the customer's audit log, so a connection that
// does not ask for policy tag names does not get the call.
func TestBigQueryScrapperConf_FetchPolicyTagsIsOptIn(t *testing.T) {
	got := bigQueryScrapperConf(&agentdwhv1.BigQueryConf{ProjectId: "my-gcp-project", Region: "europe-west2"})

	assert.False(t, got.FetchPolicyTags)
}
