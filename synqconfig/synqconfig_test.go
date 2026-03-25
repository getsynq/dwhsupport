package synqconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestApplyEnvDefaults_AllEmpty(t *testing.T) {
	t.Setenv("SYNQ_CLIENT_ID", "env-id")
	t.Setenv("SYNQ_CLIENT_SECRET", "env-secret")
	t.Setenv("SYNQ_API_ENDPOINT", "custom.synq.io:443")

	c := &SYNQConfig{}
	c.ApplyEnvDefaults()

	assert.Equal(t, "env-id", c.ClientID)
	assert.Equal(t, "env-secret", c.ClientSecret)
	assert.Equal(t, "custom.synq.io:443", c.Endpoint)
	assert.Equal(t, "custom.synq.io:443", c.IngestEndpoint)
	assert.Equal(t, "https://custom.synq.io/oauth2/token", c.OAuthURL)
}

func TestApplyEnvDefaults_YAMLTakesPrecedence(t *testing.T) {
	t.Setenv("SYNQ_CLIENT_ID", "env-id")
	t.Setenv("SYNQ_CLIENT_SECRET", "env-secret")

	c := &SYNQConfig{
		ClientID:     "yaml-id",
		ClientSecret: "yaml-secret",
		Endpoint:     "yaml.synq.io:443",
	}
	c.ApplyEnvDefaults()

	assert.Equal(t, "yaml-id", c.ClientID)
	assert.Equal(t, "yaml-secret", c.ClientSecret)
	assert.Equal(t, "yaml.synq.io:443", c.Endpoint)
}

func TestApplyEnvDefaults_FallsBackToDefault(t *testing.T) {
	// Clear env vars
	t.Setenv("SYNQ_API_ENDPOINT", "")
	t.Setenv("SYNQ_API_URL", "")

	c := &SYNQConfig{}
	c.ApplyEnvDefaults()

	assert.Equal(t, DefaultEndpoint, c.Endpoint)
	assert.Equal(t, DefaultEndpoint, c.IngestEndpoint)
	assert.Equal(t, "https://developer.synq.io/oauth2/token", c.OAuthURL)
}

func TestApplyEnvDefaults_SYNQ_API_URL_Fallback(t *testing.T) {
	t.Setenv("SYNQ_API_ENDPOINT", "")
	t.Setenv("SYNQ_API_URL", "https://api.us.synq.io")

	c := &SYNQConfig{}
	c.ApplyEnvDefaults()

	assert.Equal(t, "https://api.us.synq.io", c.Endpoint)
}

func TestApplyEnvDefaults_IngestEndpointPreserved(t *testing.T) {
	c := &SYNQConfig{
		Endpoint:       "api.synq.io:443",
		IngestEndpoint: "ingest.synq.io:443",
	}
	c.ApplyEnvDefaults()

	assert.Equal(t, "ingest.synq.io:443", c.IngestEndpoint)
}

func TestToProto(t *testing.T) {
	c := &SYNQConfig{
		ClientID:       "id",
		ClientSecret:   "secret",
		Endpoint:       "ep:443",
		IngestEndpoint: "ingest:443",
		OAuthURL:       "https://ep/oauth2/token",
	}

	proto := c.ToProto()
	assert.Equal(t, "id", proto.GetClientId())
	assert.Equal(t, "secret", proto.GetClientSecret())
	assert.Equal(t, "ep:443", proto.GetEndpoint())
	assert.Equal(t, "ingest:443", proto.GetIngestEndpoint())
	assert.Equal(t, "https://ep/oauth2/token", proto.GetOauthUrl())
}

func TestDeriveOAuthURL(t *testing.T) {
	tests := []struct {
		endpoint string
		expected string
	}{
		{"developer.synq.io:443", "https://developer.synq.io/oauth2/token"},
		{"api.us.synq.io:443", "https://api.us.synq.io/oauth2/token"},
		{"localhost:8080", "https://localhost/oauth2/token"},
		{"developer.synq.io", "https://developer.synq.io/oauth2/token"},
	}
	for _, tt := range tests {
		t.Run(tt.endpoint, func(t *testing.T) {
			assert.Equal(t, tt.expected, DeriveOAuthURL(tt.endpoint))
		})
	}
}

func TestDeriveAppURL(t *testing.T) {
	tests := []struct {
		endpoint string
		expected string
	}{
		{"developer.synq.io:443", "https://app.synq.io"},
		{"api.us.synq.io:443", "https://app.us.synq.io"},
		{"custom.example.com:443", "https://app.synq.io"},
	}
	for _, tt := range tests {
		t.Run(tt.endpoint, func(t *testing.T) {
			assert.Equal(t, tt.expected, DeriveAppURL(tt.endpoint))
		})
	}
}
