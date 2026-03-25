// Package synqconfig provides shared SYNQ API credential configuration
// used by CLI tools (synq-dwh, synq-scout, synq-recon).
package synqconfig

import (
	"fmt"
	"net"
	"os"
	"strings"

	agentv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/agent/v1"
)

// DefaultEndpoint is the default SYNQ API gRPC endpoint (EU).
const DefaultEndpoint = "developer.synq.io:443"

// DefaultUsEndpoint is the SYNQ API gRPC endpoint for the US region.
const DefaultUsEndpoint = "api.us.synq.io:443"

// SYNQConfig holds SYNQ platform connection settings.
// It can be parsed from YAML and/or populated from environment variables.
type SYNQConfig struct {
	// OAuth client ID for authenticating with the SYNQ platform.
	ClientID string `yaml:"client_id,omitempty" json:"client_id,omitempty"`
	// OAuth client secret for authenticating with the SYNQ platform.
	ClientSecret string `yaml:"client_secret,omitempty" json:"client_secret,omitempty"`
	// gRPC endpoint for the SYNQ API (e.g. "developer.synq.io:443").
	Endpoint string `yaml:"endpoint,omitempty" json:"endpoint,omitempty"`
	// gRPC endpoint for the SYNQ ingest API. Defaults to Endpoint if empty.
	IngestEndpoint string `yaml:"ingest_endpoint,omitempty" json:"ingest_endpoint,omitempty"`
	// OAuth token URL. Derived from Endpoint if empty.
	OAuthURL string `yaml:"oauth_url,omitempty" json:"oauth_url,omitempty"`
}

// ApplyEnvDefaults fills empty fields from environment variables and defaults.
// Priority: existing value (from YAML) > env var > default.
func (c *SYNQConfig) ApplyEnvDefaults() {
	if c.ClientID == "" {
		c.ClientID = os.Getenv("SYNQ_CLIENT_ID")
	}
	if c.ClientSecret == "" {
		c.ClientSecret = os.Getenv("SYNQ_CLIENT_SECRET")
	}
	if c.Endpoint == "" {
		c.Endpoint = firstNonEmpty(
			os.Getenv("SYNQ_API_ENDPOINT"),
			os.Getenv("SYNQ_API_URL"),
			DefaultEndpoint,
		)
	}
	if c.IngestEndpoint == "" {
		c.IngestEndpoint = c.Endpoint
	}
	if c.OAuthURL == "" {
		c.OAuthURL = DeriveOAuthURL(c.Endpoint)
	}
}

// ToProto converts the config to a proto SYNQ message.
func (c *SYNQConfig) ToProto() *agentv1.SYNQ {
	return &agentv1.SYNQ{
		ClientId:       c.ClientID,
		ClientSecret:   c.ClientSecret,
		Endpoint:       c.Endpoint,
		IngestEndpoint: c.IngestEndpoint,
		OauthUrl:       c.OAuthURL,
	}
}

// DeriveOAuthURL derives the OAuth token URL from the gRPC endpoint.
// For "developer.synq.io:443" it returns "https://developer.synq.io/oauth2/token".
func DeriveOAuthURL(endpoint string) string {
	host, _, err := net.SplitHostPort(endpoint)
	if err != nil {
		host = endpoint
	}
	return fmt.Sprintf("https://%s/oauth2/token", host)
}

// DeriveAppURL derives the SYNQ app URL from the gRPC endpoint.
// For "developer.synq.io:443" it returns "https://app.synq.io".
// For "api.us.synq.io:443" it returns "https://app.us.synq.io".
func DeriveAppURL(endpoint string) string {
	host, _, err := net.SplitHostPort(endpoint)
	if err != nil {
		host = endpoint
	}
	for _, prefix := range []string{"developer.", "api."} {
		if strings.HasPrefix(host, prefix) {
			return "https://app." + strings.TrimPrefix(host, prefix)
		}
	}
	return "https://app.synq.io"
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
