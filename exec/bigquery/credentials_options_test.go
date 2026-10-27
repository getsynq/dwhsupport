package bigquery

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/exec"
	"github.com/stretchr/testify/require"
)

func TestNewHTTPClient_ServiceAccountKeyIsAccepted(t *testing.T) {
	for name, tokenURI := range map[string]string{"google token endpoint": GoogleTokenURI, "no token_uri": ""} {
		t.Run(name, func(t *testing.T) {
			_, err := NewHTTPClient(
				context.Background(),
				&BigQueryConf{ProjectId: "customer-project", CredentialsJson: serviceAccountKey(t, tokenURI)},
				bigquery.Scope,
			)
			require.NoError(t, err)
		})
	}
}

func TestNewHTTPClient_ServiceAccountKeyFileIsAccepted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "key.json")
	require.NoError(t, os.WriteFile(path, []byte(serviceAccountKey(t, GoogleTokenURI)), 0o600))

	_, err := NewHTTPClient(context.Background(), &BigQueryConf{ProjectId: "customer-project", CredentialsFile: path}, bigquery.Scope)
	require.NoError(t, err)
}

func TestNewHTTPClient_MissingCredentialsFile(t *testing.T) {
	_, err := NewHTTPClient(
		context.Background(),
		&BigQueryConf{
			ProjectId:              "customer-project",
			CredentialsFile:        filepath.Join(t.TempDir(), "missing.json"),
			AllowAnyCredentialType: true,
		},
		bigquery.Scope,
	)
	require.Error(t, err)
}

// An access token is the user-level login: it needs no key and is never refused for lacking one.
func TestNewHTTPClient_AccessTokenNeedsNoKey(t *testing.T) {
	c := newCanary(t)
	withDefaultCredentials(t, c)

	_, err := NewHTTPClient(context.Background(), &BigQueryConf{ProjectId: "customer-project", AccessToken: "ya29.token"}, bigquery.Scope)
	require.NoError(t, err)
}

// The agent runs in the customer's environment, so its host's own identity is theirs to use.
func TestNewHTTPClient_DefaultCredentialsWhenAllowed(t *testing.T) {
	c := newCanary(t)
	withDefaultCredentials(t, c)

	client, err := NewHTTPClient(
		context.Background(),
		&BigQueryConf{ProjectId: "customer-project", AllowDefaultCredentials: true},
		bigquery.Scope,
	)
	require.NoError(t, err)
	useClient(client)
	require.NotZero(t, c.hits.Load(), "the default credentials were not used")
}

// Allowing default credentials does not loosen what an explicit credential may be.
func TestNewHTTPClient_DefaultCredentialsDoNotAllowOtherTypes(t *testing.T) {
	c := newCanary(t)

	_, err := NewHTTPClient(
		context.Background(),
		&BigQueryConf{ProjectId: "customer-project", CredentialsJson: externalAccountCredentials(t, c.URL), AllowDefaultCredentials: true},
		bigquery.Scope,
	)
	require.Error(t, err)
	require.Zero(t, c.hits.Load())
}

// The agent's configuration comes from the host's owner, who may use workload identity federation.
func TestNewHTTPClient_AnyCredentialTypeWhenAllowed(t *testing.T) {
	c := newCanary(t)

	client, err := NewHTTPClient(
		context.Background(),
		&BigQueryConf{ProjectId: "customer-project", CredentialsJson: externalAccountCredentials(t, c.URL), AllowAnyCredentialType: true},
		bigquery.Scope,
	)
	require.NoError(t, err)
	useClient(client)
	require.NotZero(t, c.hits.Load(), "the external_account credential was not used")
}

// A credential that is refused is the customer's to change, so it reads as one the warehouse rejected.
func TestNewHTTPClient_RefusedCredentialIsAnAuthError(t *testing.T) {
	for name, conf := range map[string]*BigQueryConf{
		"no credential":    {ProjectId: "customer-project"},
		"external_account": {ProjectId: "customer-project", CredentialsJson: externalAccountCredentials(t, "http://169.254.169.254/")},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewHTTPClient(context.Background(), conf, bigquery.Scope)
			var authErr *exec.AuthError
			require.ErrorAs(t, err, &authErr)
		})
	}
	_, err := NewHTTPClient(context.Background(), &BigQueryConf{ProjectId: "customer-project"}, bigquery.Scope)
	require.ErrorIs(t, err, ErrNoCredential)
}

func TestNewHTTPClient_AnyCredentialTypeStillNeedsAType(t *testing.T) {
	_, err := NewHTTPClient(
		context.Background(),
		&BigQueryConf{ProjectId: "customer-project", CredentialsJson: `{"client_email":"x"}`, AllowAnyCredentialType: true},
		bigquery.Scope,
	)
	require.Error(t, err)
}

func TestValidateServiceAccountKey(t *testing.T) {
	valid := serviceAccountKey(t, GoogleTokenURI)
	without := func(field string) string {
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(valid), &m))
		delete(m, field)
		out, err := json.Marshal(m)
		require.NoError(t, err)
		return string(out)
	}

	require.NoError(t, ValidateServiceAccountKey([]byte(valid)))
	require.NoError(t, ValidateServiceAccountKey([]byte(without("token_uri"))))

	for name, key := range map[string]string{
		"empty":                "",
		"not json":             "not json",
		"json array":           "[]",
		"no type":              without("type"),
		"no client_email":      without("client_email"),
		"no private_key":       without("private_key"),
		"foreign token_uri":    serviceAccountKey(t, "http://169.254.169.254/computeMetadata/v1/"),
		"external_account":     externalAccountCredentials(t, "http://169.254.169.254/"),
		"authorized_user":      `{"type":"authorized_user","client_id":"x","client_secret":"y","refresh_token":"z"}`,
		"impersonated account": `{"type":"impersonated_service_account","service_account_impersonation_url":"http://x"}`,
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, ValidateServiceAccountKey([]byte(key)))
		})
	}
}
