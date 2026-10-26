package bigquery

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
)

// canary is a server that a credential must never make the process call: it stands for the
// metadata server or any other address reachable only from inside our network.
type canary struct {
	*httptest.Server
	hits atomic.Int64
}

func newCanary(t *testing.T) *canary {
	t.Helper()
	c := &canary{}
	c.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.hits.Add(1)
		http.Error(w, "canary", http.StatusInternalServerError)
	}))
	t.Cleanup(c.Close)
	return c
}

func serviceAccountKey(t *testing.T, tokenURI string) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	keyPem := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	credentials, err := json.Marshal(map[string]string{
		"type":           "service_account",
		"project_id":     "customer-project",
		"private_key_id": "0000000000000000000000000000000000000000",
		"private_key":    string(keyPem),
		"client_email":   "quality@customer-project.iam.gserviceaccount.com",
		"client_id":      "000000000000000000000",
		"token_uri":      tokenURI,
	})
	require.NoError(t, err)
	return string(credentials)
}

func externalAccountCredentials(t *testing.T, sourceURL string) string {
	t.Helper()
	credentials, err := json.Marshal(map[string]any{
		"type":               "external_account",
		"audience":           "//iam.googleapis.com/projects/0/locations/global/workloadIdentityPools/p/providers/p",
		"subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
		"token_url":          "https://sts.googleapis.com/v1/token",
		"credential_source":  map[string]any{"url": sourceURL},
	})
	require.NoError(t, err)
	return string(credentials)
}

// withDefaultCredentials points Application Default Credentials at a key whose token endpoint is the
// canary, so any fallback to the process's own identity shows as a hit.
func withDefaultCredentials(t *testing.T, c *canary) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "adc.json")
	require.NoError(t, os.WriteFile(path, []byte(serviceAccountKey(t, c.URL)), 0o600))
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path)
}

// useClient makes one authenticated request, which is when a credential fetches its token.
func useClient(client *http.Client) {
	resp, err := client.Get("https://bigquery.googleapis.com/bigquery/v2/projects/customer-project/datasets")
	if err == nil {
		_ = resp.Body.Close()
	}
}

// A configuration that names no credential must not authenticate as the process running it: on the
// hosted side that is our own service account, which other customers may have granted access.
func TestNewHTTPClient_NoCredentialIsRefused(t *testing.T) {
	c := newCanary(t)
	withDefaultCredentials(t, c)

	client, err := NewHTTPClient(context.Background(), &BigQueryConf{ProjectId: "customer-project"}, bigquery.Scope)
	if err == nil {
		useClient(client)
	}

	require.Error(t, err)
	require.Zero(t, c.hits.Load(), "the process's default credentials were used")
}

// An external_account credential reads its subject token from a file or URL it names, so one supplied
// by a customer would make the process read that file or call that URL.
func TestNewHTTPClient_ExternalAccountIsRefused(t *testing.T) {
	c := newCanary(t)

	client, err := NewHTTPClient(
		context.Background(),
		&BigQueryConf{ProjectId: "customer-project", CredentialsJson: externalAccountCredentials(t, c.URL)},
		bigquery.Scope,
	)
	if err == nil {
		useClient(client)
	}

	require.Error(t, err)
	require.Zero(t, c.hits.Load(), "the credential's source URL was called")
}

// A service account key's token_uri is where the process posts the signed assertion and reads the
// answer from, so a key naming another address turns it into a request to that address.
func TestNewHTTPClient_ServiceAccountKeyWithForeignTokenURIIsRefused(t *testing.T) {
	c := newCanary(t)

	client, err := NewHTTPClient(
		context.Background(),
		&BigQueryConf{ProjectId: "customer-project", CredentialsJson: serviceAccountKey(t, c.URL)},
		bigquery.Scope,
	)
	if err == nil {
		useClient(client)
	}

	require.Error(t, err)
	require.Zero(t, c.hits.Load(), "the key's token_uri was called")
}

func TestNewHTTPClient_ServiceAccountKeyFileWithForeignTokenURIIsRefused(t *testing.T) {
	c := newCanary(t)
	path := filepath.Join(t.TempDir(), "key.json")
	require.NoError(t, os.WriteFile(path, []byte(serviceAccountKey(t, c.URL)), 0o600))

	client, err := NewHTTPClient(context.Background(), &BigQueryConf{ProjectId: "customer-project", CredentialsFile: path}, bigquery.Scope)
	if err == nil {
		useClient(client)
	}

	require.Error(t, err)
	require.Zero(t, c.hits.Load(), "the key's token_uri was called")
}
