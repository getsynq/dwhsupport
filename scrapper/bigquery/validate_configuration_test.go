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

	dwhexecbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
	"github.com/stretchr/testify/require"
)

// The permission check authenticates the way the connection does. A connection that names no
// credential is refused rather than checked as the process's own identity.
func TestValidateConfiguration_NoCredentialIsRefused(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Error(w, "canary", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	adc, err := json.Marshal(map[string]string{
		"type":         "service_account",
		"private_key":  string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})),
		"client_email": "process@our-project.iam.gserviceaccount.com",
		"token_uri":    srv.URL,
	})
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "adc.json")
	require.NoError(t, os.WriteFile(path, adc, 0o600))
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path)

	scrapper := &BigQueryScrapper{conf: &BigQueryScrapperConf{BigQueryConf: dwhexecbigquery.BigQueryConf{ProjectId: "customer-project"}}}
	_, err = scrapper.ValidateConfiguration(context.Background())

	require.Error(t, err)
	require.Zero(t, hits.Load(), "the process's default credentials were used")
}
