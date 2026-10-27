package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
)

const (
	workspaceServiceAccount = "ws-acme@quality-connector.iam.gserviceaccount.com"
	customerServiceAccount  = "quality@customer-project.iam.gserviceaccount.com"
)

// fakeGoogle stands for Google's token endpoint, the IAM credentials API and BigQuery. Tokens are
// named after the identity they belong to, so a test can see which identity made each call.
type fakeGoogle struct {
	*httptest.Server
	mu sync.Mutex
	// grants maps a service account to the identities allowed to mint its tokens.
	grants map[string][]string
	// generateCalls records "caller -> target" for every generateAccessToken request.
	generateCalls []string
	// apiCallers records the identity of every BigQuery request.
	apiCallers []string
	// scopes records the scopes asked for each target.
	scopes map[string][]string
	// stall, when set, holds every generateAccessToken request until it is closed.
	stall chan struct{}
}

func newFakeGoogle(t *testing.T) *fakeGoogle {
	t.Helper()
	f := &fakeGoogle{grants: map[string][]string{}, scopes: map[string][]string{}}
	f.Server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeGoogle) serve(w http.ResponseWriter, r *http.Request) {
	if f.stall != nil && strings.HasSuffix(r.URL.Path, ":generateAccessToken") {
		select {
		case <-f.stall:
		case <-r.Context().Done():
		}
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	caller := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer token-for-")
	switch {
	case r.URL.Path == "/token":
		writeJSON(w, map[string]any{"access_token": "token-for-pod", "token_type": "Bearer", "expires_in": 3600})
	case strings.HasSuffix(r.URL.Path, ":generateAccessToken"):
		target := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/projects/-/serviceAccounts/"), ":generateAccessToken")
		f.generateCalls = append(f.generateCalls, caller+" -> "+target)
		var body struct {
			Scope []string `json:"scope"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.scopes[target] = body.Scope
		allowed, exists := f.grants[target]
		if !exists {
			writeError(w, http.StatusNotFound, "Not found; Gaia id not found for email "+target)
			return
		}
		for _, a := range allowed {
			if a == caller {
				writeJSON(w, map[string]any{
					"accessToken": "token-for-" + target,
					"expireTime":  time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
				})
				return
			}
		}
		writeError(w, http.StatusForbidden, "Permission 'iam.serviceAccounts.getAccessToken' denied on resource (or it may not exist).")
	default:
		f.apiCallers = append(f.apiCallers, caller)
		writeJSON(w, map[string]any{"kind": "bigquery#datasetList"})
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": code, "message": message}})
}

// withFakeGoogle makes the process's default credentials and the IAM credentials API the fake's.
func withFakeGoogle(t *testing.T, f *fakeGoogle) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "adc.json")
	require.NoError(t, os.WriteFile(path, []byte(serviceAccountKey(t, f.URL+"/token")), 0o600))
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path)
	previous := iamCredentialsEndpoint
	iamCredentialsEndpoint = f.URL + "/"
	t.Cleanup(func() { iamCredentialsEndpoint = previous })
}

func (f *fakeGoogle) callBigQuery(t *testing.T, client *http.Client) error {
	t.Helper()
	resp, err := client.Get(f.URL + "/bigquery/v2/projects/customer-project/datasets")
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

func TestNewHTTPClient_ImpersonatesThroughTheChain(t *testing.T) {
	f := newFakeGoogle(t)
	withFakeGoogle(t, f)
	f.grants[workspaceServiceAccount] = []string{"pod"}
	f.grants[customerServiceAccount] = []string{workspaceServiceAccount}

	client, err := NewHTTPClient(context.Background(), &BigQueryConf{
		ProjectId:                  "customer-project",
		ImpersonateServiceAccounts: []string{workspaceServiceAccount, customerServiceAccount},
	}, bigquery.Scope)
	require.NoError(t, err)
	require.NoError(t, f.callBigQuery(t, client))
	require.NoError(t, f.callBigQuery(t, client))

	assert.Equal(t, []string{"pod -> " + workspaceServiceAccount, workspaceServiceAccount + " -> " + customerServiceAccount}, f.generateCalls,
		"each hop is minted once and then reused")
	assert.Equal(t, []string{customerServiceAccount, customerServiceAccount}, f.apiCallers)
	assert.Equal(t, []string{cloudPlatformScope}, f.scopes[workspaceServiceAccount], "an intermediate hop only needs to call iamcredentials")
	assert.Equal(t, []string{bigquery.Scope}, f.scopes[customerServiceAccount], "the last hop gets the scopes the caller asked for")
}

// The direct grant: the customer gives BigQuery roles to our service account, which is then the
// last and only hop.
func TestNewHTTPClient_ImpersonatesASingleServiceAccount(t *testing.T) {
	f := newFakeGoogle(t)
	withFakeGoogle(t, f)
	f.grants[workspaceServiceAccount] = []string{"pod"}

	client, err := NewHTTPClient(context.Background(), &BigQueryConf{
		ProjectId:                  "customer-project",
		ImpersonateServiceAccounts: []string{workspaceServiceAccount},
	}, bigquery.Scope)
	require.NoError(t, err)
	require.NoError(t, f.callBigQuery(t, client))

	assert.Equal(t, []string{workspaceServiceAccount}, f.apiCallers)
}

func TestNewHTTPClient_ImpersonationNamesTheHopThatWasRefused(t *testing.T) {
	cases := []struct {
		name       string
		grants     map[string][]string
		wantTarget string
		wantCaller string
		wantCode   int
	}{
		{
			name:       "the customer did not grant the token creator role",
			grants:     map[string][]string{workspaceServiceAccount: {"pod"}, customerServiceAccount: {}},
			wantTarget: customerServiceAccount,
			wantCaller: workspaceServiceAccount,
			wantCode:   http.StatusForbidden,
		},
		{
			name:       "the customer's service account does not exist",
			grants:     map[string][]string{workspaceServiceAccount: {"pod"}},
			wantTarget: customerServiceAccount,
			wantCaller: workspaceServiceAccount,
			wantCode:   http.StatusNotFound,
		},
		{
			name:       "our process may not impersonate the workspace service account",
			grants:     map[string][]string{workspaceServiceAccount: {}, customerServiceAccount: {workspaceServiceAccount}},
			wantTarget: workspaceServiceAccount,
			wantCaller: "",
			wantCode:   http.StatusForbidden,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeGoogle(t)
			withFakeGoogle(t, f)
			f.grants = tc.grants

			client, err := NewHTTPClient(context.Background(), &BigQueryConf{
				ProjectId:                  "customer-project",
				ImpersonateServiceAccounts: []string{workspaceServiceAccount, customerServiceAccount},
			}, bigquery.Scope)
			require.NoError(t, err)
			err = f.callBigQuery(t, client)
			require.Error(t, err)

			impErr, ok := AsImpersonationError(err)
			require.True(t, ok, "got %v", err)
			assert.Equal(t, tc.wantTarget, impErr.ServiceAccount)
			assert.Equal(t, tc.wantCaller, impErr.Caller)
			assert.True(t, impErr.Denied())
			var apiErr *googleapi.Error
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tc.wantCode, apiErr.Code)
			assert.True(t, IsAuthError(err), "a refused hop means no identity, so it is an auth error")
			assert.False(t, IsPermissionError(err), "a refused hop is not a missing BigQuery grant")
			assert.Empty(t, f.apiCallers, "BigQuery was called without a token")
		})
	}
}

func TestNewHTTPClient_ImpersonationRefusesAKeyAsWell(t *testing.T) {
	f := newFakeGoogle(t)
	withFakeGoogle(t, f)

	_, err := NewHTTPClient(context.Background(), &BigQueryConf{
		ProjectId:                  "customer-project",
		CredentialsJson:            serviceAccountKey(t, GoogleTokenURI),
		ImpersonateServiceAccounts: []string{workspaceServiceAccount},
	}, bigquery.Scope)
	require.Error(t, err)
	assert.Empty(t, f.generateCalls)
}

func TestNewHTTPClient_ImpersonationRefusesAnEmptyServiceAccount(t *testing.T) {
	f := newFakeGoogle(t)
	withFakeGoogle(t, f)

	_, err := NewHTTPClient(context.Background(), &BigQueryConf{
		ProjectId:                  "customer-project",
		ImpersonateServiceAccounts: []string{workspaceServiceAccount, ""},
	}, bigquery.Scope)
	require.Error(t, err)
	assert.Empty(t, f.generateCalls)
}

// The client is used long after the call that built it, often with a context that is canceled by
// then, and must still refresh its tokens.
func TestNewHTTPClient_ImpersonationOutlivesTheBuildContext(t *testing.T) {
	f := newFakeGoogle(t)
	withFakeGoogle(t, f)
	f.grants[workspaceServiceAccount] = []string{"pod"}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := NewHTTPClient(ctx, &BigQueryConf{
		ProjectId:                  "customer-project",
		ImpersonateServiceAccounts: []string{workspaceServiceAccount},
	}, bigquery.Scope)
	require.NoError(t, err)
	cancel()

	require.NoError(t, f.callBigQuery(t, client))
}

func TestImpersonationError(t *testing.T) {
	denied := &ImpersonationError{ServiceAccount: customerServiceAccount, Caller: workspaceServiceAccount, Err: &googleapi.Error{Code: 403}}
	assert.True(t, denied.Denied())
	assert.Contains(t, denied.Error(), workspaceServiceAccount+" cannot impersonate "+customerServiceAccount)

	first := &ImpersonationError{ServiceAccount: workspaceServiceAccount, Err: &googleapi.Error{Code: 403}}
	assert.Contains(t, first.Error(), "the default credentials cannot impersonate")

	unavailable := &ImpersonationError{ServiceAccount: customerServiceAccount, Err: &googleapi.Error{Code: 503}}
	assert.False(t, unavailable.Denied())
	assert.False(t, IsAuthError(unavailable), "Google being down is not a refused credential")

	wrapped := fmt.Errorf("listing datasets: %w", errors.WithStack(denied))
	got, ok := AsImpersonationError(wrapped)
	require.True(t, ok)
	assert.Same(t, denied, got)
}

// IAM can accept a token request and never answer. The request must give up after the configured
// timeout, or every BigQuery call waiting for that token waits forever too.
func TestNewHTTPClient_ImpersonationGivesUpOnAStalledTokenRequest(t *testing.T) {
	f := newFakeGoogle(t)
	withFakeGoogle(t, f)
	f.stall = make(chan struct{})
	t.Cleanup(func() { close(f.stall) })

	client, err := NewHTTPClient(context.Background(), &BigQueryConf{
		ProjectId:                  "customer-project",
		ImpersonateServiceAccounts: []string{workspaceServiceAccount},
		HTTPTimeout:                200 * time.Millisecond,
	}, bigquery.Scope)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- f.callBigQuery(t, client) }()
	select {
	case err := <-done:
		impErr, ok := AsImpersonationError(err)
		require.True(t, ok, "got %v", err)
		assert.Equal(t, workspaceServiceAccount, impErr.ServiceAccount)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(5 * time.Second):
		t.Fatal("the token request did not time out")
	}
}
