package databricks

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/databricks/databricks-sdk-go"
	"github.com/stretchr/testify/require"
)

// These tests run in a synctest bubble, so a workspace issues tokens with the lifetime
// Databricks gives an OAuth M2M token, an hour, and the throttle uses the pacing every
// client gets, and both play out in virtual time. The fake workspace is served through
// an in-memory transport rather than a listening server so that it reads the same clock
// as the client: a token is expired when the bubble says it is.

const m2mTokenLifetime = time.Hour

// TestM2mWalkOutlivesTheFirstToken is the failure seen in production: a walk over a
// rate-limited workspace runs past the lifetime of the first token, and a request the
// workspace refuses in the last seconds of that token's life is held back and sent
// again after it expired, carrying it. The workspace answers that with Invalid Token
// and the whole walk fails.
func TestM2mWalkOutlivesTheFirstToken(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := newFakeOAuthWorkspace(t, "m2m-walk.test")
		// The workspace refuses every request from shortly before the first token
		// expires to shortly after, and asks for a wait that ends past its expiry.
		fake.refuse = func(sinceStart time.Duration) (time.Duration, bool) {
			from, until := m2mTokenLifetime-20*time.Second, m2mTokenLifetime+5*time.Second
			if sinceStart >= from && sinceStart < until {
				return until - sinceStart, true
			}
			return 0, false
		}

		client := fake.workspaceClient(t, &databricks.Config{})
		walkPastTheFirstToken(t, fake, client, time.Second)

		require.Positive(t, fake.refusedCount(), "the workspace never refused anything, so this test proves nothing")
		require.Empty(t, fake.rejected(), "a request reached the workspace with a token that had expired")
	})
}

// TestM2mTokenIsRenewedInTheStaleWindow pins when the token is renewed. The SDK's
// token cache renews a token in the background once it has at most twenty minutes
// (or half its lifetime) left, and that renewal has to get a new token, not the one
// the cache already holds: otherwise a new token is issued only ten seconds before the
// old one expires, which is less than the pacing may hold a request.
func TestM2mTokenIsRenewedInTheStaleWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const step = 10 * time.Second
		const staleWindow = 20 * time.Minute

		fake := newFakeOAuthWorkspace(t, "m2m-renewal.test")
		client := fake.workspaceClient(t, &databricks.Config{})
		walkPastTheFirstToken(t, fake, client, step)

		issued := fake.issuedAt()
		require.GreaterOrEqual(t, len(issued), 2, "the token was never renewed")
		renewedAt := issued[1]
		require.GreaterOrEqual(t, renewedAt, m2mTokenLifetime-staleWindow, "the token was renewed before it went stale")
		require.LessOrEqual(t, renewedAt, m2mTokenLifetime-staleWindow+step,
			"the token was renewed %s before it expired, not when it went stale", m2mTokenLifetime-renewedAt)

		// The renewal runs in the background, so the request that starts it still goes
		// out with the old token, and the next one carries the new token.
		for _, used := range fake.usedTokens() {
			if used.at > renewedAt {
				require.Equal(t, "token-2", used.token, "a request after the renewal at %s did not carry the new token", renewedAt)
				break
			}
		}
	})
}

// walkPastTheFirstToken sends requests one after another, every step, until well past
// the lifetime of the first token, the way a lineage walk reads one table after
// another.
func walkPastTheFirstToken(t *testing.T, fake *fakeOAuthWorkspace, client *databricks.WorkspaceClient, step time.Duration) {
	t.Helper()
	ctx := context.Background()
	for fake.sinceStart() < m2mTokenLifetime+5*time.Minute {
		_, err := client.CurrentUser.Me(ctx)
		require.NoError(t, err, "request at %s failed; tokens issued at %v, expired tokens sent: %v",
			fake.sinceStart(), fake.issuedAt(), fake.rejected())
		time.Sleep(step)
	}
}

// fakeOAuthWorkspace is a workspace that authenticates its API with OAuth M2M: it
// serves the OIDC discovery the SDK reads the token endpoint from, issues short-lived
// tokens, rejects an API request whose token has expired, and refuses requests for
// exceeding its quota whenever refuse says so.
type fakeOAuthWorkspace struct {
	host      string
	startedAt time.Time

	// refuse decides whether an API request that arrives this long after the start is
	// refused for exceeding the quota, and what Retry-After the refusal carries.
	refuse func(sinceStart time.Duration) (retryAfter time.Duration, refused bool)

	mu       sync.Mutex
	expiries map[string]time.Time
	// issued is when each token was issued, in the order they were.
	issued []time.Duration
	// used is the token each accepted API request carried, in the order they arrived.
	used         []usedToken
	refused      int
	rejectedWith []string
}

type usedToken struct {
	at    time.Duration
	token string
}

const (
	fakeClientId     = "test-client-id"
	fakeClientSecret = "test-client-secret"
)

func newFakeOAuthWorkspace(t *testing.T, name string) *fakeOAuthWorkspace {
	t.Helper()
	return &fakeOAuthWorkspace{
		host:      "https://" + name,
		startedAt: time.Now(),
		expiries:  map[string]time.Time{},
	}
}

func (f *fakeOAuthWorkspace) sinceStart() time.Duration {
	return time.Since(f.startedAt)
}

// workspaceClient builds a client the way the product does, through NewWorkspaceClient,
// from cfg with the workspace's host, the M2M credentials and the in-memory transport
// filled in. The pacing is the default one: in virtual time it costs nothing to wait it
// out, and it is what bounds how long a request is held in production.
func (f *fakeOAuthWorkspace) workspaceClient(t *testing.T, cfg *databricks.Config) *databricks.WorkspaceClient {
	t.Helper()
	UsePacing(f.host, DefaultPacing)
	cfg.Host = f.host
	cfg.ClientID = fakeClientId
	cfg.ClientSecret = fakeClientSecret
	cfg.HTTPTransport = handlerTransport{handler: http.HandlerFunc(f.serve)}
	// The SDK's own client-side limiter is turned up out of the way, as in the other
	// tests of the pacing.
	cfg.RateLimitPerSecond = 1000
	client, err := NewWorkspaceClient(cfg)
	require.NoError(t, err)
	return client
}

func (f *fakeOAuthWorkspace) serve(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/oidc/.well-known/oauth-authorization-server":
		writeJson(w, http.StatusOK, map[string]any{
			"authorization_endpoint": f.host + "/oidc/v1/authorize",
			"token_endpoint":         f.host + "/oidc/v1/token",
		})
	case r.URL.Path == "/oidc/v1/token":
		f.issueToken(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/"):
		f.serveApi(w, r)
	default:
		writeJson(w, http.StatusNotFound, map[string]any{"error_code": "NOT_FOUND", "message": "no such path " + r.URL.Path})
	}
}

func (f *fakeOAuthWorkspace) issueToken(w http.ResponseWriter, r *http.Request) {
	clientId, clientSecret, ok := r.BasicAuth()
	if !ok || clientId != fakeClientId || clientSecret != fakeClientSecret || r.Method != http.MethodPost {
		writeJson(w, http.StatusUnauthorized, map[string]any{"error": "invalid_client"})
		return
	}

	f.mu.Lock()
	token := fmt.Sprintf("token-%d", len(f.issued)+1)
	f.issued = append(f.issued, f.sinceStart())
	f.expiries[token] = time.Now().Add(m2mTokenLifetime)
	f.mu.Unlock()

	writeJson(w, http.StatusOK, map[string]any{
		"access_token": token,
		"token_type":   "Bearer",
		"expires_in":   int(m2mTokenLifetime.Seconds()),
		"scope":        "all-apis",
	})
}

func (f *fakeOAuthWorkspace) serveApi(w http.ResponseWriter, r *http.Request) {
	token, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	sinceStart := f.sinceStart()

	f.mu.Lock()
	expiry, known := f.expiries[token]
	if !known || !time.Now().Before(expiry) {
		f.rejectedWith = append(f.rejectedWith, fmt.Sprintf("%s at %s", token, sinceStart))
		f.mu.Unlock()
		writeJson(w, http.StatusUnauthorized, map[string]any{"error_code": "401", "message": "Invalid Token"})
		return
	}
	if f.refuse != nil {
		if retryAfter, refused := f.refuse(sinceStart); refused {
			f.refused++
			f.mu.Unlock()
			w.Header().Set("Retry-After", fmt.Sprint(int((retryAfter + time.Second - 1).Seconds())))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = fmt.Fprint(w, rateLimitedResponse)
			return
		}
	}
	f.used = append(f.used, usedToken{at: sinceStart, token: token})
	f.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, currentUserResponse)
}

func (f *fakeOAuthWorkspace) issuedAt() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Duration(nil), f.issued...)
}

func (f *fakeOAuthWorkspace) usedTokens() []usedToken {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]usedToken(nil), f.used...)
}

func (f *fakeOAuthWorkspace) refusedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.refused
}

func (f *fakeOAuthWorkspace) rejected() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.rejectedWith...)
}

func writeJson(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// handlerTransport serves a request with a handler in the calling goroutine, so the
// handler runs inside the test's bubble and reads its clock.
type handlerTransport struct {
	handler http.Handler
}

func (h handlerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		defer req.Body.Close()
	}
	recorder := httptest.NewRecorder()
	h.handler.ServeHTTP(recorder, req)
	resp := recorder.Result()
	resp.Request = req
	return resp, nil
}
