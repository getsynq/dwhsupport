package snowflake

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/getsynq/dwhsupport/exec"
	gosnowflake "github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	fakeLoginOK = `{"success":true,"data":{"token":"tok","masterToken":"mtok","sessionId":1,"parameters":[]}}`
	fakeQueryOK = `{"success":true,"data":{"parameters":[],"rowtype":[],"rowset":[],"total":0,"returned":0,` +
		`"queryId":"q1","queryResultFormat":"json"}}`
)

// fakeLoginFailure renders the driver's login-failure envelope. Marshalled rather than
// concatenated so a message containing a quote yields a valid body, and the test fails
// on the scenario rather than on a JSON decode error.
func fakeLoginFailure(code int, msg string) string {
	body, err := json.Marshal(struct {
		Success bool            `json:"success"`
		Code    string          `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}{Code: strconv.Itoa(code), Message: msg, Data: json.RawMessage(`{}`)})
	if err != nil {
		panic(err)
	}
	return string(body)
}

// fakeSnowflake serves canned driver responses so the connect/retry path can be driven
// in-process. It records every login attempt's query string, which is where the driver
// puts databaseName — that is what makes "did we retry, and did we drop only the
// database?" observable without a warehouse.
//
// loginBody is keyed on the request rather than the attempt number so a retry succeeds
// *because* the offending parameter is gone, not merely because it happened second.
type fakeSnowflake struct {
	mu        sync.Mutex
	logins    []string
	loginBody func(query string) string
}

func (f *fakeSnowflake) attempts() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.logins...)
}

func (f *fakeSnowflake) RoundTrip(req *http.Request) (*http.Response, error) {
	// RoundTrip must always close the request body, per the http.RoundTripper contract.
	if req.Body != nil {
		defer func() { _ = req.Body.Close() }()
	}

	respond := func(body string) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Request:    req,
		}, nil
	}

	switch {
	case strings.Contains(req.URL.Path, "login-request"):
		query := req.URL.RawQuery
		f.mu.Lock()
		f.logins = append(f.logins, query)
		f.mu.Unlock()
		return respond(f.loginBody(query))
	case strings.Contains(req.URL.Path, "query-request"):
		return respond(fakeQueryOK)
	default:
		return respond(`{"success":true,"data":{}}`)
	}
}

func fakeConf(fake *fakeSnowflake, databases ...string) *SnowflakeConf {
	return &SnowflakeConf{
		Account:     "testaccount",
		User:        "test_user",
		Password:    "test_password",
		Warehouse:   "TEST_WH",
		Role:        "TEST_ROLE",
		Databases:   databases,
		Transporter: fake,
	}
}

// A resolvable first database must stay the session default: callers issue unqualified
// SQL against it (custom metrics, QueryShape, data preview), so dropping it wholesale
// would break them silently.
func TestConnectKeepsResolvableDefaultDatabase(t *testing.T) {
	fake := &fakeSnowflake{loginBody: func(string) string { return fakeLoginOK }}

	execer, err := NewSnowflakeExecutor(context.Background(), fakeConf(fake, "GOOD_DB", "OTHER_DB"))
	require.NoError(t, err)
	defer func() { _ = execer.Close() }()

	attempts := fake.attempts()
	require.Len(t, attempts, 1, "a working connection must not be retried")
	assert.Contains(t, attempts[0], "databaseName=GOOD_DB")
}

// Regression: an unresolvable session default must not refuse the whole connection.
func TestConnectRetriesWithoutUnresolvableDefaultDatabase(t *testing.T) {
	fake := &fakeSnowflake{loginBody: func(query string) string {
		if strings.Contains(query, "databaseName=NO_SUCH_DB") {
			return fakeLoginFailure(gosnowflake.ErrObjectNotExistOrAuthorized, "The requested database does not exist or not authorized.")
		}
		return fakeLoginOK
	}}

	ctx := context.Background()
	conf := fakeConf(fake, "NO_SUCH_DB", "GOOD_DB")
	execer, err := NewSnowflakeExecutor(ctx, conf)
	require.NoError(t, err, "connection must survive an unresolvable session default database")
	require.NotNil(t, execer)
	defer func() { _ = execer.Close() }()

	// The scrapper aliases this same conf, and its per-database validation reads the
	// slice — pruning the unresolvable entry here would delete the very warning this
	// fix exists to surface.
	assert.Equal(t, []string{"NO_SUCH_DB", "GOOD_DB"}, conf.Databases,
		"retry must not prune the caller's configured databases")

	attempts := fake.attempts()
	require.Len(t, attempts, 2, "expected exactly one retry")
	assert.Contains(t, attempts[0], "databaseName=NO_SUCH_DB")
	assert.NotContains(t, attempts[1], "databaseName", "retry must drop the session default database")
	// Only the database is dropped — clearing the warehouse or role too would silently
	// change which compute runs the queries, or the privileges they run under.
	assert.Contains(t, attempts[1], "warehouse=TEST_WH")
	assert.Contains(t, attempts[1], "roleName=TEST_ROLE")

	// The executor must hold the surviving pool, not the one that was closed.
	var got []int
	require.NoError(t, execer.Select(ctx, &got, "SELECT 1"), "returned executor must be usable")
}

// The retry only makes sense when the database is the sole problem. Everything else must
// fail on the first attempt, or a genuinely wrong credential would be tried twice and a
// missing warehouse would be "fixed" by dropping something unrelated.
func TestConnectDoesNotRetryForOtherFailures(t *testing.T) {
	tests := []struct {
		name      string
		databases []string
		token     string
		code      int
		message   string
	}{
		{
			name:      "warehouse shares the error code",
			databases: []string{"GOOD_DB"},
			code:      gosnowflake.ErrObjectNotExistOrAuthorized,
			message:   "The requested warehouse does not exist or not authorized.",
		},
		{
			name:      "bad credentials",
			databases: []string{"GOOD_DB"},
			code:      errIncorrectUsernameOrPassword,
			message:   "Incorrect username or password was specified.",
		},
		{
			// No database configured means no session default to drop, so there is
			// nothing a retry could change.
			name:      "no database configured",
			databases: nil,
			code:      gosnowflake.ErrObjectNotExistOrAuthorized,
			message:   "The requested database does not exist or not authorized.",
		},
		{
			// User-level auth deliberately connects without a session default even when
			// databases are configured, so there is likewise nothing to drop.
			name:      "user auth never sets a session default",
			databases: []string{"GOOD_DB"},
			token:     "oauth-token",
			code:      gosnowflake.ErrObjectNotExistOrAuthorized,
			message:   "The requested database does not exist or not authorized.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeSnowflake{loginBody: func(string) string {
				return fakeLoginFailure(tt.code, tt.message)
			}}
			conf := fakeConf(fake, tt.databases...)
			conf.Token = tt.token

			_, err := NewSnowflakeExecutor(context.Background(), conf)
			require.Error(t, err)
			assert.Len(t, fake.attempts(), 1, "must not retry")

			// The original failure must reach the caller unaltered: callers branch on
			// AuthError to decide severity, and re-wrapping would misreport the cause.
			var authErr *exec.AuthError
			require.ErrorAs(t, err, &authErr)
			assert.Contains(t, err.Error(), tt.message)
			assert.NotContains(t, err.Error(), "reconnect", "a failure that was never retried must not claim it was")
		})
	}
}

// A retry that also fails must surface both causes: the second error can be unrelated
// (a tight deadline, a transient fault) and would otherwise bury the actionable one.
func TestConnectRetryFailureKeepsOriginalCause(t *testing.T) {
	fake := &fakeSnowflake{loginBody: func(query string) string {
		if strings.Contains(query, "databaseName=NO_SUCH_DB") {
			return fakeLoginFailure(gosnowflake.ErrObjectNotExistOrAuthorized, "The requested database does not exist or not authorized.")
		}
		return fakeLoginFailure(errIncorrectUsernameOrPassword, "Incorrect username or password was specified.")
	}}

	_, err := NewSnowflakeExecutor(context.Background(), fakeConf(fake, "NO_SUCH_DB"))
	require.Error(t, err)
	require.Len(t, fake.attempts(), 2)

	var authErr *exec.AuthError
	require.ErrorAs(t, err, &authErr)
	assert.Contains(t, err.Error(), "Incorrect username or password", "retry failure should be reported")
	assert.Contains(t, err.Error(), "The requested database does not exist", "original cause must survive")
}
