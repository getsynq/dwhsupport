package snowflake

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/suite"
)

type ChunkDownloadSuite struct {
	suite.Suite
}

func TestChunkDownloadSuite(t *testing.T) {
	suite.Run(t, new(ChunkDownloadSuite))
}

// unreachableChunkURL is a result chunk on a port nothing listens on, the way a
// chunk served from a private endpoint looks from outside the customer's network.
func (s *ChunkDownloadSuite) unreachableChunkURL() string {
	return "http://" + s.closedAddr() + "/results/data_0_0_0?X-Amz-Signature=deadbeef"
}

// closedAddr is a local address nothing listens on, so a dial to it fails at once.
func (s *ChunkDownloadSuite) closedAddr() string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	addr := listener.Addr().String()
	s.Require().NoError(listener.Close())
	return addr
}

// fakeSnowflakeAPI answers a login and a query whose result is one chunk at
// chunkURL, so the driver has to download it. Everything else is acknowledged.
func fakeSnowflakeAPI(chunkURL string) *httptest.Server {
	reply := func(w http.ResponseWriter, data map[string]any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": data})
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/session/v1/login-request", func(w http.ResponseWriter, r *http.Request) {
		reply(w, map[string]any{
			"token":       "session-token",
			"masterToken": "master-token",
			"sessionId":   1,
			"sessionInfo": map[string]any{"databaseName": "DB"},
			"parameters":  []any{},
		})
	})
	mux.HandleFunc("/queries/v1/query-request", func(w http.ResponseWriter, r *http.Request) {
		reply(w, map[string]any{
			"queryId":           "01c8-0000",
			"queryResultFormat": "json",
			"rowtype":           []map[string]any{{"name": "NAME", "type": "text", "nullable": true}},
			"rowset":            [][]any{},
			"total":             1,
			"returned":          0,
			"qrmk":              "qrmk",
			"chunks":            []map[string]any{{"url": chunkURL, "rowCount": 1, "uncompressedSize": 16}},
		})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		reply(w, map[string]any{})
	})
	return httptest.NewServer(mux)
}

func (s *ChunkDownloadSuite) openAgainst(server *httptest.Server, requestTimeout time.Duration) *sql.DB {
	return s.openAgainstWith(server, requestTimeout, nil)
}

func (s *ChunkDownloadSuite) openAgainstWith(server *httptest.Server, requestTimeout time.Duration, transporter http.RoundTripper) *sql.DB {
	c, err := buildSnowflakeConfig(&SnowflakeConf{Account: "fake", User: "reader", Password: "secret", Transporter: transporter})
	s.Require().NoError(err)

	serverURL, err := url.Parse(server.URL)
	s.Require().NoError(err)
	port, err := strconv.Atoi(serverURL.Port())
	s.Require().NoError(err)
	c.Protocol = "http"
	c.Host = serverURL.Hostname()
	c.Port = port
	c.DisableTelemetry = true
	c.RequestTimeout = requestTimeout

	return sql.OpenDB(gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, *c))
}

func (s *ChunkDownloadSuite) readAll(db *sql.DB, ctx context.Context) error {
	rows, err := db.QueryContext(ctx, "select NAME from T")
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
	}
	return rows.Err()
}

// The driver retries a chunk download with no limit unless the connection sets
// one, so an unreachable chunk keeps the read waiting until the caller's own
// deadline, and all the caller learns is that it ran out of time.
func (s *ChunkDownloadSuite) TestTheConnectionBoundsAChunkDownload() {
	c, err := buildSnowflakeConfig(&SnowflakeConf{Account: "fake", User: "reader", Password: "secret"})
	s.Require().NoError(err)
	s.Positive(c.RequestTimeout)
}

// With a bound, the read fails with the download's own error, which names the
// host the chunk was to come from.
func (s *ChunkDownloadSuite) TestAnUnreachableChunkFailsTheReadWithTheDownloadError() {
	server := fakeSnowflakeAPI(s.unreachableChunkURL())
	defer server.Close()
	db := s.openAgainst(server, time.Second)
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	err := s.readAll(db, ctx)

	var urlErr *url.Error
	s.Require().ErrorAs(err, &urlErr)
	s.NoError(ctx.Err(), "the read must fail before the caller's deadline")
}

// Without a bound the same read only ends at the caller's deadline.
func (s *ChunkDownloadSuite) TestWithoutABoundTheReadWaitsForTheCallersDeadline() {
	server := fakeSnowflakeAPI(s.unreachableChunkURL())
	defer server.Close()
	db := s.openAgainst(server, 0)
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.readAll(db, ctx)

	s.True(errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded), "got %v", err)
}

// A chunk on a private endpoint is reachable only from the customer's network,
// so once it fails to dial, the driver's retries are answered from memory instead
// of each waiting out another dial timeout.
func (s *ChunkDownloadSuite) TestAPrivateEndpointIsDialedOnceAcrossRetries() {
	closed := s.closedAddr()
	var mu sync.Mutex
	privateDials := 0
	base := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		if strings.Contains(addr, ".vpce.") {
			mu.Lock()
			privateDials++
			mu.Unlock()
			addr = closed
		}
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}}
	server := fakeSnowflakeAPI("http://sfc-stage.bucket.vpce-0123456789abcdef0-abcd1234.s3.ap-southeast-2.vpce.amazonaws.com/results/data_0_0_0?X-Amz-Signature=deadbeef")
	defer server.Close()
	db := s.openAgainstWith(server, time.Second, base)
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	err := s.readAll(db, ctx)

	var urlErr *url.Error
	s.Require().ErrorAs(err, &urlErr)
	s.Contains(urlErr.URL, "vpce-0123456789abcdef0")
	mu.Lock()
	defer mu.Unlock()
	s.Equal(1, privateDials)
}

// failingTransport fails every request with err and counts them.
type failingTransport struct {
	err   error
	calls int
}

func (t *failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	t.calls++
	return nil, t.err
}

func privateEndpointRequest(ctx context.Context) *http.Request {
	req, _ := http.NewRequestWithContext(
		ctx, http.MethodGet, "https://sfc-stage.bucket.vpce-0123456789abcdef0-abcd1234.s3.ap-southeast-2.vpce.amazonaws.com/results", nil,
	)
	return req
}

// Only a dial the endpoint's network turned away says it is unreachable. A dial
// cut short by its caller, or a name that did not resolve this once, says
// nothing about the next request, and remembering it would fail every chunk
// read on the connection for a while.
func (s *ChunkDownloadSuite) TestOnlyAnUnreachableEndpointIsRemembered() {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	// A dial whose own timeout has already passed fails the way one does that
	// waited it out, with the error the dialer reports for a timeout.
	_, dialTimeout := (&net.Dialer{Timeout: time.Nanosecond}).Dial("tcp", "10.255.255.1:443")
	s.Require().Error(dialTimeout)
	_, callerCanceled := (&net.Dialer{}).DialContext(cancelled, "tcp", "10.255.255.1:443")
	s.Require().Error(callerCanceled)

	for name, tc := range map[string]struct {
		ctx      context.Context
		err      error
		remember bool
	}{
		"refused": {
			ctx: context.Background(), remember: true,
			err: &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)},
		},
		"timed out": {
			ctx: context.Background(), remember: true,
			err: dialTimeout,
		},
		"host unreachable": {
			ctx: context.Background(), remember: true,
			err: &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.EHOSTUNREACH)},
		},
		"canceled by the caller": {
			ctx: context.Background(),
			err: callerCanceled,
		},
		"timed out on the caller's deadline": {
			ctx: cancelled,
			err: dialTimeout,
		},
		"caller's context done": {
			ctx: cancelled,
			err: &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)},
		},
		"name not resolved": {
			ctx: context.Background(),
			err: &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", IsTimeout: true}},
		},
	} {
		base := &failingTransport{err: tc.err}
		transport := newPrivateEndpointTransport(base)

		_, _ = transport.RoundTrip(privateEndpointRequest(tc.ctx))
		_, _ = transport.RoundTrip(privateEndpointRequest(context.Background()))

		if tc.remember {
			s.Equal(1, base.calls, name)
		} else {
			s.Equal(2, base.calls, name)
		}
	}
}

// An account reached over PrivateLink has `.privatelink.` in its own host. A
// refused dial to it is the account being unreachable for a moment, and
// remembering it would fail every query on the connection for a while, so only
// stage hosts are remembered.
func (s *ChunkDownloadSuite) TestAPrivateLinkAccountHostIsNotRemembered() {
	for _, accountURL := range []string{
		"https://acme.privatelink.snowflakecomputing.com:443/queries/v1/query-request",
		// The China regions serve accounts under their own domain.
		"https://xy12345.cn-northwest-1.privatelink.snowflakecomputing.cn:443/queries/v1/query-request",
	} {
		base := &failingTransport{err: &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}}
		transport := newPrivateEndpointTransport(base)
		accountRequest := func() *http.Request {
			req, _ := http.NewRequest(http.MethodPost, accountURL, nil)
			return req
		}

		_, _ = transport.RoundTrip(accountRequest())
		_, _ = transport.RoundTrip(accountRequest())

		s.Equal(2, base.calls, accountURL)
	}
}
