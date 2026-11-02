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
	"strconv"
	"strings"
	"sync"
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
