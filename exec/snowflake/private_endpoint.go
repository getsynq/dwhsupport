package snowflake

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/snowflakedb/gosnowflake"
)

// requestTimeout bounds how long the driver retries one failed HTTP request,
// a result chunk download included. The driver's default is no bound at all, so
// a chunk it cannot reach keeps a read waiting until the caller's own deadline,
// with nothing to say why. The time a dial takes does not count against it.
const requestTimeout = time.Minute

// unreachableEndpointMemory is how long a private endpoint that could not be
// dialed is answered from memory instead of being dialed again.
const unreachableEndpointMemory = 10 * time.Minute

// isPrivateEndpoint says whether host is a stage behind a VPC endpoint or
// PrivateLink. Snowflake serves the results of an account with PrivateLink on
// its internal stage from such a host, and it resolves to an address in the
// customer's own network. An account reached over PrivateLink has
// `.privatelink.` in its own host too, and a failed dial to it is not a stage
// out of reach, so Snowflake's own hosts are left out.
func isPrivateEndpoint(host string) bool {
	host = strings.ToLower(host)
	if strings.HasSuffix(host, ".snowflakecomputing.com") {
		return false
	}
	return strings.Contains(host, ".vpce.") || strings.Contains(host, ".privatelink.")
}

// privateEndpointTransport fails a request at once to a private endpoint that
// could not be dialed a moment ago. Such an endpoint is reachable only from the
// customer's network, so every retry would wait out a full dial timeout again,
// and the driver retries a chunk several times over, each one with several
// attempts. The first failure is what each later request returns, so the error
// still names the endpoint and says what happened when it was dialed.
type privateEndpointTransport struct {
	base http.RoundTripper
	now  func() time.Time

	mu          sync.Mutex
	unreachable map[string]unreachableEndpoint
}

type unreachableEndpoint struct {
	err   error
	until time.Time
}

// baseTransport is the transport the driver would use on its own: the caller's,
// or the driver's default, which checks certificate revocation.
func baseTransport(configured http.RoundTripper) http.RoundTripper {
	if configured != nil {
		return configured
	}
	return gosnowflake.SnowflakeTransport
}

func newPrivateEndpointTransport(base http.RoundTripper) *privateEndpointTransport {
	return &privateEndpointTransport{base: base, now: time.Now, unreachable: map[string]unreachableEndpoint{}}
}

func (t *privateEndpointTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	host := req.URL.Hostname()
	if !isPrivateEndpoint(host) {
		return t.base.RoundTrip(req)
	}
	if err := t.remembered(host); err != nil {
		return nil, err
	}
	resp, err := t.base.RoundTrip(req)
	if err != nil && req.Context().Err() == nil && isUnreachable(err) {
		t.remember(host, err)
	}
	return resp, err
}

// isUnreachable says whether a failed request is a dial the endpoint's network
// turned away: refused, unroutable, or never answered within the dial timeout.
// A dial its caller cut short and a name that did not resolve say nothing about
// the next request, so they are not remembered.
//
// The dialer reports its own timeout and the caller's deadline as the same
// error, so the caller passes only a request whose context is still live, and
// a timeout here is the dialer's.
func isUnreachable(err error) bool {
	var opErr *net.OpError
	if !errors.As(err, &opErr) || opErr.Op != "dial" {
		return false
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) || errors.Is(err, context.Canceled) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.ENETUNREACH)
}

func (t *privateEndpointTransport) remembered(host string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.unreachable[host]
	if !ok {
		return nil
	}
	if t.now().After(entry.until) {
		delete(t.unreachable, host)
		return nil
	}
	return fmt.Errorf("private endpoint %s was not reachable from this network: %w", host, entry.err)
}

func (t *privateEndpointTransport) remember(host string, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.unreachable[host] = unreachableEndpoint{err: err, until: t.now().Add(unreachableEndpointMemory)}
}
