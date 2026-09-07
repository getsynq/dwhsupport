package databricks

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/apierr"
	"github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/httpclient"
	"github.com/databricks/databricks-sdk-go/useragent"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
)

// Every Databricks REST client in this process is built here, and that is the whole
// point of the two constructors below.
//
// A workspace's control-plane quota is shared by everything pointed at it — the
// catalog listings and per-table reads of a metrics scrape, and, in the cloud repo,
// the Unity Catalog lineage walk enrolled from the same integration. Both observe the
// same signal, a refusal from the same workspace, and both back off multiplicatively
// on it, so they converge on a share of whatever the real quota is the way two TCP
// flows converge on a share of a link. No shared budget, no cross-process protocol;
// the only thing that has to be true is that every client runs the loop, and it does
// because there is one place left that builds one.

// pacedHttpTimeoutSeconds is the per-attempt timeout the SDK puts on a request when
// the pacing is installed. The SDK's timeout is an inactivity deadline that starts
// before the request is sent, so it also has to cover the time a request spends held
// back by the throttle — with the SDK's own 60s default, a refusal waited out inside
// a single attempt would instead time the request out mid-flight. Only used when the
// caller has not set a timeout of its own.
const pacedHttpTimeoutSeconds = 120

// NewWorkspaceClient builds the Databricks workspace client, paced against the
// workspace's control-plane quota. Every caller goes through it rather than
// databricks.NewWorkspaceClient: a client built with stock SDK defaults retries a
// refusal blindly for five minutes, ignores Retry-After and never reduces the rate it
// offers, which leaves it absorbing the quota that the callers which do pace
// themselves have given up.
func NewWorkspaceClient(cfg *databricks.Config) (*databricks.WorkspaceClient, error) {
	registerUserAgent()

	client, err := databricks.NewWorkspaceClient(WithPacing(cfg))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return client, nil
}

// NewApiClient builds the raw client for endpoints the SDK does not model — Unity
// Catalog's table-lineage endpoint is the one that matters — paced by the same
// throttle as the workspace client of the same workspace.
//
// Unlike the workspace client, this one can be told not to retry a refusal itself,
// and is: the retry belongs to the caller, which is what holds the throttle and can
// wait the refusal out at the pace the whole process has converged on.
func NewApiClient(cfg *databricks.Config) (*httpclient.ApiClient, error) {
	registerUserAgent()

	clientConfig, err := config.HTTPClientConfigFromConfig((*config.Config)(WithPacing(cfg)))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create databricks api client")
	}
	clientConfig.ErrorRetriable = func(ctx context.Context, err error) bool {
		if IsRateLimitError(err) {
			return false
		}
		var apiErr *apierr.APIError
		if errors.As(err, &apiErr) {
			return apiErr.IsRetriable(ctx)
		}
		return false
	}
	// The SDK also retries anything whose message contains REQUEST_LIMIT_EXCEEDED, a
	// workaround for SCIM answering 500 on a rate limit. It matches on the message
	// after ErrorRetriable has already said no, so it would put the refusals back
	// under the SDK's blind retry.
	clientConfig.TransientErrors = lo.Reject(clientConfig.TransientErrors, func(pattern string, _ int) bool {
		return pattern == "REQUEST_LIMIT_EXCEEDED"
	})
	return httpclient.NewApiClient(clientConfig), nil
}

// registerUserAgent names the product in the user agent every request carries. The
// SDK keeps that name in an unsynchronised package global, so it is written exactly
// once per process: a client built while another one is sending — the SQL executor a
// metrics scrape builds lazily, from inside its own errgroup — otherwise writes it
// while the requests in flight are reading it.
func registerUserAgent() {
	userAgentOnce.Do(func() {
		useragent.WithProduct("synq", "1.0.0")
	})
}

var userAgentOnce sync.Once

// WithPacing installs the rate-limit pacing on a config, and is what makes every
// client built from that config share one throttle per workspace. Idempotent, so a
// config that has already been paced — one taken off a client built here — can be
// handed to another constructor.
func WithPacing(cfg *databricks.Config) *databricks.Config {
	if cfg == nil {
		cfg = &databricks.Config{}
	}
	if _, alreadyPaced := cfg.HTTPTransport.(*pacedTransport); alreadyPaced {
		return cfg
	}
	cfg.HTTPTransport = &pacedTransport{base: baseTransport(cfg)}
	if cfg.HTTPTimeoutSeconds == 0 {
		cfg.HTTPTimeoutSeconds = pacedHttpTimeoutSeconds
	}
	return cfg
}

// baseTransport is what the pacing wraps. Setting a transport on the config takes the
// SDK's own choice of one away, so what it would have built has to be built here:
// its connection-pool settings, and the one config field it honours when picking a
// transport of its own.
func baseTransport(cfg *databricks.Config) http.RoundTripper {
	if cfg.HTTPTransport != nil {
		return cfg.HTTPTransport
	}
	if cfg.InsecureSkipVerify {
		transport := newDefaultTransport()
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		return transport
	}
	// Shared by every client, the way the SDK shares its own, so that clients of one
	// workspace reuse connections instead of each holding a pool of its own.
	return sharedTransport
}

var sharedTransport = newDefaultTransport()

// newDefaultTransport mirrors the transport the SDK installs when a config names
// none (databricks-sdk-go/httpclient.makeDefaultTransport).
func newDefaultTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   runtime.GOMAXPROCS(0) + 1,
		IdleConnTimeout:       180 * time.Second,
		TLSHandshakeTimeout:   30 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

// pacedTransport is where the pacing actually happens: every request one client sends
// passes through it, so the catalog listings, the per-table reads and the lineage walk
// pace against each other rather than each discovering the workspace's limit
// separately.
type pacedTransport struct {
	base http.RoundTripper
}

func (t *pacedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	throttle := throttleForHost(strings.ToLower(req.URL.Host), DefaultPacing)
	pacing := throttle.Pacing()
	ctx := req.Context()

	var waited time.Duration
	attemptReq := req
	for attempt := 1; ; attempt++ {
		held, err := throttle.Acquire(ctx)
		if err != nil {
			return nil, err
		}
		waited += held

		resp, err := t.base.RoundTrip(attemptReq)
		if err != nil {
			// A transport failure is not the workspace refusing anything, so it neither
			// widens the spacing nor counts towards decaying it back.
			return resp, err
		}

		refused, isRefusal := refusalOf(resp)
		if !isRefusal {
			throttle.Accepted()
			return resp, nil
		}
		throttle.Refused(refused.retryAfter)

		outOfAttempts := attempt >= pacing.MaxAttempts
		outOfBudget := waited >= pacing.WaitBudget
		if outOfAttempts || outOfBudget {
			drain(resp)
			// What the workspace actually answered is logged rather than carried in the
			// error, for the reason RateLimitedError's fields document.
			logging.GetLogger(ctx).WithFields(logrus.Fields{
				"url":         req.URL.Path,
				"status":      refused.statusCode,
				"error_code":  refused.errorCode,
				"attempts":    attempt,
				"waited":      waited.Round(time.Millisecond),
				"retry_after": refused.retryAfter,
			}).Warnf("Databricks kept refusing a request for exceeding the workspace quota: %s", refused.message)
			return nil, refused.asError(attempt, waited)
		}

		retryReq, replayable := replayOf(req)
		if !replayable {
			// A body that cannot be sent again cannot be retried — not here, and not by
			// the SDK either, which gives up on the same condition. The response is handed
			// back as it came so the caller sees the API's own error; the spacing this
			// refusal adopted still applies to every request behind it.
			return resp, nil
		}
		drain(resp)
		attemptReq = retryReq
	}
}

// replayOf returns the request to send again, and reports false when its body cannot
// be rewound. A request the SDK builds carries either no body or one it can replay,
// which is every control-plane read a scrape makes.
func replayOf(req *http.Request) (*http.Request, bool) {
	if req.Body == nil || req.Body == http.NoBody {
		return req.Clone(req.Context()), true
	}
	if req.GetBody == nil {
		return nil, false
	}
	body, err := req.GetBody()
	if err != nil {
		return nil, false
	}
	clone := req.Clone(req.Context())
	clone.Body = body
	return clone, true
}

// drain finishes with a refused response so its connection goes back to the pool
// rather than being torn down under every retry.
func drain(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxPeekedErrorBody))
	_ = resp.Body.Close()
}
