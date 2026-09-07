package databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/databricks/databricks-sdk-go/apierr"
	"github.com/pkg/errors"
)

// Pacing is how a client reacts to a Databricks workspace refusing a request for
// exceeding its control-plane quota. The interval it converges on is the spacing
// between requests of the whole process, not of one caller, so it is what actually
// bounds the load put on one workspace.
type Pacing struct {
	// FirstInterval is the spacing adopted the first time a request is refused.
	// Until then a workspace is unpaced and runs at whatever the callers and the
	// SDK's own client-side limiter allow, so a workspace that is never refused pays
	// nothing for any of this.
	FirstInterval time.Duration
	// MaxInterval caps the spacing. Past it, waiting longer no longer buys a caller
	// anything its own deadline will not decide first.
	MaxInterval time.Duration
	// MaxPause caps how long one refusal may hold requests back, and with it the
	// Retry-After the API asks for. A wait longer than this outlives the per-attempt
	// timeout the SDK puts on a request, so honouring it verbatim would turn a
	// refusal into a request that times out mid-flight instead of one that waits.
	MaxPause time.Duration
	// DecayAfter is how many consecutive accepted requests it takes to halve the
	// spacing again. The quota is shared with everything else running against the
	// workspace, so a client has to keep probing back towards full speed rather than
	// stay slow for the rest of the process's life.
	DecayAfter int
	// MaxAttempts bounds how many times one request may be refused before the error
	// is handed to the caller.
	MaxAttempts int
	// WaitBudget bounds how long one request may spend held back across all of its
	// attempts. It exists for the same reason as MaxPause: the waiting happens inside
	// the SDK's per-attempt timeout, so it cannot be unbounded.
	WaitBudget time.Duration
}

// withDefaults fills in whatever a caller left unset, so that a half-built Pacing
// cannot come to mean something like "give up after no attempts at all".
func (p Pacing) withDefaults() Pacing {
	if p.FirstInterval <= 0 {
		p.FirstInterval = DefaultPacing.FirstInterval
	}
	if p.MaxInterval <= 0 {
		p.MaxInterval = DefaultPacing.MaxInterval
	}
	if p.MaxPause <= 0 {
		p.MaxPause = DefaultPacing.MaxPause
	}
	if p.DecayAfter <= 0 {
		p.DecayAfter = DefaultPacing.DecayAfter
	}
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = DefaultPacing.MaxAttempts
	}
	if p.WaitBudget <= 0 {
		p.WaitBudget = DefaultPacing.WaitBudget
	}
	return p
}

// DefaultPacing is what every client built here uses unless the workspace's throttle
// was given something else through UsePacing.
var DefaultPacing = Pacing{
	FirstInterval: 100 * time.Millisecond,
	MaxInterval:   5 * time.Second,
	MaxPause:      30 * time.Second,
	DecayAfter:    50,
	MaxAttempts:   8,
	WaitBudget:    45 * time.Second,
}

// Throttle paces every control-plane request one process sends to one workspace.
// Every request passes through it, which is the point: a refusal seen by one caller
// holds back all of them, where each caller retrying on its own kept the workspace's
// quota exhausted for as long as any of them had work left.
type Throttle struct {
	pacing Pacing

	mu sync.Mutex
	// nextAt is the earliest instant at which a request may be sent. A refusal pushes
	// it into the future for every caller at once.
	nextAt time.Time
	// interval is the spacing currently handed out between consecutive requests.
	interval time.Duration
	// accepted counts requests accepted since the last refusal, against DecayAfter.
	accepted int

	refusals int
	// waited sums what every request spent held back, so it exceeds the wall clock of
	// any one of them.
	waited time.Duration
}

// ThrottleStats reports what a workspace's throttle has done, for the caller's own
// log line and span. The counters run for the life of the process, because so does the
// throttle — a caller reporting on one run of its own takes a snapshot before it starts
// and calls Since on the one it takes after.
type ThrottleStats struct {
	// Refusals is how many requests the workspace refused for exceeding its quota.
	Refusals int
	// Waited is the total time requests spent held back. It sums over concurrent
	// requests, so it can exceed the wall clock of the run.
	Waited time.Duration
	// Interval is the spacing currently handed out. Zero means unpaced.
	Interval time.Duration
}

// Paced reports whether the workspace refused anything, which is the only reason these
// numbers are worth a log line.
func (s ThrottleStats) Paced() bool {
	return s.Refusals > 0
}

// Since reports what happened between an earlier snapshot and this one. Interval is
// current rather than differenced: it is a rate, not a count.
func (s ThrottleStats) Since(earlier ThrottleStats) ThrottleStats {
	return ThrottleStats{
		Refusals: s.Refusals - earlier.Refusals,
		Waited:   s.Waited - earlier.Waited,
		Interval: s.Interval,
	}
}

func NewThrottle(pacing Pacing) *Throttle {
	return &Throttle{pacing: pacing.withDefaults()}
}

// Pacing returns the pacing behaviour this throttle was built with.
func (t *Throttle) Pacing() Pacing {
	return t.pacing
}

// Acquire blocks until this request's turn to be sent, or until ctx is done. It
// reports how long the caller was held back.
func (t *Throttle) Acquire(ctx context.Context) (time.Duration, error) {
	wait := t.reserve()
	if wait <= 0 {
		return 0, nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return 0, errors.WithStack(ctx.Err())
	case <-timer.C:
		return wait, nil
	}
}

// reserve claims the next send slot and reports how long the caller has to wait
// before using it.
func (t *Throttle) reserve() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	at := t.nextAt
	if at.Before(now) {
		at = now
	}
	t.nextAt = at.Add(t.interval)

	wait := at.Sub(now)
	t.waited += wait
	return wait
}

// Refused records that the workspace rejected a request for exceeding its quota.
// retryAfter is what the API asked for, zero when it asked for nothing.
func (t *Throttle) Refused(retryAfter time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.refusals++
	t.accepted = 0
	t.interval = t.widen(t.interval)

	// Hold every caller off for the longer of what the API asked for and the spacing
	// just adopted, so requests resume below the pace that was refused rather than at
	// the burst that provoked it.
	pause := t.interval
	if retryAfter > pause {
		pause = retryAfter
	}
	if pause > t.pacing.MaxPause {
		pause = t.pacing.MaxPause
	}
	if resumeAt := time.Now().Add(pause); resumeAt.After(t.nextAt) {
		t.nextAt = resumeAt
	}
}

// Accepted records that a request went through, and speeds the process back up once
// enough of them have.
func (t *Throttle) Accepted() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.interval <= 0 {
		return
	}
	t.accepted++
	if t.accepted < t.pacing.DecayAfter {
		return
	}
	t.accepted = 0
	t.interval /= 2
	if t.interval < t.pacing.FirstInterval {
		t.interval = 0
	}
}

func (t *Throttle) widen(interval time.Duration) time.Duration {
	if interval <= 0 {
		return t.pacing.FirstInterval
	}
	if interval >= t.pacing.MaxInterval {
		return t.pacing.MaxInterval
	}
	if doubled := interval * 2; doubled < t.pacing.MaxInterval {
		return doubled
	}
	return t.pacing.MaxInterval
}

// Stats reports what the throttle did, for the caller's log line and span.
func (t *Throttle) Stats() ThrottleStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	return ThrottleStats{Refusals: t.refusals, Waited: t.waited, Interval: t.interval}
}

// throttles holds one throttle per workspace per process, so every client built for
// the same workspace paces against the others rather than each discovering the
// quota separately.
var throttles = struct {
	mu     sync.Mutex
	byHost map[string]*Throttle
}{byHost: map[string]*Throttle{}}

// ThrottleFor returns the throttle every client of that workspace shares, creating
// it with DefaultPacing on first use. Callers read ThrottleStats off it for their
// own log line.
func ThrottleFor(workspaceUrl string) *Throttle {
	return throttleForHost(workspaceHost(workspaceUrl), DefaultPacing)
}

// UsePacing installs a workspace's throttle with pacing of the caller's choosing. It
// has to run before the first client for that workspace is built — once a throttle
// exists it keeps the pacing it was created with, so that a client cannot have the
// spacing pulled out from under a request already in flight. Meant for tests, which
// need the convergence to happen in milliseconds, and for a caller that knows a
// workspace's quota is tighter than most.
func UsePacing(workspaceUrl string, pacing Pacing) *Throttle {
	return throttleForHost(workspaceHost(workspaceUrl), pacing)
}

func throttleForHost(host string, pacing Pacing) *Throttle {
	throttles.mu.Lock()
	defer throttles.mu.Unlock()

	throttle, ok := throttles.byHost[host]
	if !ok {
		throttle = NewThrottle(pacing)
		throttles.byHost[host] = throttle
	}
	return throttle
}

// workspaceHost is the key a workspace's throttle lives under: its host, so that a
// caller holding the configured workspace URL and a request holding only its own URL
// resolve to the same throttle.
func workspaceHost(workspaceUrl string) string {
	trimmed := strings.TrimSpace(workspaceUrl)
	if trimmed == "" {
		return ""
	}
	if !strings.Contains(trimmed, "//") {
		trimmed = "https://" + trimmed
	}
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.Host == "" {
		return strings.ToLower(strings.Trim(trimmed, "/"))
	}
	return strings.ToLower(parsed.Host)
}

// RateLimitedError is what a request refused for exceeding a workspace's quota fails
// with once the pacing has run out of attempts to spend on it.
//
// It is returned in place of the API's own error because it is the only way to keep
// the SDK from re-sending the request: the SDK retries a rate-limited response
// blindly for five minutes, ignoring Retry-After, which hides the refusal from the
// pacing and parks the caller on a request that is being refused. Everything else
// the SDK treats as transient is still the SDK's to retry.
type RateLimitedError struct {
	// StatusCode, ErrorCode and Message are what the workspace answered with. They are
	// deliberately not part of Error(): the SDK re-sends any error whose *text*
	// contains REQUEST_LIMIT_EXCEEDED — a workaround for SCIM answering 500 on a rate
	// limit — and it runs that check on whatever error the transport hands back. An
	// error quoting the API's own code or message would therefore go straight back
	// under the blind five-minute retry this type exists to prevent. Callers that want
	// them in a log line read the fields.
	StatusCode int
	ErrorCode  string
	Message    string
	// RetryAfter is the wait the API last asked for, zero when it asked for nothing.
	RetryAfter time.Duration
	// Attempts is how many times the request was sent and refused.
	Attempts int
	// Waited is how long this request spent held back before giving up.
	Waited time.Duration
}

func (e *RateLimitedError) Error() string {
	return "rate limited by the workspace: " + strconv.Itoa(e.Attempts) + " attempts over " +
		e.Waited.Round(time.Millisecond).String() + " were all refused with HTTP " + strconv.Itoa(e.StatusCode)
}

// IsRateLimitError reports whether err is a workspace refusing a request for
// exceeding its quota — whether it comes from the pacing here or straight from the
// SDK, which is what a caller holding an error off any Databricks client sees.
func IsRateLimitError(err error) bool {
	_, isRateLimit := RateLimitRetryAfter(err)
	return isRateLimit
}

// RateLimitRetryAfter reports whether err is a workspace refusing a request for
// exceeding its quota, and how long it asked us to wait before asking again.
func RateLimitRetryAfter(err error) (time.Duration, bool) {
	var rateLimited *RateLimitedError
	if errors.As(err, &rateLimited) {
		return rateLimited.RetryAfter, true
	}

	var apiErr *apierr.APIError
	if !errors.As(err, &apiErr) {
		return 0, false
	}
	// IsTooManyRequests covers a 429 and the REQUEST_LIMIT_EXCEEDED /
	// RESOURCE_EXHAUSTED codes at any status. The message is matched as well because
	// the control plane does not always answer a status or a code that maps onto one,
	// and a refusal read as an ordinary failure fails the whole scrape.
	isRateLimit := apiErr.IsTooManyRequests() ||
		apiErr.StatusCode == http.StatusTooManyRequests ||
		rateLimitMessage.MatchString(apiErr.Message)
	if !isRateLimit {
		return 0, false
	}
	return retryAfterOf(apiErr), true
}

// rateLimitMessage matches the body a workspace answers a refused request with.
var rateLimitMessage = regexp.MustCompile(`(?i)rate limit exceeded|too many requests|request limit exceeded`)

// rateLimitErrorCodes are the error codes the SDK maps onto its rate-limit error.
// The pacing sees the response rather than the mapped error, so it matches them
// itself.
var rateLimitErrorCodes = map[string]struct{}{
	"REQUEST_LIMIT_EXCEEDED": {},
	"RESOURCE_EXHAUSTED":     {},
}

func retryAfterOf(apiErr *apierr.APIError) time.Duration {
	if apiErr.ResponseWrapper == nil || apiErr.ResponseWrapper.Response == nil {
		return 0
	}
	return retryAfterHeader(apiErr.ResponseWrapper.Response.Header)
}

// retryAfterHeader reads the wait the API asked for. Retry-After is specified as
// either a number of seconds or an HTTP date, and Databricks sends both shapes
// depending on which layer refused the request.
func retryAfterHeader(header http.Header) time.Duration {
	value := header.Get("Retry-After")
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	at, err := http.ParseTime(value)
	if err != nil {
		return 0
	}
	if wait := time.Until(at); wait > 0 {
		return wait
	}
	return 0
}

// refusal is a response read as the workspace refusing a request for exceeding its
// quota.
type refusal struct {
	statusCode int
	errorCode  string
	message    string
	retryAfter time.Duration
}

func (r refusal) asError(attempts int, waited time.Duration) *RateLimitedError {
	return &RateLimitedError{
		StatusCode: r.statusCode,
		ErrorCode:  r.errorCode,
		Message:    r.message,
		RetryAfter: r.retryAfter,
		Attempts:   attempts,
		Waited:     waited,
	}
}

// maxPeekedErrorBody caps how much of a failed response is read to decide whether it
// is a refusal. Error bodies are a few hundred bytes; the cap is there so that a
// response which is not one cannot be buffered into memory wholesale.
const maxPeekedErrorBody = 64 << 10

// refusalOf reports whether a response is the workspace refusing the request for
// exceeding its quota. The body is read to decide, and put back for the SDK to map
// into its own error afterwards.
func refusalOf(resp *http.Response) (refusal, bool) {
	if resp == nil {
		return refusal{}, false
	}
	if resp.StatusCode < 400 {
		return refusal{}, false
	}

	found := refusal{statusCode: resp.StatusCode, retryAfter: retryAfterHeader(resp.Header)}
	if resp.StatusCode == http.StatusTooManyRequests {
		found.errorCode, found.message = peekApiError(resp)
		return found, true
	}

	// A refusal is not always a 429: the control plane also answers one as a 400 or a
	// 500 carrying the code or the message, which is what apierr.IsTooManyRequests
	// picks up downstream of here.
	found.errorCode, found.message = peekApiError(resp)
	if _, isRateLimit := rateLimitErrorCodes[found.errorCode]; isRateLimit {
		return found, true
	}
	if rateLimitMessage.MatchString(found.message) {
		return found, true
	}
	return refusal{}, false
}

// peekApiError reads the error code and message off a failed response without
// consuming it: whatever is read is put back, so the SDK still maps the response
// into the error the caller sees.
func peekApiError(resp *http.Response) (errorCode string, message string) {
	if resp.Body == nil {
		return "", ""
	}
	peeked, err := io.ReadAll(io.LimitReader(resp.Body, maxPeekedErrorBody))
	if len(peeked) > 0 {
		resp.Body = &rewoundBody{Reader: io.MultiReader(bytes.NewReader(peeked), resp.Body), closer: resp.Body}
	}
	if err != nil {
		return "", ""
	}

	var body struct {
		ErrorCode string `json:"error_code"`
		Message   string `json:"message"`
		Error     string `json:"error"`
	}
	if err := json.Unmarshal(peeked, &body); err != nil {
		// Not every layer in front of a workspace answers JSON — a gateway refusing a
		// request answers plain text — so the raw body is what gets matched then.
		return "", string(peeked)
	}
	if body.Message == "" {
		body.Message = body.Error
	}
	return body.ErrorCode, body.Message
}

// rewoundBody serves a response body that has already been read once, and closes the
// original underneath.
type rewoundBody struct {
	io.Reader
	closer io.Closer
}

func (b *rewoundBody) Close() error {
	return b.closer.Close()
}
