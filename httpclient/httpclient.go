// Package httpclient adapts an outbound agilelimiter.Limiter to net/http as a
// http.RoundTripper, and decides — once, in one function — what each response
// tells the controller.
//
// # The problem this package exists to keep out of the core
//
// The controller regulates a rate by watching a duration. That only works while
// the duration measures the thing that is scarce. Plenty of responses do not:
//
//	cache hit           ~1ms, no origin capacity used
//	redirect / 304      cheap, the expensive hop is elsewhere
//	202 Accepted        the work has not happened yet
//	fast 5xx / 4xx      answered without doing the work
//	circuit fallback    never reached the downstream at all
//
// Fed unfiltered into one p50, these make a saturated dependency look fast. The
// library cannot fix that from the inside: only the caller knows what a response
// means. So this package asks once, with one hook, and otherwise stays out of
// the way.
//
//	client := &http.Client{
//		Transport: httpclient.Wrap(http.DefaultTransport, lim,
//			httpclient.WithVerdict(myVerdict)),
//	}
//
// A Verdict — returning agilelimiter.Reached, Unreachable or Aborted — is the
// whole policy surface. DefaultVerdict is the behaviour you get without one: it
// is deliberately small and readable, and meant to be copied and edited rather
// than configured through options.
package httpclient

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"agilelimiter"
)

// Verdict decides what one call tells the controller. It is the only policy hook
// this package has, and it is called once per call, on the caller's goroutine, so
// it must not block or allocate.
//
//	resp and err are exactly what the wrapped RoundTripper returned and may be
//	nil in the usual ways; d is the wall time spent inside it.
//
// The three answers are the three things the controller can be told:
//
//	Reached      record d as one observation (the default for an answered call)
//	Unreachable  treat it as an outage: back off toward MinRate
//	Aborted      record nothing: this call says nothing about capacity
//
// What separates them is not "success or failure" but "did this call use the
// resource we are protecting". A timeout is a genuine signal and is Reached; a
// connection that never opened, a cache hit or an async accept is not.
type Verdict func(resp *http.Response, err error, d time.Duration) agilelimiter.Outcome

// Option customizes the wrapper.
type Option func(*config)

type config struct {
	verdict Verdict
}

// WithVerdict replaces the verdict. Pass nil to restore DefaultVerdict.
func WithVerdict(v Verdict) Option {
	return func(c *config) { c.verdict = v }
}

// Wrap returns a RoundTripper that takes a slot from lim before each request,
// measures the round trip, and applies the verdict to the result.
//
// A nil rt means http.DefaultTransport. A nil verdict means DefaultVerdict.
func Wrap(rt http.RoundTripper, lim *agilelimiter.Limiter, opts ...Option) http.RoundTripper {
	var cfg config
	for _, o := range opts {
		o(&cfg)
	}
	if rt == nil {
		rt = http.DefaultTransport
	}
	if cfg.verdict == nil {
		cfg.verdict = DefaultVerdict
	}
	return &transport{rt: rt, lim: lim, verdict: cfg.verdict}
}

type transport struct {
	rt      http.RoundTripper
	lim     *agilelimiter.Limiter
	verdict Verdict
}

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.lim.Take()
	start := time.Now()
	resp, err := t.rt.RoundTrip(req)
	d := time.Since(start)

	// A canceled round trip is a truncated measurement, not a fast response: the
	// call stopped because the caller gave up, not because the downstream was
	// quick. That is a fact about the measurement rather than a policy choice, so
	// it is enforced here — replacing the verdict cannot reintroduce the bug.
	if errors.Is(err, context.Canceled) {
		return resp, err
	}

	switch t.verdict(resp, err, d) {
	case agilelimiter.Unreachable:
		t.lim.Unavailable()
	case agilelimiter.Aborted:
		// Nothing: this call is not evidence about capacity.
	default:
		t.lim.Observe(d)
	}
	return resp, err
}

// DefaultVerdict is what Wrap does when no verdict is supplied. It is small on
// purpose: copy it and edit it rather than looking for an option that tweaks one
// branch.
//
//	429, any 5xx              Unreachable  "do not send more"
//	dial error (*net.OpError) Unreachable  refused / DNS / no route: nothing was
//	                                       sent, so the microsecond round trip is
//	                                       not a fast response
//	cache hit (Age, X-Cache)  Aborted      no origin capacity was used
//	anything else             Reached
//
// The canceled case is handled by Wrap before the verdict runs, so a replacement
// verdict cannot lose it; it appears here only so that calling DefaultVerdict
// directly behaves the same way.
//
// Two things it deliberately does not do, because they need knowledge this
// function does not have:
//
//   - An error your own library invents — an open circuit breaker, a rejected
//     payload — is not a *net.OpError, so it is observed. Add a case if those
//     return fast:
//
//     case errors.Is(err, ErrBreakerOpen):
//     return agilelimiter.Unreachable
//
//   - A 202, or a response served from a local fallback, is cheap and looks
//     healthy. Add a case if your API uses them:
//
//     case resp.StatusCode == http.StatusAccepted:
//     return agilelimiter.Aborted
func DefaultVerdict(resp *http.Response, err error, d time.Duration) agilelimiter.Outcome {
	switch {
	case err != nil && errors.Is(err, context.Canceled):
		return agilelimiter.Aborted
	case err != nil && isDialError(err):
		// The failure happened while opening the connection: nothing was sent, so
		// the microsecond round trip says nothing about capacity. This is a phase
		// check, not an error taxonomy — it covers refused, DNS failure, no route
		// and connect timeouts without naming any of them.
		return agilelimiter.Unreachable
	case err != nil:
		return agilelimiter.Reached
	case resp == nil:
		return agilelimiter.Aborted
	case defaultUnavailableStatus(resp.StatusCode):
		return agilelimiter.Unreachable
	case IsCacheHit(resp):
		return agilelimiter.Aborted
	default:
		return agilelimiter.Reached
	}
}

// isDialError reports whether err came from establishing the connection.
func isDialError(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}

// defaultUnavailableStatus is "do not send more". A client cannot tell an
// upstream bug from upstream overload, so every 5xx counts: sending less is cheap
// for us and kind to a dependency that is already failing. (The server-side
// package draws the line differently, because there the status is your own.)
func defaultUnavailableStatus(code int) bool {
	return code == http.StatusTooManyRequests || (code >= 500 && code <= 599)
}

// IsCacheHit reports whether resp was served from a cache rather than computed by
// the origin. DefaultVerdict uses it; a custom verdict can too.
//
// Two standard signals are enough in practice:
//
//   - an Age header greater than zero (RFC 7234: the response has been sitting
//     in a cache for that many seconds);
//   - a cache-status header saying HIT: X-Cache (Varnish, Fastly, CloudFront),
//     CF-Cache-Status (Cloudflare), X-Proxy-Cache, X-Cache-Status.
//
// A nil response is not a hit, and neither is an unknown header: observing a hit
// by mistake only dilutes p50, while dropping a real miss removes the only
// evidence of congestion.
func IsCacheHit(resp *http.Response) bool {
	if resp == nil {
		return false
	}
	return IsCacheHitHeader(resp.Header)
}

// cacheHeaderNames is package-level so a range over it does not allocate on the
// hot path (once per response).
var cacheHeaderNames = [...]string{"X-Cache", "X-Proxy-Cache", "X-Cache-Status", "CF-Cache-Status"}

// IsCacheHitHeader is IsCacheHit for a bare header set.
func IsCacheHitHeader(h http.Header) bool {
	if h == nil {
		return false
	}
	if age := strings.TrimSpace(h.Get("Age")); age != "" {
		if n, err := strconv.Atoi(age); err == nil && n > 0 {
			return true
		}
	}
	for _, name := range cacheHeaderNames {
		if containsHit(h.Get(name)) {
			return true
		}
	}
	return false
}

// containsHit is a case-insensitive strings.Contains that does not allocate the
// way strings.ToLower would.
func containsHit(s string) bool {
	const sub = "hit"
	for i := 0; i+len(sub) <= len(s); i++ {
		if strings.EqualFold(s[i:i+len(sub)], sub) {
			return true
		}
	}
	return false
}
