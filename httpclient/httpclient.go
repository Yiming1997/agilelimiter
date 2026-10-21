// Package httpclient adapts an outbound agilelimiter.Limiter to net/http as a
// http.RoundTripper, and keeps cache hits out of the control signal.
//
// A cache in front of a downstream splits its traffic into two populations that
// share one pipe: a hit returns in about a millisecond and consumes none of the
// origin's capacity, a miss costs whatever the origin costs. Feeding both into
// one p50 makes the controller track the cache's hit ratio instead of the
// origin's load. Measured on a 95%-hit workload (compare/cache_test.go): the
// overall SLO compliance read 98.9% while the requests that actually reached the
// origin were only 79% within SLO, at p99 369ms. The aggregate metric was green
// and the origin was saturated.
//
//	client := &http.Client{
//		Transport: httpclient.Wrap(http.DefaultTransport, lim),
//	}
//
// A hit is detected from the response (see IsCacheHit) and is not observed, so
// what the controller regulates stays the traffic that costs capacity.
//
// Two limits of this design, stated plainly:
//
//   - A hit still takes a slot, because the caller cannot know a request will
//     hit before sending it. Filtering therefore only stops hits from inflating
//     the health signal; it does not give hits a higher ceiling. To do that, send
//     hits through a second limiter at the call site, where a local cache already
//     tells you the answer for free.
//   - A cache that goes cold is a step change, and an adaptive limiter is a lagging
//     controller: it will overshoot while it discovers the new capacity. The
//     defenses against that are outside this package — coalesce misses
//     (single-flight so one fill serves many), size MaxRate for a cold origin,
//     and keep a small cache-bypassing probe on the controller's signal.
package httpclient

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"agilelimiter"
)

// Option customizes the wrapper.
type Option func(*config)

type config struct {
	isHit     func(*http.Response) bool
	badStatus map[int]bool // nil means defaultBadStatus
}

// WithCacheHit replaces the cache-hit predicate. Pass a function returning false
// to observe every response, including hits.
func WithCacheHit(fn func(*http.Response) bool) Option {
	return func(c *config) { c.isHit = fn }
}

// WithErrorStatus overrides which HTTP statuses mean "the downstream refused to
// serve this". The default is httpserver's 429 plus every 5xx.
//
// Such a response is not an error at the transport level — err is nil — but it
// must never be observed as a healthy fast round trip, which is what happens by
// default: a service that sheds with an immediate 503 answers in about a
// millisecond, the controller reads that as "the downstream is fast", and it
// raises the rate into the thing that just refused it. Those statuses are
// reported as an outage instead, so the rate backs off toward MinRate.
//
// Pass an empty list to disable the behaviour and observe every response.
func WithErrorStatus(codes ...int) Option {
	return func(c *config) {
		c.badStatus = make(map[int]bool, len(codes))
		for _, s := range codes {
			c.badStatus[s] = true
		}
	}
}

// defaultBadStatus is a 429 or any 5xx.
func defaultBadStatus(code int) bool {
	return code == http.StatusTooManyRequests || (code >= 500 && code <= 599)
}

// Wrap returns a RoundTripper that takes a slot from lim before each request,
// measures the round trip, and reports the outcome. Errors go through
// lim's Config.Classify, so "never reached the downstream" is handled there; a
// response recognised as a cache hit is not observed at all.
//
// A nil rt means http.DefaultTransport.
func Wrap(rt http.RoundTripper, lim *agilelimiter.Limiter, opts ...Option) http.RoundTripper {
	cfg := config{isHit: IsCacheHit}
	for _, o := range opts {
		o(&cfg)
	}
	if rt == nil {
		rt = http.DefaultTransport
	}
	return &transport{rt: rt, lim: lim, cfg: cfg}
}

type transport struct {
	rt  http.RoundTripper
	lim *agilelimiter.Limiter
	cfg config
}

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.lim.Take()
	start := time.Now()
	resp, err := t.rt.RoundTrip(req)
	d := time.Since(start)

	if err != nil {
		// Config.Classify decides what a failure means: a timeout is a real round
		// trip and is observed, a refused connection becomes a synthetic
		// congested sample, a caller-side abort is ignored.
		t.lim.ObserveOutcome(d, err)
		return resp, err
	}
	if t.isBadStatus(resp.StatusCode) {
		// The downstream answered, but with "I could not serve this". That is not
		// an error at the transport level, so it would otherwise be recorded as a
		// fast healthy round trip — the same trap as a refused connection.
		//
		// The attempt's own duration is deliberately discarded: a 503 that took
		// two seconds and a 503 that took two milliseconds both mean "send less",
		// and the backoff is capped at one step per tick either way.
		t.lim.Unavailable()
		return resp, nil
	}
	if t.cfg.isHit != nil && t.cfg.isHit(resp) {
		// Served from a cache: no origin capacity was used, so this says nothing
		// about whether the origin can take more.
		return resp, nil
	}
	t.lim.ObserveOutcome(d, nil)
	return resp, nil
}

func (t *transport) isBadStatus(code int) bool {
	if t.cfg.badStatus != nil {
		return t.cfg.badStatus[code]
	}
	return defaultBadStatus(code)
}

// IsCacheHit reports whether resp was served from a cache rather than computed
// by the origin. It is the default predicate for Wrap.
//
// Two standard signals are enough in practice:
//
//   - an Age header greater than zero (RFC 7234: the response has been sitting
//     in a cache for that many seconds);
//   - a cache-status header saying HIT: X-Cache (Varnish, Fastly, CloudFront),
//     CF-Cache-Status (Cloudflare), X-Proxy-Cache, X-Cache-Status.
//
// A nil response is not a hit. Unknown headers are not a hit: it is better to
// observe a hit by mistake (the sample dilutes p50, which a higher quantile can
// still rescue) than to drop a real miss (the only evidence of congestion is
// gone). When in doubt, this function answers "not a hit".
func IsCacheHit(resp *http.Response) bool {
	if resp == nil {
		return false
	}
	if age := strings.TrimSpace(resp.Header.Get("Age")); age != "" {
		if n, err := strconv.Atoi(age); err == nil && n > 0 {
			return true
		}
	}
	for _, name := range cacheHeaderNames {
		if containsHit(resp.Header.Get(name)) {
			return true
		}
	}
	return false
}

// cacheHeaderNames is package-level so a range over it does not allocate on the
// hot path (once per response).
var cacheHeaderNames = [...]string{"X-Cache", "X-Proxy-Cache", "X-Cache-Status", "CF-Cache-Status"}

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
