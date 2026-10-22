// Package httpserver adapts an inbound agilelimiter.ConcurrencyLimiter to
// net/http: it sheds load with 429 when the service is saturated, and feeds each
// admitted request's handling time back to the controller.
//
// # One hook, no built-in opinion about what a response means
//
// A handler that fails instantly reports a ~0 handling time, and so does one
// served from memory, one rejected by an auth check, and a CORS preflight. Fed
// unfiltered into one p50 they make a struggling service look fast. Only the
// caller knows what a response means, so this package asks once:
//
//	lim := agilelimiter.NewConcurrency(agilelimiter.ConcurrencyConfig{...})
//	srv := httpserver.Wrap(mux, lim,
//		httpserver.WithVerdict(myVerdict))
//
//	func myVerdict(r *http.Request, status int, h http.Header, d time.Duration) agilelimiter.Outcome {
//		switch {
//		case r.Context().Err() != nil:
//			return agilelimiter.Aborted
//		case strings.HasPrefix(r.URL.Path, "/health"), r.URL.Path == "/metrics":
//			return agilelimiter.Aborted
//		case status == http.StatusTooManyRequests, status >= 500:
//			return agilelimiter.Unreachable
//		case httpserver.IsCacheHitHeader(h):
//			return agilelimiter.Aborted
//		default:
//			return agilelimiter.Reached
//		}
//	}
//
// DefaultVerdict is the behaviour you get without one: small, readable, and meant
// to be copied and edited rather than configured through options.
package httpserver

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"agilelimiter"
)

// Verdict decides what one admitted request tells the controller. It is the only
// policy hook this middleware has, and it is called once per request, after the
// handler ran, so it must not block or allocate.
//
//	status is what the handler wrote (200 if it never called WriteHeader) and h
//	are the response headers it set; d is the handler's own duration.
//
// The three answers are the three things the controller can be told:
//
//	Reached      record d as one observation (the default for real work)
//	Unreachable  treat it as an outage: back off toward MinLimit
//	Aborted      record nothing: this request says nothing about capacity
type Verdict func(r *http.Request, status int, h http.Header, d time.Duration) agilelimiter.Outcome

// Option customizes a Wrap middleware. The zero set is a plain-text 429 with no
// Retry-After header.
type Option func(*config)

type config struct {
	retryAfter time.Duration
	reject     http.HandlerFunc
	verdict    Verdict
}

// WithRetryAfter sets the Retry-After header (whole seconds, rounded up) on a
// rejected request, telling the client when to try again.
func WithRetryAfter(d time.Duration) Option {
	return func(c *config) { c.retryAfter = d }
}

// WithReject replaces the default overload response. Use it to match your API's
// error format (e.g. a JSON body). The Retry-After header, if configured, is
// still set before your handler runs.
func WithReject(h http.HandlerFunc) Option {
	return func(c *config) { c.reject = h }
}

// WithVerdict replaces the verdict. Pass nil to restore DefaultVerdict.
func WithVerdict(v Verdict) Option {
	return func(c *config) { c.verdict = v }
}

// DefaultVerdict is what Wrap does when no verdict is supplied. Copy it and edit
// it rather than looking for an option that tweaks one branch.
//
//	request context canceled   Aborted      the client went away; the handler
//	                                        returned early because nobody is
//	                                        listening, not because it was fast
//	429, 502, 503, 504         Unreachable  "cannot serve right now"
//	cache hit (Age, X-Cache)   Aborted      no expensive path was touched
//	anything else              Reached      including 500, which is usually a
//	                                        bug: backing off would turn a failing
//	                                        endpoint into a smaller one
//
// Two things it deliberately does not do, because they need knowledge this
// function does not have:
//
//   - Health checks, metrics scrapes and CORS preflights are cheap and, on a
//     low-traffic service, a large share of the samples. Exclude them by path.
//   - A long-lived stream (SSE, websocket upgrade, long poll) drags the median
//     up for every other request sharing the limiter. Give it its own budget.
func DefaultVerdict(r *http.Request, status int, h http.Header, d time.Duration) agilelimiter.Outcome {
	switch {
	case r != nil && r.Context().Err() != nil:
		return agilelimiter.Aborted
	case defaultUnavailableStatus(status):
		return agilelimiter.Unreachable
	case IsCacheHitHeader(h):
		return agilelimiter.Aborted
	default:
		return agilelimiter.Reached
	}
}

// defaultUnavailableStatus is the statuses that mean "cannot serve right now"
// rather than "your code has a bug".
func defaultUnavailableStatus(code int) bool {
	switch code {
	case http.StatusTooManyRequests, // 429
		http.StatusBadGateway,         // 502
		http.StatusServiceUnavailable, // 503
		http.StatusGatewayTimeout:     // 504
		return true
	}
	return false
}

// cacheHeaderNames is package-level so a range over it does not allocate on the
// hot path (once per request).
var cacheHeaderNames = [...]string{"X-Cache", "X-Proxy-Cache", "X-Cache-Status", "CF-Cache-Status"}

// IsCacheHitHeader reports whether the response was served from a cache rather
// than computed on the expensive path. DefaultVerdict uses it; a custom verdict
// can too.
//
// It reads the standard signals: an Age header greater than zero (RFC 7234), or a
// HIT in X-Cache / X-Proxy-Cache / X-Cache-Status / CF-Cache-Status. Your handler
// has to set one — for a hit served by your own cache that is a single line
// before writing the body.
//
// Unknown headers answer "not a hit": observing a hit by mistake only dilutes
// p50, while dropping a real request removes the only evidence of congestion.
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

// Wrap returns an http.Handler that admits each request through lim. An admitted
// request runs next and its handling time is reported to the controller through
// the verdict. A request that cannot get a slot is shed immediately (default 429)
// rather than queued — shedding, not queueing, is what keeps the limiter from
// amplifying an overload.
//
// The measured time is next's own duration: admission never blocks, so there is
// no queue wait to contaminate the signal, and rejected requests are never
// measured. The slot is released even if next panics.
func Wrap(next http.Handler, lim *agilelimiter.ConcurrencyLimiter, opts ...Option) http.Handler {
	var cfg config
	for _, o := range opts {
		o(&cfg)
	}
	verdict := cfg.verdict
	if verdict == nil {
		verdict = DefaultVerdict
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !lim.TryTake() {
			reject(w, r, cfg)
			return
		}
		defer lim.Release() // released even if next panics

		start := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(sw, r)
		d := time.Since(start)

		// A canceled request is a truncated measurement: the handler returned
		// because nobody is listening, not because it was fast. That is a fact
		// about the measurement rather than a policy choice, so it is enforced
		// here — replacing the verdict cannot reintroduce the bug.
		if r.Context().Err() != nil {
			return
		}

		switch verdict(r, sw.status(), w.Header(), d) {
		case agilelimiter.Unreachable:
			lim.Unavailable()
		case agilelimiter.Aborted:
			// Nothing: this request is not evidence about capacity.
		default:
			lim.Observe(d)
		}
	})
}

// statusWriter records the status the handler wrote without changing anything
// else about the response. It forwards the optional interfaces so that streaming
// (http.Flusher), WebSocket upgrades (http.Hijacker), HTTP/2 push and sendfile
// (io.ReaderFrom) keep working through the middleware.
type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// status is the status the client will see; WriteHeader may never be called.
func (w *statusWriter) status() int {
	if w.code == 0 {
		return http.StatusOK
	}
	return w.code
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		if w.code == 0 {
			w.code = http.StatusOK
		}
		f.Flush()
	}
}

func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := w.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}

func (w *statusWriter) Push(target string, opts *http.PushOptions) error {
	if p, ok := w.ResponseWriter.(http.Pusher); ok {
		return p.Push(target, opts)
	}
	return http.ErrNotSupported
}

func (w *statusWriter) ReadFrom(r io.Reader) (int64, error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	if rf, ok := w.ResponseWriter.(io.ReaderFrom); ok {
		return rf.ReadFrom(r)
	}
	return io.Copy(w.ResponseWriter, r)
}

// Unwrap lets http.ResponseController (Go 1.20+) reach the underlying writer.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func reject(w http.ResponseWriter, r *http.Request, cfg config) {
	if cfg.retryAfter > 0 {
		secs := int((cfg.retryAfter + time.Second - 1) / time.Second) // round up
		w.Header().Set("Retry-After", strconv.Itoa(secs))
	}
	if cfg.reject != nil {
		cfg.reject(w, r)
		return
	}
	http.Error(w, "server busy, retry shortly", http.StatusTooManyRequests)
}

// StatsHandler reports the limiter's current concurrency limit and control
// signals as JSON. Mount it outside Wrap so it stays readable while the service
// is saturated.
func StatsHandler(lim *agilelimiter.ConcurrencyLimiter) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := lim.Stats()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"limit":    lim.Limit(),
			"inflight": lim.InFlight(),
			"baseline": s.Baseline.String(),
			"fast":     s.Fast.String(),
			"samples":  s.Samples,
			"p50":      s.P50.String(),
			"p99":      s.P99.String(),
		})
	})
}
