// Package httpserver adapts an inbound agilelimiter.ConcurrencyLimiter to
// net/http: it sheds load with 429 when the service is saturated, and feeds
// every admitted request's handling time back to the controller.
//
// Apply it once, around the whole protected subtree. Every route beneath shares
// the same concurrency budget, because they compete for the same process
// resources — do not build a limiter per endpoint.
//
//	lim := agilelimiter.NewConcurrency(agilelimiter.ConcurrencyConfig{
//		MaxLimit:     200,
//		InitialLimit: 20,
//		SLOTarget:    300 * time.Millisecond,
//	})
//	lim.Start()
//	defer lim.Stop()
//
//	mux := http.NewServeMux()
//	mux.HandleFunc("/api/things", getThings)
//	http.ListenAndServe(":8080", httpserver.Wrap(mux, lim))
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

// Option customizes a Wrap middleware. The zero set is a plain-text 429 with no
// Retry-After header.
type Option func(*config)

type config struct {
	retryAfter time.Duration
	reject     http.HandlerFunc
	isHit      func(http.Header) bool
	badStatus  map[int]bool // nil means defaultUnavailableStatus
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

// WithCacheHit replaces the predicate that decides a response was served without
// touching the expensive path. It receives the response headers after the
// handler ran. The default is IsCacheHit.
//
// A hit is not observed, because a request answered from a cache says nothing
// about how much of the service's real capacity is left — the same reason
// httpclient skips hits on the outbound side.
func WithCacheHit(fn func(http.Header) bool) Option {
	return func(c *config) { c.isHit = fn }
}

// WithErrorStatus replaces the set of response statuses that mean "this request
// could not be served right now" rather than "your code has a bug". Those are
// reported to the controller as an outage, so the limit backs off instead of
// climbing on a stream of instant failures. Pass an empty list to disable it.
//
// The default is 429, 502, 503 and 504. A plain 500 is deliberately excluded: it
// usually means a bug, and backing off does not fix a bug — it only turns a
// failing endpoint into a smaller failing endpoint. Raise it to a whole 5xx range
// if your service uses 500 for overload.
func WithErrorStatus(codes ...int) Option {
	return func(c *config) {
		c.badStatus = make(map[int]bool, len(codes))
		for _, s := range codes {
			c.badStatus[s] = true
		}
	}
}

// defaultUnavailableStatus is the statuses that mean "cannot serve right now".
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

// Wrap returns an http.Handler that admits each request through lim. An admitted
// request runs next and its handling time is observed. A request that cannot get
// a slot is shed immediately (default 429) rather than queued — shedding, not
// queueing, is what keeps the limiter from amplifying an overload.
//
// The measured time is next's own duration: admission never blocks, so there is
// no queue wait to contaminate the signal, and rejected requests are never
// observed. The slot is released even if next panics.
//
// The response is inspected before it is observed. A status in the unavailable
// set (429/502/503/504 by default) becomes an outage sample, so a handler that
// fails instantly cannot pass for a fast healthy service and raise the limit; a
// response recognised as a cache hit is not observed at all. Both are
// configurable, and both only look at what the handler already wrote.
func Wrap(next http.Handler, lim *agilelimiter.ConcurrencyLimiter, opts ...Option) http.Handler {
	cfg := config{isHit: IsCacheHit}
	for _, o := range opts {
		o(&cfg)
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

		if cfg.isBadStatus(sw.status()) {
			// The request was admitted but the service could not serve it. Its
			// duration is discarded on purpose: a 503 that took 5ms and one that
			// took 5s both mean "stop admitting", and the backoff is one step per
			// tick either way.
			lim.Unavailable()
			return
		}
		if cfg.isHit != nil && cfg.isHit(w.Header()) {
			return // cheap path: says nothing about remaining capacity
		}
		lim.Observe(d)
	})
}

func (c *config) isBadStatus(code int) bool {
	if c.badStatus != nil {
		return c.badStatus[code]
	}
	return defaultUnavailableStatus(code)
}

// IsCacheHit reports whether the response was served from a cache rather than
// computed on the expensive path. It is the default predicate for Wrap.
//
// It reads the standard signals: an Age header greater than zero (RFC 7234), or
// a HIT in X-Cache / X-Proxy-Cache / X-Cache-Status / CF-Cache-Status. Your
// handler has to set one of them — for a hit served out of process, by your own
// cache, that is a single line before writing the body.
//
// Unknown headers answer "not a hit": observing a hit by mistake only dilutes
// p50, while dropping a real request removes the only evidence of congestion.
func IsCacheHit(h http.Header) bool {
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
