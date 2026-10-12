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
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"agilelimiter"
)

// Option customizes a Wrap middleware. The zero set is a plain-text 429 with no
// Retry-After header.
type Option func(*config)

type config struct {
	retryAfter time.Duration
	reject     http.HandlerFunc
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

// Wrap returns an http.Handler that admits each request through lim. An admitted
// request runs next and its handling time is observed. A request that cannot get
// a slot is shed immediately (default 429) rather than queued — shedding, not
// queueing, is what keeps the limiter from amplifying an overload.
//
// The measured time is next's own duration: admission never blocks, so there is
// no queue wait to contaminate the signal, and rejected requests are never
// observed. The slot is released even if next panics.
func Wrap(next http.Handler, lim *agilelimiter.ConcurrencyLimiter, opts ...Option) http.Handler {
	var cfg config
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
		next.ServeHTTP(w, r)
		lim.Observe(time.Since(start))
	})
}

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
		})
	})
}
