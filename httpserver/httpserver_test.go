package httpserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"agilelimiter"
)

// newLimiter builds a started limiter. A high InitialLimit makes admission the
// default so pass-through tests are not shed.
func newLimiter(t *testing.T, cfg agilelimiter.ConcurrencyConfig) *agilelimiter.ConcurrencyLimiter {
	t.Helper()
	lim := agilelimiter.NewConcurrency(cfg)
	lim.Start()
	t.Cleanup(lim.Stop)
	return lim
}

// sizeOneLimiter returns a limiter with a single slot, already occupied, so any
// request through Wrap is deterministically shed.
func sizeOneLimiterHeld(t *testing.T) *agilelimiter.ConcurrencyLimiter {
	t.Helper()
	lim := newLimiter(t, agilelimiter.ConcurrencyConfig{MinLimit: 1, MaxLimit: 1, InitialLimit: 1})
	if !lim.TryTake() {
		t.Fatal("could not occupy the single slot")
	}
	t.Cleanup(lim.Release)
	return lim
}

func TestWrapPassesAdmittedRequestThrough(t *testing.T) {
	lim := newLimiter(t, agilelimiter.ConcurrencyConfig{MaxLimit: 10, InitialLimit: 10})
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("hi"))
	})

	rec := httptest.NewRecorder()
	Wrap(next, lim).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))

	if rec.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusTeapot)
	}
	if rec.Body.String() != "hi" {
		t.Fatalf("body = %q, want %q", rec.Body.String(), "hi")
	}
	if got := lim.InFlight(); got != 0 {
		t.Fatalf("inflight after request = %d, want 0 (slot leaked)", got)
	}
}

func TestWrapShedsWhenFull(t *testing.T) {
	lim := sizeOneLimiterHeld(t)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler must not run for a shed request")
	})

	rec := httptest.NewRecorder()
	Wrap(next, lim).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
}

func TestWrapDoesNotRunHandlerWhenShed(t *testing.T) {
	// Explicit companion to TestWrapShedsWhenFull: the wrapped handler itself
	// records that it ran, so a false pass cannot hide behind t.Error noise.
	lim := sizeOneLimiterHeld(t)
	ran := false
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { ran = true })

	Wrap(next, lim).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x", nil))

	if ran {
		t.Fatal("handler ran despite the limiter being full")
	}
}

func TestWrapRetryAfterHeaderRoundsUp(t *testing.T) {
	lim := sizeOneLimiterHeld(t)
	h := Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), lim,
		WithRetryAfter(1500*time.Millisecond))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))

	if got := rec.Header().Get("Retry-After"); got != "2" {
		t.Fatalf("Retry-After = %q, want %q", got, "2")
	}
}

func TestWrapCustomReject(t *testing.T) {
	lim := sizeOneLimiterHeld(t)
	h := Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), lim,
		WithRetryAfter(time.Second),
		WithReject(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"busy"}`))
		}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if got := rec.Body.String(); got != `{"error":"busy"}` {
		t.Fatalf("body = %q", got)
	}
	// The option-set Retry-After is still applied before the custom reject.
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want %q", got, "1")
	}
}

func TestWrapReleasesSlotOnPanic(t *testing.T) {
	lim := newLimiter(t, agilelimiter.ConcurrencyConfig{MaxLimit: 5, InitialLimit: 5})
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") })
	h := Wrap(next, lim)

	func() {
		defer func() { _ = recover() }() // swallow the handler's panic
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x", nil))
	}()

	if got := lim.InFlight(); got != 0 {
		t.Fatalf("inflight after panic = %d, want 0 (slot leaked)", got)
	}
}

func TestStatsHandler(t *testing.T) {
	lim := newLimiter(t, agilelimiter.ConcurrencyConfig{MaxLimit: 7, InitialLimit: 7})

	rec := httptest.NewRecorder()
	StatsHandler(lim).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/limit", nil))

	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if got := m["limit"].(float64); got != 7 {
		t.Fatalf("limit = %v, want 7", got)
	}
	for _, k := range []string{"inflight", "baseline", "fast"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("missing key %q in %v", k, m)
		}
	}
}

// TestWrapEndToEnd exercises the wrapper through a real httptest server to make
// sure the 429 and the pass-through behave the same over the wire.
func TestWrapEndToEnd(t *testing.T) {
	lim := sizeOneLimiterHeld(t)
	root := http.NewServeMux()
	root.Handle("/api/", Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}), lim))
	srv := httptest.NewServer(root)
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + "/api/x")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}
}

// newFastLimiter starts a limiter with a 10ms tick and a wide range, so a few
// hundred milliseconds of traffic is enough to see the limit move.
func newFastLimiter(t *testing.T) *agilelimiter.ConcurrencyLimiter {
	t.Helper()
	lim := agilelimiter.NewConcurrency(agilelimiter.ConcurrencyConfig{
		MinLimit: 10, MaxLimit: 200, InitialLimit: 100,
		SLOTarget:        time.Second,
		Tick:             10 * time.Millisecond,
		FastHalfLife:     10 * time.Millisecond,
		SlowHalfLife:     10 * time.Millisecond,
		SlowDownHalfLife: 10 * time.Millisecond,
	})
	lim.Start()
	t.Cleanup(lim.Stop)
	return lim
}

// driveFor serves requests through h for d, discarding the responses.
func driveFor(t *testing.T, h http.Handler, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x", nil))
	}
}

func fastHandler(status int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Millisecond)
		w.WriteHeader(status)
	})
}

// A handler that answers 503 instantly reports a ~0 handling time, which without
// this would read as a fast healthy service and raise the limit.
func TestUnavailableStatusBacksOff(t *testing.T) {
	for _, status := range []int{
		http.StatusTooManyRequests,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			lim := newFastLimiter(t)
			driveFor(t, Wrap(fastHandler(status), lim), 300*time.Millisecond)
			if got := lim.Limit(); got > 50 {
				t.Fatalf("limit %d did not back off toward MinLimit on %d", got, status)
			}
		})
	}
}

// A 500 is a bug, not a capacity signal: backing off would turn a failing
// endpoint into a smaller failing endpoint and fix nothing.
func TestBugStatusIsObservedNormally(t *testing.T) {
	lim := newFastLimiter(t)
	driveFor(t, Wrap(fastHandler(http.StatusInternalServerError), lim), 300*time.Millisecond)
	if got := lim.Limit(); got < 150 {
		t.Fatalf("limit %d fell on a 500; an application bug must not back the limiter off", got)
	}
}

func TestWithErrorStatusOverride(t *testing.T) {
	lim := newFastLimiter(t)
	// An empty set disables status-based backoff entirely.
	driveFor(t, Wrap(fastHandler(http.StatusServiceUnavailable), lim, WithErrorStatus()),
		300*time.Millisecond)
	if got := lim.Limit(); got < 150 {
		t.Fatalf("limit %d fell with status backoff disabled", got)
	}
}

// A cache hit consumes none of the expensive capacity, so it must not feed the
// controller: with every response a hit, the limit stays exactly where it was.
func TestCacheHitIsNotObserved(t *testing.T) {
	lim := newFastLimiter(t)
	hit := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Cache", "HIT")
		time.Sleep(time.Millisecond)
		_, _ = w.Write([]byte("cached"))
	})
	driveFor(t, Wrap(hit, lim), 300*time.Millisecond)

	if got := lim.Limit(); got != 100 {
		t.Fatalf("limit moved to %d on cache hits; they must not be observed", got)
	}

	// The same handler without the header does move it, so the assertion above is
	// about the header and not about a limiter that never ticks.
	lim2 := newFastLimiter(t)
	miss := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Millisecond)
		_, _ = w.Write([]byte("fresh"))
	})
	driveFor(t, Wrap(miss, lim2), 300*time.Millisecond)
	if got := lim2.Limit(); got <= 100 {
		t.Fatalf("limit %d did not climb on observed responses", got)
	}
}

func TestWithCacheHitOverride(t *testing.T) {
	lim := newFastLimiter(t)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Cache", "HIT")
		time.Sleep(time.Millisecond)
	})
	driveFor(t, Wrap(h, lim, WithCacheHit(func(http.Header) bool { return false })),
		300*time.Millisecond)
	if got := lim.Limit(); got <= 100 {
		t.Fatalf("limit %d did not climb with the hit predicate disabled", got)
	}
}

// The wrapper must not break streaming: a handler that type-asserts Flusher has
// to keep working, and the flush has to reach the real writer.
func TestStatusWriterKeepsFlusher(t *testing.T) {
	lim := newFastLimiter(t)
	var flushed bool
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f, ok := w.(http.Flusher)
		if !ok {
			t.Error("wrapped ResponseWriter does not implement http.Flusher")
			return
		}
		f.Flush()
		flushed = true
	})
	rec := httptest.NewRecorder()
	Wrap(h, lim).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))

	if !flushed {
		t.Fatal("handler never saw a Flusher")
	}
	if !rec.Flushed {
		t.Fatal("flush did not reach the underlying writer")
	}
}

// Go 1.20+ handlers use http.NewResponseController, which walks Unwrap.
func TestStatusWriterSupportsResponseController(t *testing.T) {
	lim := newFastLimiter(t)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)
		if err := rc.Flush(); err != nil {
			t.Errorf("ResponseController.Flush: %v", err)
		}
	})
	rec := httptest.NewRecorder()
	Wrap(h, lim).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	if !rec.Flushed {
		t.Fatal("ResponseController did not reach the underlying writer")
	}
}

func TestImplicitStatusIsObserved(t *testing.T) {
	lim := newFastLimiter(t)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Millisecond)
		_, _ = w.Write([]byte("ok")) // no WriteHeader: implicit 200
	})
	driveFor(t, Wrap(h, lim), 300*time.Millisecond)
	if got := lim.Limit(); got <= 100 {
		t.Fatalf("limit %d did not climb; an implicit 200 must be observed", got)
	}
}

// IsCacheHit runs once per response, so it must not allocate. The first version
// used strings.ToLower plus a per-call slice literal and cost 16B every time.
func TestIsCacheHitIsAllocationFree(t *testing.T) {
	h := http.Header{}
	h.Set("X-Cache", "HIT")

	if n := testing.AllocsPerRun(100, func() { IsCacheHit(h) }); n != 0 {
		t.Fatalf("IsCacheHit allocates %.2f times per call, want 0", n)
	}
}
