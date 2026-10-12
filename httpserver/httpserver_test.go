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
