package httpserver

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"agilelimiter"
)

// nopWriter is a ResponseWriter that allocates nothing and stores nothing, so a
// benchmark measures the middleware and not the recorder.
type nopWriter struct{ h http.Header }

func (w *nopWriter) Header() http.Header {
	if w.h == nil {
		w.h = http.Header{}
	}
	return w.h
}

func (w *nopWriter) Write(b []byte) (int, error) { return len(b), nil }
func (w *nopWriter) WriteHeader(int)             {}

var benchBody = []byte("ok")

// benchSink keeps the raw baseline from being optimised away.
var benchSink int

func benchLimiter() *agilelimiter.ConcurrencyLimiter {
	return agilelimiter.NewConcurrency(agilelimiter.ConcurrencyConfig{
		MinLimit: 1, MaxLimit: 1 << 30, InitialLimit: 1 << 30,
	})
}

func BenchmarkWrap(b *testing.B) {
	raw := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, _ := w.Write(benchBody)
		benchSink += n
	})
	wrapped := Wrap(raw, benchLimiter())
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	w := &nopWriter{}

	b.Run("raw", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			raw.ServeHTTP(w, req)
		}
	})
	b.Run("wrapped", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			wrapped.ServeHTTP(w, req)
		}
	})
}

// One request must produce exactly one sample: the status branch, the cache
// branch and the normal branch have to be mutually exclusive.
func TestObservedOncePerRequest(t *testing.T) {
	const n = 10
	lim := agilelimiter.NewConcurrency(agilelimiter.ConcurrencyConfig{
		MinLimit: 1, MaxLimit: 1_000, InitialLimit: 1_000,
		SLOTarget: time.Second,
		Tick:      200 * time.Millisecond,
	})
	lim.Start()
	defer lim.Stop()

	h := Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(benchBody)
	}), lim)
	for i := 0; i < n; i++ {
		h.ServeHTTP(&nopWriter{}, httptest.NewRequest(http.MethodGet, "/x", nil))
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s := lim.Stats().Samples; s != 0 {
			if s != n {
				t.Fatalf("one request produced %d samples for %d requests", s, n)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no tick ever saw the samples")
}

// A shed request never reaches the handler, so it must never produce a sample
// either; otherwise the limiter would be feeding on the requests it just refused.
func TestShedRequestsProduceNoSamples(t *testing.T) {
	lim := agilelimiter.NewConcurrency(agilelimiter.ConcurrencyConfig{
		MinLimit: 1, MaxLimit: 1, InitialLimit: 1,
		Tick: 200 * time.Millisecond,
	})
	lim.Start()
	defer lim.Stop()

	if !lim.TryTake() { // occupy the only slot
		t.Fatal("could not occupy the single slot")
	}
	defer lim.Release()

	h := Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler must not run for a shed request")
	}), lim)

	for i := 0; i < 10; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
		if rec.Code != http.StatusTooManyRequests {
			t.Fatalf("status = %d, want 429", rec.Code)
		}
	}

	// Two ticks pass with nothing admitted: the controller must see no samples.
	time.Sleep(350 * time.Millisecond)
	if s := lim.Stats().Samples; s != 0 {
		t.Fatalf("shed requests produced %d samples", s)
	}
}
