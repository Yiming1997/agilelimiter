package httpclient

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"agilelimiter"
)

// benchResp is reused so the transport allocates nothing: the benchmark should
// measure the wrapper, not the fixture.
var benchResp = &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: http.NoBody}

// benchSink keeps the compiler from removing the raw baseline entirely.
var benchSink int

type nopTransport struct{}

func (nopTransport) RoundTrip(*http.Request) (*http.Response, error) { return benchResp, nil }

// wrapBenchLimiter never sleeps: Per = 1ns with a rate of 1e9 truncates the
// pacing interval to zero, so Take() is just a mutex and a clock read. Without
// this the benchmark measures the OS timer, not the wrapper.
func wrapBenchLimiter() *agilelimiter.Limiter {
	return agilelimiter.New(agilelimiter.Config{
		MinRate: 1, MaxRate: 1_000_000_000, InitialRate: 1_000_000_000,
		Per: time.Nanosecond,
	})
}

func BenchmarkWrap(b *testing.B) {
	raw := nopTransport{}
	wrapped := Wrap(raw, wrapBenchLimiter())
	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)

	b.Run("raw", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			resp, _ := raw.RoundTrip(req)
			benchSink += resp.StatusCode
		}
	})
	b.Run("wrapped", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			resp, _ := wrapped.RoundTrip(req)
			benchSink += resp.StatusCode
		}
	})
}

// One request must produce exactly one sample. Double counting is easy to
// introduce by accident (observe in the error branch and again at the end), and
// invisible in production — it would just quietly make the limiter twice as
// reactive as it should be.
func TestObservedOncePerRequest(t *testing.T) {
	const n = 10
	lim := agilelimiter.New(agilelimiter.Config{
		MinRate: 1, MaxRate: 1_000_000, InitialRate: 100_000,
		Tick:      200 * time.Millisecond,
		SLOTarget: time.Second,
	})
	lim.Start()
	defer lim.Stop()

	rt := Wrap(nopTransport{}, lim)
	for i := 0; i < n; i++ {
		if _, err := rt.RoundTrip(httptest.NewRequest(http.MethodGet, "http://example.com/", nil)); err != nil {
			t.Fatal(err)
		}
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
