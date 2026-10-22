package httpclient

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"agilelimiter"
)

func TestIsCacheHit(t *testing.T) {
	cases := []struct {
		name   string
		header http.Header
		want   bool
	}{
		{"no headers", http.Header{}, false},
		{"Age 0 is still fresh", http.Header{"Age": {"0"}}, false},
		{"Age 60 served from cache", http.Header{"Age": {"60"}}, true},
		{"Age garbage", http.Header{"Age": {"soon"}}, false},
		{"X-Cache HIT", http.Header{"X-Cache": {"HIT"}}, true},
		{"X-Cache Hit from cloudfront", http.Header{"X-Cache": {"Hit from cloudfront"}}, true},
		{"X-Cache MISS", http.Header{"X-Cache": {"MISS"}}, false},
		{"CF-Cache-Status HIT", http.Header{"Cf-Cache-Status": {"HIT"}}, true},
		{"CF-Cache-Status MISS", http.Header{"Cf-Cache-Status": {"MISS"}}, false},
		{"X-Proxy-Cache HIT", http.Header{"X-Proxy-Cache": {"HIT"}}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsCacheHit(&http.Response{Header: c.header}); got != c.want {
				t.Fatalf("IsCacheHit(%v) = %v, want %v", c.header, got, c.want)
			}
		})
	}
	if IsCacheHit(nil) {
		t.Fatal("nil response must not be a hit")
	}
}

// countingTransport answers every request with the given headers after a short
// delay, so the measured round trip is a real positive duration.
type countingTransport struct {
	headers http.Header
	delay   time.Duration
	calls   int
}

func (c *countingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	c.calls++
	time.Sleep(c.delay)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     c.headers.Clone(),
		Body:       io.NopCloser(strings.NewReader("ok")),
	}, nil
}

// newLimiter starts a limiter with a fast tick so a control step happens well
// inside the test.
func newLimiter(t *testing.T, cfg agilelimiter.Config) *agilelimiter.Limiter {
	t.Helper()
	cfg.Tick = 10 * time.Millisecond
	cfg.FastHalfLife = 10 * time.Millisecond
	cfg.SlowHalfLife = 10 * time.Millisecond
	lim := agilelimiter.New(cfg)
	lim.Start()
	t.Cleanup(lim.Stop)
	return lim
}

// waitForBaseline waits until the controller has a baseline (i.e. it took at
// least one sample and ran a step), or the deadline passes.
func waitForBaseline(lim *agilelimiter.Limiter, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if lim.Stats().Baseline > 0 {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func TestWrapPassesResponseThrough(t *testing.T) {
	lim := newLimiter(t, agilelimiter.Config{MinRate: 10, MaxRate: 1000, InitialRate: 1000})
	inner := &countingTransport{headers: http.Header{"X-Test": {"1"}}}
	rt := Wrap(inner, lim)

	resp, err := rt.RoundTrip(httptest.NewRequest(http.MethodGet, "http://example.com/", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("X-Test") != "1" {
		t.Fatalf("response not passed through: %d %v", resp.StatusCode, resp.Header)
	}
	if inner.calls != 1 {
		t.Fatalf("underlying transport calls = %d, want 1", inner.calls)
	}
}

func TestCacheHitIsNotObserved(t *testing.T) {
	lim := newLimiter(t, agilelimiter.Config{MinRate: 10, MaxRate: 1000, InitialRate: 1000})
	rt := Wrap(&countingTransport{
		headers: http.Header{"Age": {"120"}},
		delay:   2 * time.Millisecond,
	}, lim)

	for i := 0; i < 5; i++ {
		resp, err := rt.RoundTrip(httptest.NewRequest(http.MethodGet, "http://example.com/", nil))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}

	if waitForBaseline(lim, 300*time.Millisecond) {
		t.Fatalf("a cache hit must not feed the controller, but baseline = %v", lim.Stats().Baseline)
	}
}

func TestCacheMissIsObserved(t *testing.T) {
	lim := newLimiter(t, agilelimiter.Config{MinRate: 10, MaxRate: 1000, InitialRate: 1000})
	rt := Wrap(&countingTransport{
		headers: http.Header{"X-Cache": {"MISS"}},
		delay:   2 * time.Millisecond,
	}, lim)

	for i := 0; i < 5; i++ {
		resp, err := rt.RoundTrip(httptest.NewRequest(http.MethodGet, "http://example.com/", nil))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}

	if !waitForBaseline(lim, time.Second) {
		t.Fatal("a cache miss must feed the controller, but no baseline was ever set")
	}
}

// One function is the whole policy surface: a verdict that observes everything
// overrides the default's cache-hit skip.
func TestVerdictOverrideObservesEverything(t *testing.T) {
	lim := newLimiter(t, agilelimiter.Config{MinRate: 10, MaxRate: 1000, InitialRate: 1000})
	rt := Wrap(&countingTransport{
		headers: http.Header{"Age": {"120"}}, // DefaultVerdict would skip this
		delay:   2 * time.Millisecond,
	}, lim, WithVerdict(func(*http.Response, error, time.Duration) agilelimiter.Outcome {
		return agilelimiter.Reached
	}))

	for i := 0; i < 5; i++ {
		resp, err := rt.RoundTrip(httptest.NewRequest(http.MethodGet, "http://example.com/", nil))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	if !waitForBaseline(lim, time.Second) {
		t.Fatal("a verdict returning Reached must make every response observable")
	}
}

var errUnreachable = errors.New("connection refused")

// The verdict sees exactly what the transport returned, which is what makes
// "this never reached the downstream" expressible at all.
func TestVerdictSeesTheTransportError(t *testing.T) {
	var seen error
	lim := newLimiter(t, agilelimiter.Config{
		MinRate: 10, MaxRate: 1000, InitialRate: 100,
		SLOTarget: 200 * time.Millisecond,
	})
	rt := Wrap(failingTransport{}, lim, WithVerdict(
		func(_ *http.Response, err error, _ time.Duration) agilelimiter.Outcome {
			seen = err
			if errors.Is(err, errUnreachable) {
				return agilelimiter.Unreachable
			}
			return agilelimiter.Reached
		}))

	if _, err := rt.RoundTrip(httptest.NewRequest(http.MethodGet, "http://example.com/", nil)); err == nil {
		t.Fatal("expected the transport error to be returned")
	}
	if !errors.Is(seen, errUnreachable) {
		t.Fatalf("the verdict saw %v, want %v", seen, errUnreachable)
	}
}

type failingTransport struct{}

func (failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errUnreachable
}

// statusTransport answers every request with a fixed status.
type statusTransport struct {
	status int
	delay  time.Duration
}

func (s statusTransport) RoundTrip(*http.Request) (*http.Response, error) {
	time.Sleep(s.delay)
	return &http.Response{
		StatusCode: s.status,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader("")),
	}, nil
}

// driveFor keeps sending requests through rt for d. The limiters under test are
// driven by their own ticker, so the measurement has to run long enough for a
// few control steps rather than a fixed call count.
func driveFor(t *testing.T, rt http.RoundTripper, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		resp, err := rt.RoundTrip(httptest.NewRequest(http.MethodGet, "http://example.com/", nil))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
}

// A 429 or a 5xx is not a Go error, so without the verdict it looks like a fast,
// healthy round trip and the controller accelerates into the service that just
// refused it. A 404 is a real round trip and must not be treated that way.
func TestStatusClassification(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		verdict Verdict
		backoff bool
	}{
		{"503 backs off", http.StatusServiceUnavailable, nil, true},
		{"500 backs off", http.StatusInternalServerError, nil, true},
		{"429 backs off", http.StatusTooManyRequests, nil, true},
		{"404 is a healthy round trip", http.StatusNotFound, nil, false},
		{"a custom verdict narrows it to 503", http.StatusInternalServerError, statusBackoff(503), false},
		{"a custom verdict turns it off", http.StatusServiceUnavailable,
			func(*http.Response, error, time.Duration) agilelimiter.Outcome {
				return agilelimiter.Reached
			}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			lim := newLimiter(t, agilelimiter.Config{
				MinRate: 10, MaxRate: 200, InitialRate: 100,
				SLOTarget: time.Second,
				Tick:      10 * time.Millisecond,
			})
			var opts []Option
			if c.verdict != nil {
				opts = append(opts, WithVerdict(c.verdict))
			}
			rt := Wrap(statusTransport{status: c.status, delay: time.Millisecond}, lim, opts...)
			driveFor(t, rt, 300*time.Millisecond)

			got := lim.Rate()
			if c.backoff && got > 50 {
				t.Fatalf("rate %v did not back off toward MinRate on %d", got, c.status)
			}
			if !c.backoff && got < 150 {
				t.Fatalf("rate %v fell on %d, which must be observed normally", got, c.status)
			}
		})
	}
}

// statusBackoff is an example of a small custom policy: back off on exactly these
// statuses, hand everything else to the default.
func statusBackoff(codes ...int) Verdict {
	return func(resp *http.Response, err error, d time.Duration) agilelimiter.Outcome {
		if err != nil {
			return DefaultVerdict(resp, err, d)
		}
		for _, c := range codes {
			if resp != nil && resp.StatusCode == c {
				return agilelimiter.Unreachable
			}
		}
		return agilelimiter.Reached
	}
}

// The default is a pure function, so its rules are testable directly instead of
// only through a running limiter.
func TestDefaultVerdictRules(t *testing.T) {
	cases := []struct {
		name string
		resp *http.Response
		err  error
		want agilelimiter.Outcome
	}{
		{"200 observed", &http.Response{StatusCode: 200, Header: http.Header{}}, nil, agilelimiter.Reached},
		{"404 observed", &http.Response{StatusCode: 404, Header: http.Header{}}, nil, agilelimiter.Reached},
		{"429 backs off", &http.Response{StatusCode: 429, Header: http.Header{}}, nil, agilelimiter.Unreachable},
		{"503 backs off", &http.Response{StatusCode: 503, Header: http.Header{}}, nil, agilelimiter.Unreachable},
		{"500 backs off", &http.Response{StatusCode: 500, Header: http.Header{}}, nil, agilelimiter.Unreachable},
		{"cache hit skipped", &http.Response{StatusCode: 200, Header: http.Header{"X-Cache": {"HIT"}}}, nil, agilelimiter.Aborted},
		{"canceled skipped", nil, context.Canceled, agilelimiter.Aborted},
		{"timeout observed", nil, context.DeadlineExceeded, agilelimiter.Reached},
		{"nil response skipped", nil, nil, agilelimiter.Aborted},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := DefaultVerdict(c.resp, c.err, 10*time.Millisecond); got != c.want {
				t.Fatalf("DefaultVerdict = %v, want %v", got, c.want)
			}
		})
	}
}

// IsCacheHit runs once per response, so it must not allocate.
func TestIsCacheHitIsAllocationFree(t *testing.T) {
	h := http.Header{}
	h.Set("X-Cache", "HIT")
	resp := &http.Response{Header: h}
	if n := testing.AllocsPerRun(100, func() { IsCacheHit(resp) }); n != 0 {
		t.Fatalf("IsCacheHit allocates %.2f times per call, want 0", n)
	}
}

// Replacing the verdict must not be able to reintroduce "a canceled call reads as
// a fast healthy response": the wrapper filters that before the verdict runs,
// because it is a broken measurement rather than a policy choice.
func TestCanceledIsNotObservedEvenWithAPermissiveVerdict(t *testing.T) {
	lim := newLimiter(t, agilelimiter.Config{MinRate: 10, MaxRate: 1000, InitialRate: 1000})
	rt := Wrap(canceledTransport{}, lim, WithVerdict(
		func(*http.Response, error, time.Duration) agilelimiter.Outcome {
			return agilelimiter.Reached // asks to observe everything
		}))

	for i := 0; i < 5; i++ {
		if _, err := rt.RoundTrip(httptest.NewRequest(http.MethodGet, "http://example.com/", nil)); !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	}
	if waitForBaseline(lim, 300*time.Millisecond) {
		t.Fatalf("a canceled call reached the controller (baseline %v); the wrapper must filter it", lim.Stats().Baseline)
	}
}

type canceledTransport struct{}

func (canceledTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, context.Canceled
}

// A failure while opening the connection says nothing about capacity: nothing was
// sent, and it returns in microseconds. This is a phase check on the error, not a
// taxonomy, so refused / DNS / no route / connect timeout all land here.
func TestDefaultVerdictClassifiesDialErrors(t *testing.T) {
	dial := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	read := &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset")}

	if got := DefaultVerdict(nil, dial, time.Microsecond); got != agilelimiter.Unreachable {
		t.Fatalf("dial error = %v, want Unreachable", got)
	}
	if got := DefaultVerdict(nil, read, 5*time.Second); got != agilelimiter.Reached {
		t.Fatalf("read error = %v, want Reached: the call reached the downstream", got)
	}
	if got := DefaultVerdict(nil, errUnreachable, time.Microsecond); got != agilelimiter.Reached {
		t.Fatalf("plain error = %v, want Reached: only a *net.OpError proves the dial phase", got)
	}
}
