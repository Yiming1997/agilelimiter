package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"agilelimiter"
	"agilelimiter/httpclient"
)

// Temporary diagnostic: does the verdict actually change where the rate goes?
// Same polluted traffic, two policies — observe everything vs DefaultVerdict —
// and we watch the rate after a fixed run.

type fixedTransport struct {
	status int
	header http.Header
	err    error
}

func (f fixedTransport) RoundTrip(*http.Request) (*http.Response, error) {
	time.Sleep(time.Millisecond) // every response is "fast"
	if f.err != nil {
		return nil, f.err
	}
	return &http.Response{StatusCode: f.status, Header: f.header, Body: http.NoBody}, nil
}

var errRefused = errors.New("connection refused")

func runVerdictArm(rt http.RoundTripper, observeAll bool, run time.Duration) (rate float64, samples uint64) {
	lim := agilelimiter.New(agilelimiter.Config{
		MinRate:          20,
		MaxRate:          500,
		InitialRate:      100,
		SLOTarget:        300 * time.Millisecond,
		FastHalfLife:     250 * time.Millisecond,
		SlowHalfLife:     2 * time.Second,
		SlowDownHalfLife: 500 * time.Millisecond,
		UpStep:           0.10, // 1.1^10 per second, so the trend is visible in 2s
		Tick:             100 * time.Millisecond,
	})
	verdict := httpclient.DefaultVerdict
	if observeAll {
		verdict = func(*http.Response, error, time.Duration) agilelimiter.Outcome {
			return agilelimiter.Reached
		}
	}
	client := httpclient.Wrap(rt, lim, httpclient.WithVerdict(verdict))
	lim.Start()
	defer lim.Stop()

	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	deadline := time.Now().Add(run)
	for time.Now().Before(deadline) {
		resp, _ := client.RoundTrip(req)
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}
	st := lim.Stats()
	return st.Rate, st.Samples
}

func TestVerdictEffect(t *testing.T) {
	if testing.Short() {
		t.Skip("2s per arm; skipped in -short")
	}

	const run = 2 * time.Second
	cases := []struct {
		name string
		rt   http.RoundTripper
	}{
		{"503 shedding", fixedTransport{status: 503, header: http.Header{}}},
		{"500 bug", fixedTransport{status: 500, header: http.Header{}}},
		{"cache hit Age=60", fixedTransport{status: 200, header: http.Header{"Age": {"60"}}}},
		{"caller canceled", fixedTransport{err: context.Canceled}},
		{"connection refused", fixedTransport{err: &net.OpError{Op: "dial", Net: "tcp", Err: errRefused}}},
		{"202 accepted (async)", fixedTransport{status: 202, header: http.Header{}}},
		{"404 not found", fixedTransport{status: 404, header: http.Header{}}},
	}
	fmt.Printf("VERDICT run=%s initial=100 min=20 max=500\n", run)
	for _, c := range cases {
		naive, _ := runVerdictArm(c.rt, true, run)
		def, samples := runVerdictArm(c.rt, false, run)
		fmt.Printf("VERDICT|%-20s|observe-all=%6.0f|DefaultVerdict=%6.0f|last-tick-samples=%d\n",
			c.name, naive, def, samples)
	}
}
