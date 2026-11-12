package main

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"agilelimiter"
)

// loadOptions is one driving run: Workers callers hammer the limiter for
// Duration after a Settle warm-up that is not scored. Settle exists because an
// adaptive controller needs a few ticks to find the operating point; scoring
// it would measure the ramp instead of the algorithm.
type loadOptions struct {
	Workers     int
	Settle      time.Duration
	Duration    time.Duration
	RefuseSleep time.Duration
}

// loadResult is the measured snapshot: completion counters plus the latency
// quantiles of the measured window.
type loadResult struct {
	Admitted    int64
	Good        int64
	Hits        int64
	Misses      int64
	Refused     int64
	Observed    int64
	Unavailable int64
	Mean        time.Duration
	P50         time.Duration
	P99         time.Duration
	GoodPct     float64
}

// loadStats is the plugin-owned, hot-path state. Counters are atomics because
// every admitted request increments them from many goroutines; the latency
// slice is guarded by a mutex because appending needs a slice header and the
// run is short enough that the contention is irrelevant next to the simulated
// RTT. Per the Store layering rule only the handle is shared, never the
// counters themselves.
type loadStats struct {
	admitted    atomic.Int64
	good        atomic.Int64
	hits        atomic.Int64
	misses      atomic.Int64
	refused     atomic.Int64
	observed    atomic.Int64
	unavailable atomic.Int64

	mu   sync.Mutex
	lats []time.Duration
}

func (s *loadStats) record(r response) {
	s.admitted.Add(1)
	if r.Hit {
		s.hits.Add(1)
	} else {
		s.misses.Add(1)
	}
	if r.Good {
		s.good.Add(1)
	}
	switch r.Outcome {
	case agilelimiter.Unreachable:
		s.unavailable.Add(1)
	case agilelimiter.Aborted:
		// no observation: a cheap path says nothing about capacity
	default:
		s.observed.Add(1)
	}
	s.mu.Lock()
	s.lats = append(s.lats, r.Dur)
	s.mu.Unlock()
}

func (s *loadStats) snapshot() loadResult {
	res := loadResult{
		Admitted:    s.admitted.Load(),
		Good:        s.good.Load(),
		Hits:        s.hits.Load(),
		Misses:      s.misses.Load(),
		Refused:     s.refused.Load(),
		Observed:    s.observed.Load(),
		Unavailable: s.unavailable.Load(),
	}
	if res.Admitted > 0 {
		res.GoodPct = 100 * float64(res.Good) / float64(res.Admitted)
	}
	s.mu.Lock()
	lats := make([]time.Duration, len(s.lats))
	copy(lats, s.lats)
	s.mu.Unlock()
	if len(lats) == 0 {
		return res
	}
	var sum time.Duration
	for _, d := range lats {
		sum += d
	}
	res.Mean = sum / time.Duration(len(lats))
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	res.P50 = lats[int(float64(len(lats)-1)*0.50)]
	res.P99 = lats[int(float64(len(lats)-1)*0.99)]
	return res
}

// rateEstimator turns admitted requests into a requests/second estimate for
// the concurrency limiter, where Rate() is a limit (an in-flight count), not a
// rate. The rate limiter does not need it: its target is already req/s, and
// using the target (like the existing compare harness) avoids the sampling
// lag that a measured estimate would add.
type rateEstimator struct {
	count atomic.Int64
	last  atomic.Int64 // rate * 1000
}

func newRateEstimator() *rateEstimator {
	e := &rateEstimator{}
	e.last.Store(1000) // start at 1 req/s rather than 0
	return e
}

func (e *rateEstimator) add() { e.count.Add(1) }

func (e *rateEstimator) rate() float64 { return float64(e.last.Load()) / 1000 }

func (e *rateEstimator) sampleLoop(interval time.Duration, done <-chan struct{}) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			n := e.count.Swap(0)
			e.last.Store(int64(float64(n) / interval.Seconds() * 1000))
		case <-done:
			return
		}
	}
}

// feed tells the controller what one request said about the downstream. It is
// the same three-way policy the httpclient/httpserver adapters apply, spelled
// out here so the harness exercises Observe/Unavailable directly.
func feed(h limiterHandle, r response) {
	switch r.Outcome {
	case agilelimiter.Unreachable:
		h.Unavailable()
	case agilelimiter.Aborted:
		// nothing: a cache hit or a rejected call is not evidence
	default:
		h.Observe(r.Dur)
	}
}

// driveLoad is the shared engine behind --load and the self-contained h*
// scenarios. It never records a request admitted during Settle; those still
// feed the controller, so the operating point is real by the time scoring
// starts.
func driveLoad(ctx context.Context, h limiterHandle, resp responder, opt loadOptions, stats *loadStats) {
	if opt.Workers < 1 {
		opt.Workers = 1
	}
	if opt.RefuseSleep <= 0 {
		opt.RefuseSleep = 2 * time.Millisecond
	}

	meter := newRateEstimator()
	meterDone := make(chan struct{})
	go meter.sampleLoop(250*time.Millisecond, meterDone)
	defer close(meterDone)

	var seq atomic.Uint64
	start := time.Now()
	measureEnd := opt.Settle + opt.Duration
	deadline := start.Add(measureEnd)

	var wg sync.WaitGroup
	for i := 0; i < opt.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil || !time.Now().Before(deadline) {
					return
				}
				if !h.Admit(ctx) {
					if elapsed := time.Since(start); elapsed >= opt.Settle && elapsed < measureEnd {
						stats.refused.Add(1)
					}
					time.Sleep(opt.RefuseSleep)
					continue
				}
				admittedAt := time.Since(start)
				// The window is [Settle, Settle+Duration). A request granted
				// after it is a draining straggler: at a low rate the bucket
				// can hold hundreds of callers past the deadline, and counting
				// those grants would divide a long drain by a short window and
				// report a rate the limiter never actually sustained.
				if admittedAt >= measureEnd {
					h.Release()
					return
				}
				meter.add()
				offered := h.Rate()
				if h.Kind() != "rate" {
					offered = meter.rate()
				}
				r := resp.Respond(offered, admittedAt, seq.Add(1))
				time.Sleep(r.Dur)
				h.Release()
				if ctx.Err() != nil {
					return
				}
				feed(h, r)
				if admittedAt >= opt.Settle {
					stats.record(r)
				}
			}
		}()
	}
	// Run for the full settle+duration before considering a drain. Waiting on
	// the workers themselves is not enough: at a low rate most of them are
	// parked inside Take for the whole run, so a naive wg.Wait (or a grace
	// measured from start) would cut the measurement short. The timer is the
	// measurement clock; workers see the same deadline at the top of their
	// loop and stop admitting.
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
	waitBounded(&wg, 3*time.Second)
}

// waitBounded waits for the workers to drain, but not forever. At a low rate
// the bucket serialises blocked callers (one Take holds the bucket mutex while
// it sleeps), so N callers can take N/rate seconds to drain — a one-second
// probe rate with 256 workers would take over four minutes. Scoring has
// already stopped at that point; the stragglers wake, see the deadline and
// exit, and the process ends shortly after. A bounded grace keeps the CLI
// responsive without shortening the measured window.
func waitBounded(wg *sync.WaitGroup, grace time.Duration) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(grace):
	}
}

// loadPlugin drives the limiter and prints the measured outcome. It has no End
// phase: the run is synchronous and the limiter's lifecycle belongs to
// --limiter.
type loadPlugin struct{}

func (loadPlugin) Name() string { return "load" }
func (loadPlugin) Desc() string {
	return "drive concurrent load through the limiter and report goodput/latency"
}

func (loadPlugin) Deps() []string { return []string{"limiter", "downstream"} }

func (loadPlugin) Options() []Option {
	return []Option{
		{Name: "workers", Default: "256", Help: "concurrent callers"},
		{Name: "settle", Default: "3s", Help: "warm-up not scored (lets the controller converge)"},
		{Name: "dur", Default: "6s", Help: "measured duration"},
		{Name: "refuse-sleep", Default: "2ms", Help: "back-off after a non-blocking refusal"},
	}
}

func (loadPlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	opts := ParseOptions(args)

	workers, err := GetInt(opts, "workers", 256)
	if err != nil {
		return err
	}
	settle, err := GetDuration(opts, "settle", 3*time.Second)
	if err != nil {
		return err
	}
	dur, err := GetDuration(opts, "dur", 6*time.Second)
	if err != nil {
		return err
	}
	refuseSleep, err := GetDuration(opts, "refuse-sleep", 2*time.Millisecond)
	if err != nil {
		return err
	}
	if workers < 1 {
		return fmt.Errorf("option %q: must be >= 1", "workers")
	}
	if settle < 0 || dur <= 0 {
		return fmt.Errorf("settle must be >= 0 and dur > 0")
	}

	h, err := Require[limiterHandle](rt.Store, "limiter", "handle")
	if err != nil {
		return err
	}
	resp, err := Require[responder](rt.Store, "downstream", "responder")
	if err != nil {
		return err
	}

	stats := &loadStats{}
	Provide(rt.Store, "load", "stats", stats)

	driveLoad(ctx, h, resp, loadOptions{
		Workers: workers, Settle: settle, Duration: dur, RefuseSleep: refuseSleep,
	}, stats)

	res := stats.snapshot()
	Provide(rt.Store, "load", "result", res)

	secs := dur.Seconds()
	fmt.Fprintf(rt.Out,
		"[load] admitted/s=%.1f good/s=%.1f good%%=%.1f mean=%s p50=%s p99=%s "+
			"hits=%d misses=%d refused/s=%.1f observed=%d unavailable=%d final_rate=%.1f\n",
		float64(res.Admitted)/secs, float64(res.Good)/secs, res.GoodPct,
		res.Mean.Round(time.Millisecond), res.P50.Round(time.Millisecond), res.P99.Round(time.Millisecond),
		res.Hits, res.Misses, float64(res.Refused)/secs, res.Observed, res.Unavailable, h.Rate())
	return nil
}
