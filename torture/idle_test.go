package torture

import (
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Yiming1997/agilelimiter"
)

// ---------------------------------------------------------------------------
// the silence window, shared by both limiters
// ---------------------------------------------------------------------------

// idleSubject abstracts limit/rate readings and in-flight counts for both limiters.
type idleSubject struct {
	quota func() float64
	busy  func() int
	stats func() agilelimiter.Stats
}

// silenceSample is one reading of a limiter while nothing is offered to it.
type silenceSample struct {
	at       time.Duration
	quota    float64 // advertised limit or rate
	target   float64 // internal controller target rate
	baseline time.Duration
	samples  uint64
	busy     int
}

// sampleSilence reads limiter metrics at regular intervals during silence without sending traffic.
func sampleSilence(s idleSubject, dur, every time.Duration) []silenceSample {
	start := time.Now()
	t := time.NewTicker(every)
	defer t.Stop()
	var out []silenceSample
	for time.Since(start) < dur {
		<-t.C
		st := s.stats()
		out = append(out, silenceSample{
			at:       time.Since(start),
			quota:    s.quota(),
			target:   st.Rate,
			baseline: st.Baseline,
			samples:  st.Samples,
			busy:     s.busy(),
		})
	}
	return out
}

// waitIdle waits until all in-flight requests drain and the controller sees at
// least two quiet ticks with zero samples, ensuring no trailing adjustments.
func waitIdle(s idleSubject, tick, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	var drained time.Time
	for time.Now().Before(deadline) {
		if s.busy() > 0 {
			drained = time.Time{}
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if drained.IsZero() {
			drained = time.Now()
		}
		if time.Since(drained) >= 2*tick && s.stats().Samples == 0 {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// holdThroughSilence asserts that quota, internal rate, and baseline remain
// frozen throughout the silence window with zero traffic and no floor collapse.
func holdThroughSilence(t *testing.T, idle []silenceSample, pre agilelimiter.Stats, preQuota, least float64, floor int) {
	t.Helper()
	if len(idle) == 0 {
		t.Fatal("no readings in the silence window; nothing was verified")
	}
	first := idle[0]

	// 1. Lossless transition into silence.
	if first.quota != preQuota {
		t.Errorf("quota changed before the first silence sample: %.3f -> %.3f at %v",
			preQuota, first.quota, first.at)
	}
	if first.target != pre.Rate {
		t.Errorf("internal rate changed before the first silence sample: %.6f -> %.6f at %v",
			pre.Rate, first.target, first.at)
	}

	// 2. Advertised quota must remain strictly unchanged.
	for _, s := range idle {
		if s.quota != first.quota {
			t.Errorf("quota moved during silence: %.3f at %v, then %.3f at %v",
				first.quota, first.at, s.quota, s.at)
			break
		}
	}

	// 3. Controller internal target rate must not drift.
	for _, s := range idle {
		if s.target != first.target {
			t.Errorf("internal rate drifted during silence: %.6f -> %.6f at %v",
				first.target, s.target, s.at)
			break
		}
	}

	// 4. Window must be completely idle.
	for _, s := range idle {
		if s.samples != 0 || s.busy != 0 {
			t.Errorf("silence window was not silent at %v: samples=%d busy=%d",
				s.at, s.samples, s.busy)
			break
		}
	}

	// 5. Baseline must survive the silence unchanged.
	for _, s := range idle {
		if s.baseline != pre.Baseline {
			t.Errorf("baseline changed during silence: %v -> %v at %v",
				pre.Baseline, s.baseline, s.at)
			break
		}
	}

	// 6. Quota must not collapse toward the floor.
	if first.quota < least {
		t.Errorf("quota opened the silence at %.3f, below the %.3f the load earned",
			first.quota, least)
	}
	if first.quota <= float64(floor) {
		t.Errorf("quota sat at the floor (%d) through the silence", floor)
	}
}

// ---------------------------------------------------------------------------
// inbound: the concurrency limit
// ---------------------------------------------------------------------------

// TestSilenceDoesNotDecayTheLimit verifies that zero traffic does not decay
// the concurrency limit of an inbound service over real HTTP.
// Phases: A (load ramp) -> B (drain) -> C (silence) -> D (probe) -> E (resumed load).
func TestSilenceDoesNotDecayTheLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end load test")
	}

	const (
		rate    = 600.0 // requests per second offered
		workers = 40
		phase   = 3 * time.Second
	)

	cfg := tortureCfg()
	cfg.DownStep = 0.30

	var seq atomic.Int64
	srv, client, lim := newService(cfg, workload{base: 50 * time.Millisecond}, &seq)
	defer srv.Close()
	defer lim.Stop()
	lim.Start()

	subject := idleSubject{
		quota: func() float64 { return float64(lim.Limit()) },
		busy:  lim.InFlight,
		stats: lim.Stats,
	}

	// A: Load phase to ramp up the limit.
	load := drivePaced(client, srv.URL, lim, rate, workers, phase)
	load.label = "load"
	limitAtLoadEnd := lim.Limit()
	t.Logf("load   | %s", load)

	if load.ok200 < 500 {
		t.Fatalf("only %d requests were served under load; the harness is broken, not the limiter",
			load.ok200)
	}
	if limitAtLoadEnd <= cfg.InitialLimit {
		t.Fatalf("limit never grew past its initial %d under %.0f req/s; there is nothing to hold",
			cfg.InitialLimit, rate)
	}

	// B: Wait for in-flight requests to drain.
	if !waitIdle(subject, cfg.Tick, 4*cfg.Tick+time.Second) {
		t.Fatalf("service never went idle: inflight=%d samples=%d",
			lim.InFlight(), lim.Stats().Samples)
	}
	pre := lim.Stats()
	preQuota := float64(lim.Limit())
	if pre.Baseline <= 0 {
		t.Fatal("no baseline was established before the silence; phase A proved nothing")
	}

	// C: Silence phase, sampled twice per control tick.
	every := cfg.Tick / 2
	idle := sampleSilence(subject, phase, every)
	if want := int(phase / every); len(idle) < want/2 {
		t.Fatalf("only %d readings in the silence window, want about %d; the harness is broken",
			len(idle), want)
	}
	first := idle[0]
	t.Logf("silence| %d readings over %v: limit=%.0f rate=%.3f baseline=%v",
		len(idle), phase, first.quota, first.target, first.baseline)

	holdThroughSilence(t, idle, pre, preQuota, float64(limitAtLoadEnd)*(1-cfg.DownStep), cfg.FloorLimit)

	// D: First request after silence.
	began := time.Now()
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("first request after the silence failed: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	firstLat := time.Since(began)
	t.Logf("first request after silence: %d in %v", resp.StatusCode, firstLat)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("first request after the silence was shed: %d", resp.StatusCode)
	}

	// E: Resumed load to verify capacity was preserved.
	resumed := drivePaced(client, srv.URL, lim, rate, workers, phase)
	resumed.label = "resumed"
	t.Logf("resumed| %s", resumed)

	if resumed.ok200 == 0 {
		t.Fatal("nothing was served after the silence")
	}
	if resumed.shedPct() > load.shedPct()+5 {
		t.Errorf("shed %.1f%% after the silence against %.1f%% on the cold load: "+
			"the silence cost capacity", resumed.shedPct(), load.shedPct())
	}
	if resumed.shedPct() > 5 {
		t.Errorf("shed %.1f%% after the silence on clean load, want at most 5%%",
			resumed.shedPct())
	}
}
