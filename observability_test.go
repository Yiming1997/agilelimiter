package agilelimiter

import (
	"testing"
	"time"
)

// A frozen rate means two completely different things — "nothing changed" and
// "the controller saw nothing" — and before Stats.Samples they looked identical.
func TestStatsReportsWhatTheLastTickSaw(t *testing.T) {
	l := New(Config{MinRate: 10, MaxRate: 1000, InitialRate: 100})

	if s := l.Stats(); s.Samples != 0 || s.P50 != 0 || s.P99 != 0 {
		t.Fatalf("a limiter that never ticked reports %+v, want zeroes", s)
	}

	for i := 0; i < 25; i++ {
		l.Observe(20 * time.Millisecond)
	}
	l.ctrl.step()

	s := l.Stats()
	if s.Samples != 25 {
		t.Fatalf("Samples = %d, want 25", s.Samples)
	}
	if s.P50 == 0 || s.P99 == 0 {
		t.Fatalf("P50/P99 = %v/%v, want the tick's quantiles", s.P50, s.P99)
	}
	if s.P50 > 20*time.Millisecond {
		t.Fatalf("P50 = %v, must not exceed the true median 20ms", s.P50)
	}

	// A tick with no samples must say so, rather than silently re-reporting the
	// previous one as if it were fresh.
	l.ctrl.step()
	if s := l.Stats(); s.Samples != 0 || s.P50 != 0 || s.P99 != 0 {
		t.Fatalf("after a blind tick Stats = %+v, want zeroes", s)
	}
}

// Two control loops would double the step rate, and every gain in the control law
// is a per-tick increment, so that silently doubles the gains too.
func TestStartIsIdempotent(t *testing.T) {
	l := New(Config{
		MinRate:      10,
		MaxRate:      200_000,
		InitialRate:  100,
		UpStep:       0.5, // 1.5x per tick makes N loops obvious after a few ticks
		Tick:         20 * time.Millisecond,
		FastHalfLife: 10 * time.Millisecond,
		SlowHalfLife: 10 * time.Millisecond,
		SLOTarget:    time.Second,
	})
	l.Start()
	l.Start()
	l.Start()
	defer l.Stop()

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		l.Observe(time.Millisecond)
		time.Sleep(time.Millisecond)
	}

	// One loop: ~10 steps -> ~6k. Three loops: ~30 steps -> MaxRate.
	if got := l.Rate(); got > 50_000 {
		t.Fatalf("rate climbed to %v; Start ran more than one control loop", got)
	}
}

func TestConcurrencyStartIsIdempotent(t *testing.T) {
	l := NewConcurrency(ConcurrencyConfig{
		MinLimit:         2,
		MaxLimit:         200_000,
		InitialLimit:     100,
		UpStep:           0.5,
		Tick:             20 * time.Millisecond,
		FastHalfLife:     10 * time.Millisecond,
		SlowHalfLife:     10 * time.Millisecond,
		SlowDownHalfLife: 10 * time.Millisecond,
		SLOTarget:        time.Second,
	})
	l.Start()
	l.Start()
	defer l.Stop()

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		l.Observe(time.Millisecond)
		time.Sleep(time.Millisecond)
	}
	if got := l.Limit(); got > 50_000 {
		t.Fatalf("limit climbed to %v; Start ran more than one control loop", got)
	}
}

// quantile must use nearest-rank (ceil), not truncation. With truncation q=0.99
// over two samples ranked the *faster* one, so a tick holding one fast and one
// timed-out request reported the timeout as healthy and the tail guard could not
// fire on the exact tick it exists for.
func TestQuantileNearestRank(t *testing.T) {
	one := &histogram{}
	one.record(3 * time.Second)
	if _, p99, _ := one.snapshot(); p99 < time.Second {
		t.Fatalf("p99 of a single slow sample = %v, want the sample itself", p99)
	}

	two := &histogram{}
	two.record(10 * time.Millisecond)
	two.record(3 * time.Second)
	if _, p99, _ := two.snapshot(); p99 < time.Second {
		t.Fatalf("p99 of (10ms, 3s) = %v; nearest-rank must pick the 3s sample", p99)
	}

	// At 100 samples the outlier is exactly 1%, and p99 is the 99th of 100 - the
	// second slowest - so a lone outlier no longer defines it.
	hundred := &histogram{}
	for i := 0; i < 99; i++ {
		hundred.record(10 * time.Millisecond)
	}
	hundred.record(3 * time.Second)
	if _, p99, _ := hundred.snapshot(); p99 > time.Second {
		t.Fatalf("p99 of 99 fast + 1 slow = %v, want a fast value", p99)
	}
}

// The behavioural half of the same fix: one timeout in a two-sample tick must
// move the rate, because that tick's p99 is the timeout.
func TestTailGuardFiresOnOneTimeout(t *testing.T) {
	l := New(Config{
		MinRate: 10, MaxRate: 1000, InitialRate: 500,
		SLOTarget: 200 * time.Millisecond,
		Tick:      250 * time.Millisecond,
	})
	for i := 0; i < 200; i++ {
		l.Observe(10 * time.Millisecond)
	}
	l.ctrl.step()
	base := l.Rate()

	l.Observe(10 * time.Millisecond)
	l.Observe(3 * time.Second) // p50 stays fast, p99 is the timeout
	l.ctrl.step()

	if got := l.Rate(); got >= base {
		t.Fatalf("rate %v did not back off from %v; the tail guard missed the timeout", got, base)
	}
}
