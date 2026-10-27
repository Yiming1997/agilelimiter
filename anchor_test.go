package agilelimiter

import (
	"testing"
	"time"
)

// The scenario AnchorSLO exists for: the downstream is healthy, it just answers
// a little slower for a while. 20ms -> 50ms against a 200ms SLO is a quarter of
// the budget, and 2.5x the fastest response ever seen. The relative law reads
// the ratio and collapses; the absolute law reads the number and does not move.
func TestSLOAnchorIgnoresBenignRise(t *testing.T) {
	l := New(Config{
		MinRate:     10,
		MaxRate:     500,
		InitialRate: 200,
		SLOTarget:   200 * time.Millisecond,
		Anchor:      AnchorSLO,
	})
	for i := 0; i < 40; i++ {
		feed(l, 20*time.Millisecond, 100)
	}
	if got := l.Rate(); got != 500 {
		t.Fatalf("expected to saturate at 500 while healthy, got %v", got)
	}

	for i := 0; i < 40; i++ {
		feed(l, 50*time.Millisecond, 100) // the benign bump
	}
	if got := l.Rate(); got != 500 {
		t.Fatalf("a 50ms latency under a 200ms SLO must cost no rate, got %v", got)
	}
}

// The same rise against the default anchor, pinned as the regression the new
// mode is an answer to: the rate falls to MinRate and stays there.
func TestBaselineAnchorCollapsesOnBenignRise(t *testing.T) {
	l := New(Config{
		MinRate:     10,
		MaxRate:     500,
		InitialRate: 200,
		SLOTarget:   200 * time.Millisecond, // 4x the latency that triggers the collapse
	})
	for i := 0; i < 40; i++ {
		feed(l, 20*time.Millisecond, 100)
	}
	for i := 0; i < 40; i++ {
		feed(l, 50*time.Millisecond, 100)
	}
	if got := l.Rate(); got != 10 {
		t.Fatalf("expected the relative anchor to fall to MinRate 10, got %v", got)
	}
}

// The absolute anchor still defends the SLO: once the median is close to the
// threshold (0.8 x SLO), it backs off.
func TestSLOAnchorBacksOffNearSLO(t *testing.T) {
	l := New(Config{
		MinRate:     10,
		MaxRate:     500,
		InitialRate: 400,
		SLOTarget:   100 * time.Millisecond, // window = 80ms
		Anchor:      AnchorSLO,
	})
	before := l.Rate()
	for i := 0; i < 20; i++ {
		feed(l, 95*time.Millisecond, 100) // over the 80ms window, under the SLO
	}
	if got := l.Rate(); got >= before {
		t.Fatalf("expected a backoff at 95ms against an 80ms window, rate=%v (was %v)", got, before)
	}
}

// Hysteresis: one tick on the wrong side is not evidence, two are.
func TestBackoffTicksNeedAgreement(t *testing.T) {
	l := New(Config{
		MinRate:      10,
		MaxRate:      500,
		InitialRate:  300,
		BackoffTicks: 2,
		RecoverTicks: 2,
	})
	for i := 0; i < 30; i++ {
		feed(l, 10*time.Millisecond, 100) // settle at MaxRate, cold start cleared
	}
	before := l.Rate()

	feed(l, 200*time.Millisecond, 100) // one bad tick
	if got := l.Rate(); got != before {
		t.Fatalf("a single tick moved the rate: %v -> %v", before, got)
	}

	feed(l, 200*time.Millisecond, 100) // the second agreeing tick
	if got := l.Rate(); got >= before {
		t.Fatalf("two agreeing ticks must back off, rate=%v (was %v)", got, before)
	}
}

// A tick inside the band breaks the run: agreement must be consecutive.
//
// FastHalfLife is one tick, so the fast signal is exactly this tick's p50. With
// the 3s default an alternating signal never returns to the band on its own —
// the EWMA needs several ticks to decay — and the test would measure the
// smoothing rather than the counter.
func TestHysteresisRequiresConsecutiveTicks(t *testing.T) {
	l := New(Config{
		MinRate:      10,
		MaxRate:      500,
		InitialRate:  300,
		FastHalfLife: time.Millisecond,
		BackoffTicks: 2,
		RecoverTicks: 2,
	})
	for i := 0; i < 30; i++ {
		feed(l, 10*time.Millisecond, 100)
	}
	before := l.Rate()

	for i := 0; i < 10; i++ {
		feed(l, 200*time.Millisecond, 100) // bad, but never twice in a row
		feed(l, 10*time.Millisecond, 100)  // good: resets the run
	}
	if got := l.Rate(); got != before {
		t.Fatalf("alternating jitter must not move the rate: %v -> %v", before, got)
	}
}

// RecoverTicks gates the up direction the same way.
func TestRecoverTicksNeedAgreement(t *testing.T) {
	l := New(Config{
		MinRate:      10,
		MaxRate:      500,
		InitialRate:  300,
		FastHalfLife: time.Millisecond, // see TestHysteresisRequiresConsecutiveTicks
		BackoffTicks: 1,
		RecoverTicks: 3,
	})
	for i := 0; i < 30; i++ {
		feed(l, 10*time.Millisecond, 100) // healthy baseline, saturate
	}
	for i := 0; i < 20; i++ {
		feed(l, 200*time.Millisecond, 100) // drive down, clears cold start
	}
	low := l.Rate()

	feed(l, 10*time.Millisecond, 100) // one healthy tick
	if got := l.Rate(); got != low {
		t.Fatalf("one healthy tick moved the rate before RecoverTicks agreed: %v -> %v", low, got)
	}
	feed(l, 10*time.Millisecond, 100) // two
	if got := l.Rate(); got != low {
		t.Fatalf("two healthy ticks moved the rate before RecoverTicks agreed: %v -> %v", low, got)
	}
	feed(l, 10*time.Millisecond, 100) // three: now it may probe
	if got := l.Rate(); got <= low {
		t.Fatalf("three agreeing healthy ticks must probe up, rate=%v (was %v)", got, low)
	}
}

// AnchorSLO without an SLO has nothing to anchor to: it must behave like the
// relative law rather than produce a zero-width window.
func TestSLOAnchorWithoutSLOFallsBack(t *testing.T) {
	cfg := withDefaults(Config{MinRate: 10, MaxRate: 500, Anchor: AnchorSLO})
	// The request is kept, so Config() can report the fallback instead of hiding
	// it; what changes is the law that actually runs.
	if cfg.Anchor != AnchorSLO {
		t.Fatalf("Anchor = %v, want the request preserved for reporting", cfg.Anchor)
	}
	if cfg.BackoffTicks != 1 || cfg.RecoverTicks != 1 {
		t.Fatalf("ticks = %d/%d, want 1/1 for the baseline anchor", cfg.BackoffTicks, cfg.RecoverTicks)
	}

	l := New(Config{MinRate: 10, MaxRate: 500, InitialRate: 200, Anchor: AnchorSLO})
	for i := 0; i < 40; i++ {
		feed(l, 20*time.Millisecond, 100)
	}
	for i := 0; i < 40; i++ {
		feed(l, 50*time.Millisecond, 100)
	}
	if got := l.Rate(); got >= 200 {
		t.Fatalf("expected the relative law (backoff on a 2.5x rise), got %v", got)
	}
}

// The defaults must not move: AnchorBaseline with no hysteresis is the shipped
// behaviour, and the SLO anchor gets 2/2 without being asked.
func TestAnchorDefaults(t *testing.T) {
	base := withDefaults(Config{MinRate: 1, MaxRate: 10, SLOTarget: time.Second})
	if base.BackoffTicks != 1 || base.RecoverTicks != 1 {
		t.Fatalf("baseline anchor ticks = %d/%d, want 1/1", base.BackoffTicks, base.RecoverTicks)
	}

	slo := withDefaults(Config{MinRate: 1, MaxRate: 10, SLOTarget: time.Second, Anchor: AnchorSLO})
	if slo.BackoffTicks != 2 || slo.RecoverTicks != 2 {
		t.Fatalf("SLO anchor ticks = %d/%d, want 2/2", slo.BackoffTicks, slo.RecoverTicks)
	}
}
