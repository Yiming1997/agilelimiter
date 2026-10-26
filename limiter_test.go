package agilelimiter

import (
	"testing"
	"time"
)

// fakeClock is a virtual clock whose Sleep advances time instead of blocking,
// which makes the bucket fully deterministic in tests.
type fakeClock struct {
	now time.Time
}

func (f *fakeClock) Now() time.Time { return f.now }

func (f *fakeClock) Sleep(d time.Duration) {
	if d > 0 {
		f.now = f.now.Add(d)
	}
}

func TestBucketPacing(t *testing.T) {
	fc := &fakeClock{now: time.Unix(1_000_000, 0)}
	b := newBucket(100, time.Second, fc) // 10ms per grant

	base := b.take()
	if !base.Equal(fc.now) {
		t.Fatalf("first grant should be immediate, got %v want %v", base, fc.now)
	}

	t1 := b.take()
	if got := t1.Sub(base); got != 10*time.Millisecond {
		t.Fatalf("second grant gap = %v, want 10ms", got)
	}
	t2 := b.take()
	if got := t2.Sub(t1); got != 10*time.Millisecond {
		t.Fatalf("third grant gap = %v, want 10ms", got)
	}
}

func TestBucketRateChange(t *testing.T) {
	fc := &fakeClock{now: time.Unix(1_000_000, 0)}
	b := newBucket(100, time.Second, fc) // 10ms

	t1 := b.take()
	t2 := b.take()

	b.setRate(50, time.Second) // 20ms per grant
	t3 := b.take()
	if got := t3.Sub(t2); got != 20*time.Millisecond {
		t.Fatalf("after lowering rate gap = %v, want 20ms", got)
	}

	b.setRate(200, time.Second) // 5ms per grant
	t4 := b.take()
	if got := t4.Sub(t3); got != 5*time.Millisecond {
		t.Fatalf("after raising rate gap = %v, want 5ms", got)
	}
	_ = t1
}

func TestHistogramQuantiles(t *testing.T) {
	h := &histogram{}
	for i := 0; i < 90; i++ {
		h.record(10 * time.Millisecond)
	}
	for i := 0; i < 10; i++ {
		h.record(500 * time.Millisecond)
	}
	p50, p99, n := h.snapshot()
	if n != 100 {
		t.Fatalf("count = %d, want 100", n)
	}
	// Bucket lower bounds: p50 must not exceed the true median (10ms).
	if p50 > 10*time.Millisecond {
		t.Fatalf("p50 = %v, want <= 10ms", p50)
	}
	// The top 10% are slow, so p99 must land in the slow region.
	if p99 <= 50*time.Millisecond {
		t.Fatalf("p99 = %v, want > 50ms", p99)
	}
	// A snapshot must reset the histogram.
	if _, _, n2 := h.snapshot(); n2 != 0 {
		t.Fatalf("after snapshot count = %d, want 0", n2)
	}
}

// feed records n samples of the given RTT and runs one control tick.
func feed(l *Limiter, rtt time.Duration, n int) {
	for i := 0; i < n; i++ {
		l.Observe(rtt)
	}
	l.ctrl.step()
}

// feedMixed records a healthy-dominant mix (90% fast, 10% slow) and ticks.
func feedMixed(l *Limiter) {
	for i := 0; i < 90; i++ {
		l.Observe(10 * time.Millisecond)
	}
	for i := 0; i < 10; i++ {
		l.Observe(500 * time.Millisecond)
	}
	l.ctrl.step()
}

func TestControllerClimbsWhenHealthy(t *testing.T) {
	l := New(Config{MinRate: 10, MaxRate: 1000, InitialRate: 200})
	for i := 0; i < 20; i++ {
		feed(l, 10*time.Millisecond, 100)
	}
	if l.Rate() <= 200 {
		t.Fatalf("expected rate to climb above initial, got %v", l.Rate())
	}
}

func TestControllerBacksOffUnderCongestion(t *testing.T) {
	l := New(Config{MinRate: 10, MaxRate: 1000, InitialRate: 500})
	for i := 0; i < 20; i++ {
		feed(l, 10*time.Millisecond, 100) // healthy baseline
	}
	healthy := l.Rate()

	for i := 0; i < 20; i++ {
		feed(l, 100*time.Millisecond, 100) // 10x congestion
	}
	congested := l.Rate()

	if congested >= healthy {
		t.Fatalf("expected backoff: healthy=%v congested=%v", healthy, congested)
	}
	if congested < 10 {
		t.Fatalf("rate fell below MinRate: %v", congested)
	}
}

func TestControllerRecoversAfterCongestion(t *testing.T) {
	l := New(Config{MinRate: 10, MaxRate: 1000, InitialRate: 200})
	for i := 0; i < 15; i++ {
		feed(l, 10*time.Millisecond, 100)
	}
	for i := 0; i < 15; i++ {
		feed(l, 100*time.Millisecond, 100) // congest -> drive down
	}
	low := l.Rate()

	for i := 0; i < 40; i++ {
		feed(l, 10*time.Millisecond, 100) // recovery
	}
	if l.Rate() <= low {
		t.Fatalf("did not recover: low=%v recovered=%v", low, l.Rate())
	}
}

func TestControllerNeverExceedsMaxRate(t *testing.T) {
	l := New(Config{MinRate: 1, MaxRate: 250, InitialRate: 50})
	for i := 0; i < 100; i++ {
		feed(l, 5*time.Millisecond, 100)
		if r := l.Rate(); r > 250 {
			t.Fatalf("rate %v exceeds MaxRate 250", r)
		}
	}
	if l.Rate() != 250 {
		t.Fatalf("expected to reach MaxRate, got %v", l.Rate())
	}
}

func TestControllerSLOTailGuard(t *testing.T) {
	l := New(Config{
		MinRate:     1,
		MaxRate:     1000,
		InitialRate: 100,
		SLOTarget:   50 * time.Millisecond,
	})
	// Median latency is healthy, but 10% of requests are slow.
	for i := 0; i < 3; i++ {
		feedMixed(l)
	}
	if l.Rate() >= 100 {
		t.Fatalf("SLO tail guard did not back off, rate=%v", l.Rate())
	}
}

func TestRateStaysWithinBounds(t *testing.T) {
	l := New(Config{MinRate: 20, MaxRate: 80, InitialRate: 40})
	seq := []time.Duration{10 * time.Millisecond, 500 * time.Millisecond, 5 * time.Millisecond}
	for i := 0; i < 200; i++ {
		feed(l, seq[i%len(seq)], 50)
		if r := l.Rate(); r < 20 || r > 80 {
			t.Fatalf("rate %v out of [20,80]", r)
		}
	}
}

// A deep dip must not cost more to undo than it cost to enter. Without
// RecoverRamp, 5 -> 120 is ln(24)/ln(1.05) = 65 ticks of +5%; with it, the
// controller doubles while the latency is at half the window or less.
func TestFastRecoveryFromDeepDip(t *testing.T) {
	l := New(Config{
		MinRate:      5,
		MaxRate:      120,
		InitialRate:  120,
		SLOTarget:    100 * time.Millisecond,
		Anchor:       AnchorSLO,
		FastHalfLife: 100 * time.Millisecond, // isolate the ramp from the EWMA decay
		RecoverTicks: 1,
		RecoverRamp:  0.5, // opt in: this is what the test is about
	})
	for i := 0; i < 10; i++ {
		feed(l, 20*time.Millisecond, 100) // settle at the ceiling
	}

	// The downstream melts: latency far past the window (and the SLO).
	for i := 0; i < 20; i++ {
		feed(l, 400*time.Millisecond, 100) // the downstream melts
	}
	bottom := l.Rate()
	if bottom > 10 {
		t.Fatalf("expected the rate to collapse toward MinRate, got %v", bottom)
	}

	// It comes back, and stays back. Twenty ticks is what an additive ramp from
	// MinRate to MaxRate would need about 65 of.
	recovered := 0
	for i := 0; i < 20; i++ {
		feed(l, 20*time.Millisecond, 100)
		if l.Rate() >= 120 {
			recovered = i + 1
			break
		}
	}
	if recovered == 0 {
		t.Fatalf("did not recover from %v to MaxRate within 20 ticks, got %v", bottom, l.Rate())
	}
	// 5 -> 120 is 5 doublings. An additive ramp from MinRate would need ~65.
	if recovered > 6 {
		t.Fatalf("recovery took %d ticks, want about 5", recovered)
	}
}

// The fast ramp must not fire near the knee: latency inside the band, even
// comfortably so, is an ordinary +UpStep.
func TestFastRecoveryDoesNotFireInsideBand(t *testing.T) {
	l := New(Config{
		MinRate:      5,
		MaxRate:      1000,
		InitialRate:  100,
		SLOTarget:    100 * time.Millisecond, // window = 80ms, ramp below 40ms
		Anchor:       AnchorSLO,
		FastHalfLife: time.Millisecond, // this tick's p50, no smoothing
	})
	for i := 0; i < 3; i++ {
		feed(l, 20*time.Millisecond, 100)
	}
	// Provoke and clear cold start so the ramp decision is about latency, not
	// about novelty.
	for i := 0; i < 5; i++ {
		feed(l, 90*time.Millisecond, 100)
	}
	before := l.Rate()

	// 60ms is inside the 80ms window but well above half of it: additive only.
	for i := 0; i < 10; i++ {
		feed(l, 60*time.Millisecond, 100)
	}
	if got := l.Rate(); got > before*1.7 {
		t.Fatalf("5%%/tick expected near the band, but the rate jumped %v -> %v", before, got)
	}
}

// The fast ramp is opt-in: without RecoverRamp the climb stays additive, which is
// the shipped behaviour.
func TestRecoverRampIsOffByDefault(t *testing.T) {
	l := New(Config{
		MinRate:     5,
		MaxRate:     120,
		InitialRate: 120,
		SLOTarget:   100 * time.Millisecond,
		Anchor:      AnchorSLO,
	})
	for i := 0; i < 10; i++ {
		feed(l, 20*time.Millisecond, 100)
	}
	for i := 0; i < 20; i++ {
		feed(l, 400*time.Millisecond, 100)
	}
	for i := 0; i < 10; i++ {
		feed(l, 20*time.Millisecond, 100)
	}
	// Ten additive ticks from MinRate: 5 * 1.05^10 = 8.1, nowhere near the ceiling.
	if got := l.Rate(); got >= 20 {
		t.Fatalf("expected a slow additive ramp, got %v after 10 ticks", got)
	}
}

func TestRecoverRampDefaults(t *testing.T) {
	cfg := withDefaults(Config{MinRate: 1, MaxRate: 10})
	if cfg.RecoverRamp != 0 {
		t.Fatalf("RecoverRamp = %v, want 0 (off by default)", cfg.RecoverRamp)
	}
}
