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

// Recovery from a deep dip is an additive ramp, and this test is the reason the
// library documents "we do not do this" instead of shipping a knob for it.
//
// Re-arming the multiplicative probe when the latency fell far below the window
// was implemented, measured and removed: it fixed recovery (25 -> 52 req/s of
// goodput when capacity came back) and cost more elsewhere (59 -> 46 when capacity
// dropped), because doubling is also how you overshoot a cliff. See
// docs/design.md §9.6.
func TestRecoveryFromDeepDipIsAdditive(t *testing.T) {
	l := New(Config{
		MinRate:     5,
		MaxRate:     120,
		InitialRate: 120,
		SLOTarget:   100 * time.Millisecond,
		Anchor:      AnchorSLO,
	})
	for i := 0; i < 10; i++ {
		feed(l, 20*time.Millisecond, 100) // settle at the ceiling, clear cold start
	}
	for i := 0; i < 20; i++ {
		feed(l, 400*time.Millisecond, 100) // the downstream melts
	}
	if bottom := l.Rate(); bottom > 10 {
		t.Fatalf("expected the rate to collapse toward MinRate, got %v", bottom)
	}

	for i := 0; i < 10; i++ {
		feed(l, 20*time.Millisecond, 100) // it comes back
	}
	// Ten additive ticks from MinRate: 5 * 1.05^10 = 8.1. A multiplicative probe
	// would have reached the ceiling in five, which is what we decided against.
	if got := l.Rate(); got >= 20 {
		t.Fatalf("recovery must stay additive, got %v after 10 ticks", got)
	}
}
