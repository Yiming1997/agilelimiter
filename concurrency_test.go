package agilelimiter

import (
	"errors"
	"testing"
	"time"
)

// feedConc records n samples of the given handling time and runs one control
// tick, mirroring feed for the outbound limiter.
func feedConc(l *ConcurrencyLimiter, d time.Duration, n int) {
	for i := 0; i < n; i++ {
		l.Observe(d)
	}
	l.ctrl.step()
}

func TestConcurrencyAdmitsUpToLimit(t *testing.T) {
	l := NewConcurrency(ConcurrencyConfig{MinLimit: 1, MaxLimit: 8, InitialLimit: 2})

	if !l.TryTake() {
		t.Fatal("first acquire should succeed")
	}
	if !l.TryTake() {
		t.Fatal("second acquire should succeed")
	}
	if l.TryTake() {
		t.Fatal("third acquire should be refused at limit 2")
	}
	if got := l.InFlight(); got != 2 {
		t.Fatalf("in-flight = %d, want 2", got)
	}

	l.Release()
	if !l.TryTake() {
		t.Fatal("acquire should succeed after a release")
	}
	if got := l.InFlight(); got != 2 {
		t.Fatalf("in-flight = %d, want 2", got)
	}
}

func TestDoRejectsWhenFull(t *testing.T) {
	l := NewConcurrency(ConcurrencyConfig{MinLimit: 1, MaxLimit: 1, InitialLimit: 1})

	if !l.TryTake() {
		t.Fatal("should acquire the only slot")
	}

	ran := false
	err := l.Do(func() error { ran = true; return nil })
	if !errors.Is(err, ErrOverloaded) {
		t.Fatalf("want ErrOverloaded, got %v", err)
	}
	if ran {
		t.Fatal("fn must not run when overloaded")
	}

	l.Release()
	if err := l.Do(func() error { ran = true; return nil }); err != nil {
		t.Fatalf("unexpected error after release: %v", err)
	}
	if !ran {
		t.Fatal("fn should have run once a slot was free")
	}
}

func TestDoReleasesAndObserves(t *testing.T) {
	l := NewConcurrency(ConcurrencyConfig{MinLimit: 1, MaxLimit: 4, InitialLimit: 2})

	if err := l.Do(func() error { return nil }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := l.InFlight(); got != 0 {
		t.Fatalf("in-flight after Do = %d, want 0", got)
	}
	if _, _, n := l.hist.snapshot(); n != 1 {
		t.Fatalf("expected exactly 1 observation, got %d", n)
	}
}

func TestConcurrencyLimitClimbsWhenHealthy(t *testing.T) {
	l := NewConcurrency(ConcurrencyConfig{MinLimit: 1, MaxLimit: 1000, InitialLimit: 100})
	for i := 0; i < 20; i++ {
		feedConc(l, 10*time.Millisecond, 100)
	}
	if l.Limit() <= 100 {
		t.Fatalf("expected limit to climb above initial, got %d", l.Limit())
	}
}

func TestConcurrencyBacksOffUnderCongestion(t *testing.T) {
	l := NewConcurrency(ConcurrencyConfig{MinLimit: 10, MaxLimit: 1000, InitialLimit: 500})
	for i := 0; i < 20; i++ {
		feedConc(l, 10*time.Millisecond, 100) // healthy baseline
	}
	healthy := l.Limit()

	for i := 0; i < 20; i++ {
		feedConc(l, 100*time.Millisecond, 100) // 10x congestion
	}
	congested := l.Limit()

	if congested >= healthy {
		t.Fatalf("expected backoff: healthy=%d congested=%d", healthy, congested)
	}
	if congested < 10 {
		t.Fatalf("limit fell below MinLimit: %d", congested)
	}
}

func TestConcurrencyRecoversAfterCongestion(t *testing.T) {
	l := NewConcurrency(ConcurrencyConfig{MinLimit: 10, MaxLimit: 1000, InitialLimit: 200})
	for i := 0; i < 15; i++ {
		feedConc(l, 10*time.Millisecond, 100)
	}
	for i := 0; i < 15; i++ {
		feedConc(l, 100*time.Millisecond, 100) // congest -> drive down
	}
	low := l.Limit()

	for i := 0; i < 40; i++ {
		feedConc(l, 10*time.Millisecond, 100) // recovery
	}
	if l.Limit() <= low {
		t.Fatalf("did not recover: low=%d recovered=%d", low, l.Limit())
	}
}

func TestConcurrencyNeverExceedsMaxLimit(t *testing.T) {
	l := NewConcurrency(ConcurrencyConfig{MinLimit: 1, MaxLimit: 250, InitialLimit: 50})
	for i := 0; i < 100; i++ {
		feedConc(l, 5*time.Millisecond, 100)
		if c := l.Limit(); c > 250 {
			t.Fatalf("limit %d exceeds MaxLimit 250", c)
		}
	}
	if l.Limit() != 250 {
		t.Fatalf("expected to reach MaxLimit, got %d", l.Limit())
	}
}

func TestConcurrencySLOTailGuard(t *testing.T) {
	l := NewConcurrency(ConcurrencyConfig{
		MinLimit:     1,
		MaxLimit:     1000,
		InitialLimit: 100,
		SLOTarget:    50 * time.Millisecond,
	})
	// Median handling time is healthy, but 10% of requests are slow.
	for i := 0; i < 3; i++ {
		for j := 0; j < 90; j++ {
			l.Observe(10 * time.Millisecond)
		}
		for j := 0; j < 10; j++ {
			l.Observe(500 * time.Millisecond)
		}
		l.ctrl.step()
	}
	if l.Limit() >= 100 {
		t.Fatalf("SLO tail guard did not back off, limit=%d", l.Limit())
	}
}

func TestConcurrencyLimitStaysWithinBounds(t *testing.T) {
	l := NewConcurrency(ConcurrencyConfig{MinLimit: 20, MaxLimit: 80, InitialLimit: 40})
	seq := []time.Duration{10 * time.Millisecond, 500 * time.Millisecond, 5 * time.Millisecond}
	for i := 0; i < 200; i++ {
		feedConc(l, seq[i%len(seq)], 50)
		if c := l.Limit(); c < 20 || c > 80 {
			t.Fatalf("limit %d out of [20,80]", c)
		}
	}
}
