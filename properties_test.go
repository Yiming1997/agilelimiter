package agilelimiter

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The clamps are the promise to the downstream ("never more than MaxRate") and to
// the service ("never fewer than MinRate"). Fuzz the control law against them
// rather than checking a couple of hand-picked traces.
func TestRateNeverEscapesBoundsUnderRandomLoad(t *testing.T) {
	const min, max = 20, 200
	l := New(Config{
		MinRate: min, MaxRate: max, InitialRate: 100,
		SLOTarget: 300 * time.Millisecond,
		UpStep:    0.9, // deliberately brutal, to push against both clamps
		DownStep:  0.9,
	})
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 5000; i++ {
		n := 1 + rng.Intn(50)
		d := time.Duration(1+rng.Intn(900)) * time.Millisecond
		for j := 0; j < n; j++ {
			l.Observe(d)
		}
		l.ctrl.step()
		if got := l.Rate(); got < min || got > max {
			t.Fatalf("tick %d: rate %v escaped [%d, %d]", i, got, min, max)
		}
	}
}

// MaxRate is "a number you are willing to promise the downstream", so exceeding
// it is a correctness bug, not a tuning issue — even with many callers racing.
func TestBucketNeverExceedsItsRate(t *testing.T) {
	const rate = 500
	b := newBucket(rate, time.Second, realClock{})

	var grants atomic.Int64
	var wg sync.WaitGroup
	deadline := time.Now().Add(200 * time.Millisecond)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				b.take()
				grants.Add(1)
			}
		}()
	}
	wg.Wait()

	// 200ms at 500/s is 100 grants; allow the first-call realign plus one in
	// flight per goroutine.
	if got := grants.Load(); got > 125 {
		t.Fatalf("%d grants in 200ms at %d/s, want <= 125", got, rate)
	}
	if got := grants.Load(); got == 0 {
		t.Fatal("bucket granted nothing at all")
	}
}

// The sub-bucket rewrite claims ~19% resolution: the reported quantile is the
// bucket's lower bound, so it must never exceed the sample and never fall more
// than one bucket width below it.
func TestHistogramResolutionIsSubBucket(t *testing.T) {
	for d := 4 * time.Nanosecond; d < time.Minute; d += d/7 + 1 {
		h := &histogram{}
		h.record(d)
		p50, _, n := h.snapshot()
		if n != 1 {
			t.Fatalf("n = %d, want 1", n)
		}
		if p50 > d {
			t.Fatalf("quantile %v exceeds the recorded sample %v", p50, d)
		}
		if p50 < d-d/5 {
			t.Fatalf("quantile %v is more than 20%% below the recorded sample %v", p50, d)
		}
	}
}

// A quantile over unrecorded durations must stay ordered: doubling the sample
// must not lower the reported value.
func TestHistogramQuantileIsMonotonic(t *testing.T) {
	var last time.Duration
	for d := 10 * time.Millisecond; d < time.Second; d *= 2 {
		h := &histogram{}
		for i := 0; i < 100; i++ {
			h.record(d)
		}
		p50, p99, _ := h.snapshot()
		if p50 < last {
			t.Fatalf("p50 fell from %v to %v as the sample grew", last, p50)
		}
		if p99 < p50 {
			t.Fatalf("p99 %v < p50 %v", p99, p50)
		}
		last = p50
	}
}

// Unavailable is called from every failing caller during an outage, so it has to
// be safe to hammer concurrently.
func TestUnavailableIsConcurrencySafe(t *testing.T) {
	l := New(Config{MinRate: 10, MaxRate: 1000, InitialRate: 500,
		SLOTarget: 200 * time.Millisecond})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				l.Unavailable()
				l.Observe(time.Millisecond)
			}
		}()
	}
	wg.Wait()
	for i := 0; i < 20; i++ {
		l.ctrl.step()
	}
	if got := l.Rate(); got < 10 || got > 1000 {
		t.Fatalf("rate %v escaped its bounds", got)
	}
}
