package agilelimiter

import (
	"testing"
	"time"
)

// slowStartLimiter builds a limiter whose per-tick factor is exact: no SLOTarget
// (so the tail guard cannot interfere), a tick of one second, and a very long
// slow half-life so the baseline does not move during the test.
func slowStartLimiter(t *testing.T, gain float64) *Limiter {
	t.Helper()
	return New(Config{
		MinRate:         10,
		MaxRate:         1 << 20,
		InitialRate:     100,
		UpStep:          0.05,
		DownStep:        0.30,
		SlowStartGain:   gain,
		Tick:            time.Second,
		FastHalfLife:    time.Second,
		SlowHalfLife:    time.Minute,
		TargetInflation: 1.0,
	})
}

// While the controller has never seen congestion it does not know where the knee
// is, so a healthy tick multiplies instead of adding. 10 -> 500 costs six
// doublings; the same trip at +5% costs ln(50)/ln(1.05) = 80 ticks.
func TestSlowStartDoublesWhileHealthy(t *testing.T) {
	l := slowStartLimiter(t, 2.0)
	feed(l, 10*time.Millisecond, 100) // seeds the filters

	if !l.Stats().ColdStart {
		t.Fatal("a fresh limiter must report ColdStart")
	}
	prev := l.Rate()
	for i := 0; i < 3; i++ {
		feed(l, 10*time.Millisecond, 100)
		got := l.Rate()
		if got < prev*1.9 || got > prev*2.1 {
			t.Fatalf("tick %d: rate %v from %v, want roughly 2x", i, got, prev)
		}
		prev = got
	}
}

// One backoff is enough evidence that the knee has been found; from then on the
// controller must behave like ordinary additive increase, forever.
func TestSlowStartIsOverAfterTheFirstBackoff(t *testing.T) {
	l := slowStartLimiter(t, 2.0)
	feed(l, 10*time.Millisecond, 100)
	feed(l, 10*time.Millisecond, 100)
	before := l.Rate()

	feed(l, 500*time.Millisecond, 100) // congestion
	if l.Stats().ColdStart {
		t.Fatal("a backoff must end cold start")
	}
	if got := l.Rate(); got >= before {
		t.Fatalf("rate %v did not fall on congestion (was %v)", got, before)
	}

	// Let the fast EWMA forget the slow sample, then look at the first probe up.
	prev := l.Rate()
	for i := 0; i < 40; i++ {
		feed(l, 10*time.Millisecond, 100)
		got := l.Rate()
		if got > prev {
			if got > prev*1.10 {
				t.Fatalf("first probe after congestion was %.2fx; want the additive +5%%", got/prev)
			}
			return
		}
		prev = got
	}
	t.Fatal("the rate never probed up again")
}

// SlowStartGain 1 is the documented way to ask for pure AIMD from the first tick.
func TestSlowStartGainOneIsPureAIMD(t *testing.T) {
	l := slowStartLimiter(t, 1)
	feed(l, 10*time.Millisecond, 100)
	prev := l.Rate()
	feed(l, 10*time.Millisecond, 100)

	if got := l.Rate(); got > prev*1.06 {
		t.Fatalf("rate %v from %v: SlowStartGain 1 must add UpStep, not multiply", got, prev)
	}
}

// Saturation at MaxRate is also the end of cold start: there is nothing left to
// discover above the ceiling you set yourself.
func TestColdStartEndsAtMaxRate(t *testing.T) {
	l := New(Config{
		MinRate: 10, MaxRate: 200, InitialRate: 100,
		Tick: time.Second, FastHalfLife: time.Second, SlowHalfLife: time.Minute,
		TargetInflation: 1.0,
	})
	feed(l, 10*time.Millisecond, 100)
	feed(l, 10*time.Millisecond, 100) // 100 -> 200, clamped at MaxRate

	if got := l.Rate(); got != 200 {
		t.Fatalf("rate = %v, want the 200 ceiling", got)
	}
	if l.Stats().ColdStart {
		t.Fatal("ColdStart must clear once the rate is pinned at MaxRate")
	}
}

// The flag never re-arms: a recovery from an outage is an ordinary ramp, not a
// second slow start.
func TestColdStartNeverRearms(t *testing.T) {
	l := slowStartLimiter(t, 2.0)
	feed(l, 10*time.Millisecond, 100)
	feed(l, 500*time.Millisecond, 100) // congestion -> ColdStart cleared
	for i := 0; i < 5; i++ {
		feed(l, 10*time.Millisecond, 100)
	}
	if l.Stats().ColdStart {
		t.Fatal("ColdStart re-armed on its own")
	}
}

// Starting at the floor is what makes cold start expensive, and it is the worst
// possible guess: coming down costs about a seventh of what coming up costs.
func TestDefaultInitialRateIsAFractionOfTheCeiling(t *testing.T) {
	if got := New(Config{MinRate: 10, MaxRate: 800}).Rate(); got != 100 {
		t.Fatalf("default InitialRate = %v, want MaxRate/8 = 100", got)
	}
	// A ceiling that leaves no room above the floor still starts at the floor.
	if got := New(Config{MinRate: 10, MaxRate: 10}).Rate(); got != 10 {
		t.Fatalf("default InitialRate = %v, want the floor 10", got)
	}
	// An explicit initial is respected.
	if got := New(Config{MinRate: 10, MaxRate: 800, InitialRate: 30}).Rate(); got != 30 {
		t.Fatalf("explicit InitialRate = %v, want 30", got)
	}
}

// The same default reaches the concurrency limiter, where it is a starting
// semaphore size rather than a starting rate.
func TestConcurrencyDefaultInitialLimit(t *testing.T) {
	l := NewConcurrency(ConcurrencyConfig{MaxLimit: 800})
	defer l.Stop()
	if got := l.Limit(); got != 100 {
		t.Fatalf("default InitialLimit = %d, want MaxLimit/8 = 100", got)
	}
}
