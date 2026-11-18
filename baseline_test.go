package agilelimiter

import (
	"math"
	"testing"
	"time"
)

// baselineAt feeds n ticks of the same RTT and returns the baseline.
func baselineAt(l *Limiter, rtt time.Duration, n int) float64 {
	for i := 0; i < n; i++ {
		l.Observe(rtt)
		l.ctrl.step()
	}
	return l.ctrl.slow
}

// TestBaselineRiseCapIsPerSecond pins the unit of the rise cap: a fraction per
// second of wall clock, so one tick may spend
//
//	slow × MaxBaselineRise × (Tick / 1s)
//
// Before the fix the cap was a fraction per *tick*, which made it disagree
// with SlowHalfLife — two knobs that describe the same physical quantity —
// whenever the control loop did not run once a second.
func TestBaselineRiseCapIsPerSecond(t *testing.T) {
	const rise = 0.002
	for _, tick := range []time.Duration{
		time.Second,
		500 * time.Millisecond,
		250 * time.Millisecond,
		100 * time.Millisecond,
	} {
		l := New(Config{
			MaxRate:         100,
			InitialRate:     100,
			SLOTarget:       time.Second,
			Tick:            tick,
			MaxBaselineRise: rise,
		})
		baselineAt(l, 10*time.Millisecond, 50) // let the baseline settle
		before := l.ctrl.slow
		// One huge jump: the EWMA wants far more than the cap allows, so the
		// cap is what decides.
		baselineAt(l, 500*time.Millisecond, 1)
		got := l.ctrl.slow - before
		want := before * rise * (float64(tick) / float64(time.Second))
		if rel := math.Abs(got-want) / want; rel > 1e-9 {
			t.Fatalf("tick=%v: baseline rose %.9f in one tick, want %.9f "+
				"(before=%.0f, cap = slow × %.4f × %.3f)", tick, got, want, before, rise, float64(tick)/float64(time.Second))
		}
	}
}

// TestBaselineRiseIsIndependentOfTick is the property the fix buys: over the
// same wall-clock time the baseline climbs by the same amount, whatever the
// control-loop period.
//
// Before the fix the cap was per tick, so a 250ms tick let the baseline climb
// 61.5% per minute while a 1s tick let it climb 12.7% — changing how often the
// controller decided quietly changed what the limiter tolerated.
func TestBaselineRiseIsIndependentOfTick(t *testing.T) {
	const wall = 60 * time.Second
	var ref float64
	for i, tick := range []time.Duration{
		time.Second,
		500 * time.Millisecond,
		250 * time.Millisecond,
		100 * time.Millisecond,
	} {
		l := New(Config{
			MaxRate:     100,
			InitialRate: 100,
			SLOTarget:   time.Second,
			Tick:        tick,
		})
		baselineAt(l, 10*time.Millisecond, 50)
		base := l.ctrl.slow
		// The downstream slows down for good; advance one minute of wall clock.
		baselineAt(l, 100*time.Millisecond, int(wall/tick))
		got := l.ctrl.slow / base
		if i == 0 {
			ref = got
			continue
		}
		if rel := math.Abs(got-ref) / ref; rel > 0.01 {
			t.Fatalf("tick=%v: after %s the baseline is %.4fx its old value, "+
				"tick=1s gives %.4fx — the climb rate must not depend on Tick",
				tick, wall, got, ref)
		}
	}
}

// TestBaselineRiseUnchangedAtOneSecondTick pins the compatibility promise: at
// the default one-second tick the new formula is bit-for-bit the old one, so
// the default value needs no migration.
func TestBaselineRiseUnchangedAtOneSecondTick(t *testing.T) {
	l := New(Config{
		MaxRate:         100,
		InitialRate:     100,
		SLOTarget:       time.Second,
		Tick:            time.Second,
		MaxBaselineRise: 0.002,
	})
	baselineAt(l, 10*time.Millisecond, 50)
	before := l.ctrl.slow
	baselineAt(l, 500*time.Millisecond, 1)
	got := l.ctrl.slow - before
	want := before * 0.002 // the old formula, unscaled
	if math.Abs(got-want) > want*1e-9 {
		t.Fatalf("baseline rose %.9f in one tick, want %.9f — at a one-second "+
			"tick the behaviour must not change", got, want)
	}
}

// TestConcurrencyBaselineRiseIsPerSecond checks the same property on the
// inbound actuator: ConcurrencyConfig.MaxBaselineRise reaches the same
// controller through toConfig.
func TestConcurrencyBaselineRiseIsPerSecond(t *testing.T) {
	l := NewConcurrency(ConcurrencyConfig{
		MinLimit:        2,
		MaxLimit:        100,
		InitialLimit:    100,
		SLOTarget:       time.Second,
		Tick:            250 * time.Millisecond,
		MaxBaselineRise: 0.002,
	})
	for i := 0; i < 50; i++ {
		l.Observe(10 * time.Millisecond)
		l.ctrl.step()
	}
	before := l.ctrl.slow
	l.Observe(500 * time.Millisecond)
	l.ctrl.step()
	got := l.ctrl.slow - before
	want := before * 0.002 * 0.25
	if math.Abs(got-want) > want*1e-9 {
		t.Fatalf("baseline rose %.9f in one tick, want %.9f", got, want)
	}
}
