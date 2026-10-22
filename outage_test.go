package agilelimiter

import (
	"context"
	"errors"
	"testing"
	"time"
)

var errRefused = errors.New("connection refused")
var errCallerGone = errors.New("context canceled")

func classifyTest(err error) Outcome {
	switch {
	case errors.Is(err, errRefused):
		return Unreachable
	case errors.Is(err, errCallerGone):
		return Aborted
	}
	return Reached
}

// A fast-failing downstream returns in microseconds. With the default policy
// that reads as a healthy round trip, so the controller keeps raising the rate
// into a dependency that is already down. This is the behaviour Config.Classify
// exists to fix, pinned here so the default cannot silently change.
func TestDefaultPolicyTreatsFastFailureAsHealthy(t *testing.T) {
	fc := &fakeClock{now: time.Unix(1_000_000, 0)}
	l := New(Config{
		MinRate: 10, MaxRate: 1000, InitialRate: 100,
		SLOTarget: 200 * time.Millisecond,
		Clock:     fc,
	})
	for i := 0; i < 20; i++ {
		if err := l.Do(func() error { return errRefused }); !errors.Is(err, errRefused) {
			t.Fatalf("Do returned %v, want errRefused", err)
		}
		l.ctrl.step()
	}
	if l.Rate() <= 100 {
		t.Fatalf("default policy should treat a ~0 RTT failure as healthy and raise the rate, got %v", l.Rate())
	}
}

func TestUnreachableBacksOffTowardMinRate(t *testing.T) {
	fc := &fakeClock{now: time.Unix(1_000_000, 0)}
	l := New(Config{
		MinRate: 10, MaxRate: 1000, InitialRate: 500,
		SLOTarget: 200 * time.Millisecond,
		Classify:  classifyTest,
		Clock:     fc,
	})
	for i := 0; i < 30; i++ {
		l.Do(func() error { return errRefused })
		l.ctrl.step()
	}
	if got := l.Rate(); got != 10 {
		t.Fatalf("an unreachable downstream must back off to MinRate, got %v", got)
	}
}

func TestAbortedTellsTheControllerNothing(t *testing.T) {
	fc := &fakeClock{now: time.Unix(1_000_000, 0)}
	l := New(Config{
		MinRate: 10, MaxRate: 1000, InitialRate: 300,
		SLOTarget: 200 * time.Millisecond,
		Classify:  classifyTest,
		Clock:     fc,
	})
	for i := 0; i < 20; i++ {
		l.Do(func() error { return errCallerGone })
		l.ctrl.step()
	}
	if got := l.Rate(); got != 300 {
		t.Fatalf("a caller-side abort is not a downstream signal; rate should hold at 300, got %v", got)
	}
}

func TestUnreachableIsNoOpWithoutAnAnchor(t *testing.T) {
	// No SLOTarget and no sample yet: there is no honest synthetic value, so
	// nothing may be recorded — inventing one would corrupt the baseline.
	l := New(Config{MinRate: 10, MaxRate: 100, InitialRate: 50})
	l.Unavailable()
	if _, _, n := l.hist.snapshot(); n != 0 {
		t.Fatalf("recorded %d samples with nothing to anchor against, want 0", n)
	}
}

func TestUnreachableSampleReadsAsCongestion(t *testing.T) {
	// With a baseline but no SLOTarget the synthetic sample must still land above
	// the tolerated window, so the proportional backoff fires.
	l := New(Config{MinRate: 10, MaxRate: 1000, InitialRate: 200})
	feed(l, 10*time.Millisecond, 100) // baseline = 10ms
	high := 2 * 10 * time.Millisecond // (1+TargetInflation) x baseline, default 1.0

	l.Unavailable()
	p50, _, n := l.hist.snapshot()
	if n != 1 {
		t.Fatalf("recorded %d samples, want 1", n)
	}
	if p50 <= high {
		t.Fatalf("synthetic sample %v must read above the tolerated window %v", p50, high)
	}
}

func TestOutageSamplePrefersSLOTarget(t *testing.T) {
	c := newController(withDefaults(Config{SLOTarget: 200 * time.Millisecond}), &histogram{}, func(int) {})
	s, ok := c.outageSample()
	if !ok {
		t.Fatal("expected a synthetic sample when SLOTarget is set")
	}
	if s != 300*time.Millisecond {
		t.Fatalf("outage sample = %v, want 1.5 x SLOTarget = 300ms", s)
	}
}

// The inbound side has the same failure mode: a handler that answers 503 without
// doing any work reports a ~0 handling time, which reads as "fast and healthy".
func TestConcurrencyUnreachableBacksOffTowardMinLimit(t *testing.T) {
	fc := &fakeClock{now: time.Unix(1_000_000, 0)}
	l := NewConcurrency(ConcurrencyConfig{
		MinLimit: 2, MaxLimit: 500, InitialLimit: 300,
		SLOTarget: 200 * time.Millisecond,
		Classify:  classifyTest,
		Clock:     fc,
	})
	for i := 0; i < 40; i++ {
		l.Do(func() error { return errRefused })
		l.ctrl.step()
	}
	if got := l.Limit(); got != 2 {
		t.Fatalf("an unreachable dependency must back the limit off to MinLimit, got %v", got)
	}
}

// A canceled call is the one failure the library classifies without help: the
// caller gave up, so the truncated duration is not a statement about the
// downstream. A timeout is the opposite and must still be observed.
func TestCanceledCallTellsTheControllerNothing(t *testing.T) {
	l := New(Config{MinRate: 10, MaxRate: 1000, InitialRate: 100})
	if err := l.Do(func() error { return context.Canceled }); !errors.Is(err, context.Canceled) {
		t.Fatalf("Do returned %v", err)
	}
	if _, _, n := l.hist.snapshot(); n != 0 {
		t.Fatalf("recorded %d samples for a canceled call, want 0", n)
	}
}

func TestDeadlineExceededIsStillObserved(t *testing.T) {
	l := New(Config{MinRate: 10, MaxRate: 1000, InitialRate: 100})
	l.Do(func() error { return context.DeadlineExceeded })
	if _, _, n := l.hist.snapshot(); n != 1 {
		t.Fatalf("recorded %d samples for a timeout, want 1: a timeout is a real signal", n)
	}
}

// The built-in default is overridable: someone who wants cancellations counted
// can say so with Classify.
func TestClassifyOverridesTheCanceledDefault(t *testing.T) {
	l := New(Config{
		MinRate: 10, MaxRate: 1000, InitialRate: 100,
		Classify: func(error) Outcome { return Reached },
	})
	l.Do(func() error { return context.Canceled })
	if _, _, n := l.hist.snapshot(); n != 1 {
		t.Fatalf("recorded %d samples, want 1: Classify must win over the default", n)
	}
}
