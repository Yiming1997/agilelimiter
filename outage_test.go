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
	l.ctrl.step()
	if s := l.Stats(); s.Samples != 0 || s.Rate != 50 {
		t.Fatalf("recorded %d samples and moved the rate to %v with nothing to anchor against, want 0 and 50",
			s.Samples, s.Rate)
	}
}

func TestUnreachableSampleReadsAsCongestion(t *testing.T) {
	// With a baseline but no SLOTarget the synthetic sample of an outage tick
	// must still land above the tolerated window, and the tick must back off.
	l := New(Config{MinRate: 10, MaxRate: 1000, InitialRate: 200})
	feed(l, 10*time.Millisecond, 100) // baseline = 10ms
	high := 2 * 10 * time.Millisecond // (1+TargetInflation) x baseline, default 1.0
	before := l.Rate()

	l.Unavailable()
	l.ctrl.step()
	s := l.Stats()
	if s.Samples != 1 {
		t.Fatalf("an outage tick recorded %d samples, want 1", s.Samples)
	}
	if s.P50 <= high {
		t.Fatalf("synthetic sample %v must read above the tolerated window %v", s.P50, high)
	}
	if s.Rate >= before {
		t.Fatalf("an outage tick must back off: rate %v, was %v", s.Rate, before)
	}
}

// A refusal among answered calls is not an outage: it must not reach the
// histogram, so it cannot become a tick's p99.
func TestMinorityUnreachableRecordsNothing(t *testing.T) {
	l := New(Config{MinRate: 10, MaxRate: 1000, InitialRate: 200, SLOTarget: 100 * time.Millisecond})
	feed(l, 10*time.Millisecond, 50)
	for i := 0; i < 49; i++ {
		l.Observe(10 * time.Millisecond)
	}
	l.Unavailable()
	l.ctrl.step()
	if s := l.Stats(); s.Samples != 49 || s.P99 > 20*time.Millisecond {
		t.Fatalf("one refusal among 49 answers: %d samples, p99 %v; want 49 real samples and a real p99",
			s.Samples, s.P99)
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

// A truncated measurement is a lower bound on the real duration. Past the SLO
// that bound is evidence of slowness and is kept; under it, or with no SLO to
// compare against, it says nothing and is dropped. Both limiters, the same rule.
func TestObserveTruncatedKeepsOnlyWhatIsPastTheSLO(t *testing.T) {
	const slo = 100 * time.Millisecond
	for _, c := range []struct {
		name string
		slo  time.Duration
		d    time.Duration
		want uint64
	}{
		{"past the SLO", slo, 150 * time.Millisecond, 1},
		{"under the SLO", slo, 50 * time.Millisecond, 0},
		{"at the SLO", slo, slo, 0},
		{"no SLO", 0, time.Second, 0},
	} {
		l := New(Config{MaxRate: 1000, InitialRate: 100, SLOTarget: c.slo})
		l.ObserveTruncated(c.d)
		cl := NewConcurrency(ConcurrencyConfig{MaxLimit: 100, InitialLimit: 10, SLOTarget: c.slo})
		cl.ObserveTruncated(c.d)
		for _, n := range []uint64{l.hist.total.Load(), cl.hist.total.Load()} {
			if n != c.want {
				t.Errorf("%s: %d samples recorded, want %d", c.name, n, c.want)
			}
		}
	}
}

// Do applies it to a canceled call, under the default policy and under a
// Classify that calls cancellation Aborted — the README's own example — alike:
// the rule is about the measurement, not the policy.
func TestSlowCanceledCallIsEvidence(t *testing.T) {
	for _, classify := range []func(error) Outcome{nil, classifyTest, func(error) Outcome { return Aborted }} {
		fc := &fakeClock{now: time.Unix(1_000_000, 0)}
		l := New(Config{MaxRate: 1000, InitialRate: 100, SLOTarget: 200 * time.Millisecond,
			Classify: classify, Clock: fc})
		l.Do(func() error {
			fc.now = fc.now.Add(300 * time.Millisecond) // the caller gave up after 300ms
			return context.Canceled
		})
		p50, _, n := l.hist.snapshot()
		if n != 1 || p50 < 200*time.Millisecond {
			t.Fatalf("a call canceled after 300ms against a 200ms SLO: %d samples, p50 %v; want 1 at >= 200ms", n, p50)
		}
	}
}

// A call nobody waited for does not show that the downstream answers again, so
// keeping one must not end probe mode.
func TestObserveTruncatedKeepsProbeMode(t *testing.T) {
	l := New(Config{FloorRate: 50, ProbeRate: 5, MaxRate: 1000, InitialRate: 100, SLOTarget: 100 * time.Millisecond})
	l.Unavailable()
	l.ObserveTruncated(time.Second)
	if !l.ctrl.probe {
		t.Fatal("a truncated sample ended probe mode")
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
