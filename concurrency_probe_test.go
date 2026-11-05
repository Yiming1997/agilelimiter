package agilelimiter

// ConcurrencyLimiter must leave probe mode when a request completes normally.
//
// Unavailable() sets probe=true and lowers the clamp to ProbeLimit so the limiter
// can keep probing a dependency that is down without hammering it. The first
// healthy sample is the evidence that it is back, and the clamp must return to
// FloorLimit.
//
// Limiter (outbound) did this in both Observe and observeOutcome. ConcurrencyLimiter
// did it in neither, so a single Unavailable pinned the floor to ProbeLimit for
// the life of the process: 5000 healthy Observe calls and 500 successful Do
// calls later, ctrl.floor() still returned ProbeLimit.

import (
	"context"
	"errors"
	"testing"
	"time"
)

func probeTestCfg() ConcurrencyConfig {
	return ConcurrencyConfig{
		MinLimit: 1, FloorLimit: 100, ProbeLimit: 10,
		MaxLimit:     400,
		InitialLimit: 200,
		SLOTarget:    100 * time.Millisecond,
		Tick:         time.Second,
		SlowHalfLife: 45 * time.Second,
	}
}

// floorOf reports the clamp the controller is currently using.
func floorOf(l *ConcurrencyLimiter) int { return l.ctrl.floor() }

func probeOf(l *ConcurrencyLimiter) bool { return l.ctrl.probe }

// A healthy sample via Observe must return the clamp to FloorLimit.
func TestConcurrencyObserveClearsProbe(t *testing.T) {
	l := NewConcurrency(probeTestCfg())
	defer l.Stop()

	l.Unavailable()
	if !probeOf(l) {
		t.Fatal("Unavailable must enter probe mode")
	}
	if got := floorOf(l); got != 10 {
		t.Fatalf("floor in probe mode = %d, want ProbeLimit 10", got)
	}

	l.Observe(10 * time.Millisecond)

	if probeOf(l) {
		t.Fatal("a normal Observe must clear probe mode")
	}
	if got := floorOf(l); got != 100 {
		t.Fatalf("floor after a healthy sample = %d, want FloorLimit 100", got)
	}
}

// The same via Do, the recommended call path.
func TestConcurrencyDoClearsProbe(t *testing.T) {
	l := NewConcurrency(probeTestCfg())
	defer l.Stop()

	l.Unavailable()
	if !probeOf(l) {
		t.Fatal("Unavailable must enter probe mode")
	}

	if err := l.Do(func() error { return nil }); err != nil {
		t.Fatalf("Do: %v", err)
	}

	if probeOf(l) {
		t.Fatal("a successful Do must clear probe mode")
	}
	if got := floorOf(l); got != 100 {
		t.Fatalf("floor after a successful Do = %d, want FloorLimit 100", got)
	}
}

// An aborted request is not evidence of recovery and must NOT clear probe mode:
// a canceled handler says nothing about whether the service can serve.
func TestConcurrencyAbortedDoesNotClearProbe(t *testing.T) {
	l := NewConcurrency(probeTestCfg())
	defer l.Stop()

	l.Unavailable()
	if !probeOf(l) {
		t.Fatal("Unavailable must enter probe mode")
	}

	// context.Canceled is classified Aborted by defaultOutcome.
	if err := l.Do(func() error { return context.Canceled }); !errors.Is(err, context.Canceled) {
		t.Fatalf("Do returned %v", err)
	}

	if !probeOf(l) {
		t.Fatal("an aborted request must not clear probe mode: it is not evidence of recovery")
	}
	if got := floorOf(l); got != 10 {
		t.Fatalf("floor after an abort = %d, want to stay at ProbeLimit 10", got)
	}
}

// End to end: a severe degradation drives the limit down, and recovery must be
// immediate once the service answers normally again rather than creeping back at
// UpStep.
//
// Measured before the fix, with FloorLimit=120 and ProbeLimit=5: after twelve
// outage ticks the limit sat at 5, and thirty healthy ticks later it had climbed
// only to 23 (5 -> 120 was never reached; the clamp stayed at 5). With the fix
// the first healthy tick restores the clamp to 120 and the limit reaches it in
// one step.
func TestConcurrencyRecoversToFloor(t *testing.T) {
	l := NewConcurrency(probeTestCfg())
	defer l.Stop()

	// Drive into probe mode. Outage samples pull the limit toward ProbeLimit,
	// which is the only state in which the floor clamp actually binds.
	for i := 0; i < 40; i++ {
		l.Unavailable()
		l.ctrl.step()
	}
	if !probeOf(l) {
		t.Fatal("sustained outage must keep probe mode on")
	}
	if got := l.Limit(); got >= 100 {
		t.Fatalf("limit after a sustained outage = %d; the trace needs it pushed "+
			"down to ProbeLimit for this test to mean anything", got)
	}

	// One healthy completion.
	l.Observe(5 * time.Millisecond)
	if probeOf(l) {
		t.Fatal("recovery must clear probe mode")
	}

	// With the clamp at FloorLimit=100, a single step returns the limit to it.
	l.ctrl.step()
	if got := l.Limit(); got < 100 {
		t.Fatalf("limit after recovery + one step = %d, want >= FloorLimit 100: "+
			"the clamp did not return, so recovery is creeping at UpStep", got)
	}
}
