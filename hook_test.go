package agilelimiter

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestHooksAdmitRunsBeforeDo(t *testing.T) {
	fc := &fakeClock{now: time.Unix(1_000_000, 0)}
	var order []string
	l := New(Config{MaxRate: 100, InitialRate: 100, Clock: fc})
	l.SetHooks(&Hooks{Admit: []AdmitHook{func() error {
		order = append(order, "hook")
		return nil
	}}})

	if err := l.Do(func() error {
		order = append(order, "fn")
		return nil
	}); err != nil {
		t.Fatalf("Do returned %v", err)
	}
	if len(order) != 2 || order[0] != "hook" || order[1] != "fn" {
		t.Fatalf("call order = %v, want [hook fn]", order)
	}
	if got := l.hist.total.Load(); got != 1 {
		t.Fatalf("observations = %d, want 1", got)
	}
}

func TestHooksAdmitRejectShortCircuits(t *testing.T) {
	fc := &fakeClock{now: time.Unix(1_000_000, 0)}
	sentinel := errors.New("no token")
	second := false
	fnCalled := false
	l := New(Config{MaxRate: 100, InitialRate: 100, Clock: fc})
	l.SetHooks(&Hooks{Admit: []AdmitHook{
		func() error { return sentinel },
		func() error { second = true; return nil },
	}})

	err := l.Do(func() error {
		fnCalled = true
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Do error = %v, want %v", err, sentinel)
	}
	if second {
		t.Fatal("Admit hooks kept running after an error")
	}
	if fnCalled {
		t.Fatal("fn ran on a rejected call")
	}
	if got := l.bucket.grants.Load(); got != 0 {
		t.Fatalf("grants = %d, want 0: a rejection must not take a slot", got)
	}
	if got := l.hist.total.Load(); got != 0 {
		t.Fatalf("observations = %d, want 0: a rejection must not be observed", got)
	}
}

func TestHooksObserveRunsAfterCall(t *testing.T) {
	fc := &fakeClock{now: time.Unix(1_000_000, 0)}
	var order []string
	l := New(Config{MaxRate: 100, InitialRate: 100, Clock: fc})
	l.SetHooks(&Hooks{Observe: []ObserveHook{func(Observation) {
		order = append(order, "hook")
	}}})

	if err := l.Do(func() error {
		order = append(order, "fn")
		return nil
	}); err != nil {
		t.Fatalf("Do returned %v", err)
	}
	if len(order) != 2 || order[0] != "fn" || order[1] != "hook" {
		t.Fatalf("call order = %v, want [fn hook]", order)
	}
	if got := l.hist.total.Load(); got != 1 {
		t.Fatalf("observations = %d, want 1", got)
	}
}

func TestHooksObserveSeesObservation(t *testing.T) {
	fc := &fakeClock{now: time.Unix(1_000_000, 0)}
	var got Observation
	l := New(Config{MaxRate: 100, InitialRate: 100, Clock: fc})
	if err := l.SetHooks(&Hooks{Observe: []ObserveHook{func(o Observation) {
		got = o
	}}}); err != nil {
		t.Fatalf("SetHooks: %v", err)
	}

	if err := l.Do(func() error {
		fc.now = fc.now.Add(7 * time.Millisecond) // simulate the round trip
		return nil
	}); err != nil {
		t.Fatalf("Do returned %v", err)
	}
	if got.Duration != 7*time.Millisecond {
		t.Fatalf("Duration = %v, want 7ms", got.Duration)
	}
	if got.Err != nil {
		t.Fatalf("Err = %v, want nil", got.Err)
	}
	if got.Outcome != Reached {
		t.Fatalf("Outcome = %v, want Reached", got.Outcome)
	}
}

func TestHooksObserveSeesClassification(t *testing.T) {
	fc := &fakeClock{now: time.Unix(1_000_000, 0)}
	sentinel := errors.New("down")
	var got Observation
	l := New(Config{
		MaxRate: 100, InitialRate: 100, Clock: fc,
		Classify: func(error) Outcome { return Unreachable },
	})
	if err := l.SetHooks(&Hooks{Observe: []ObserveHook{func(o Observation) {
		got = o
	}}}); err != nil {
		t.Fatalf("SetHooks: %v", err)
	}

	_ = l.Do(func() error { return sentinel })
	if !errors.Is(got.Err, sentinel) {
		t.Fatalf("Err = %v, want %v", got.Err, sentinel)
	}
	if got.Outcome != Unreachable {
		t.Fatalf("Outcome = %v, want Unreachable", got.Outcome)
	}
}

func TestHooksObserveRunsAfterController(t *testing.T) {
	fc := &fakeClock{now: time.Unix(1_000_000, 0)}
	var recordedAtHook uint64
	l := New(Config{MaxRate: 100, InitialRate: 100, Clock: fc})
	if err := l.SetHooks(&Hooks{Observe: []ObserveHook{func(Observation) {
		recordedAtHook = l.hist.total.Load()
	}}}); err != nil {
		t.Fatalf("SetHooks: %v", err)
	}

	if err := l.Do(func() error { return nil }); err != nil {
		t.Fatalf("Do returned %v", err)
	}
	if recordedAtHook != 1 {
		t.Fatalf("samples at hook time = %d, want 1: Observe hooks must run after the controller was told",
			recordedAtHook)
	}
}

func TestSetHooksRejectsNil(t *testing.T) {
	l := New(Config{MaxRate: 100, InitialRate: 100})
	if err := l.SetHooks(nil); err == nil {
		t.Fatal("SetHooks(nil) = nil, want an error")
	}
	if l.hooks != nil {
		t.Fatal("SetHooks(nil) stored hooks")
	}
}

func TestSetHooksOnlyOnce(t *testing.T) {
	fc := &fakeClock{now: time.Unix(1_000_000, 0)}
	first := &Hooks{}
	l := New(Config{MaxRate: 100, InitialRate: 100, Clock: fc})
	if err := l.SetHooks(first); err != nil {
		t.Fatalf("first SetHooks: %v", err)
	}
	if err := l.SetHooks(&Hooks{}); err == nil {
		t.Fatal("second SetHooks = nil, want an error")
	}
	if l.hooks != first {
		t.Fatal("second SetHooks replaced the hooks")
	}

	if err := l.Do(func() error { return nil }); err != nil {
		t.Fatalf("Do returned %v", err)
	}
}

func TestTakeBypassesHooks(t *testing.T) {
	fc := &fakeClock{now: time.Unix(1_000_000, 0)}
	called := false
	l := New(Config{MaxRate: 100, InitialRate: 100, Clock: fc})
	l.SetHooks(&Hooks{Admit: []AdmitHook{func() error { called = true; return nil }}})

	l.Take()
	if called {
		t.Fatal("Take ran the hooks; hooks belong to Do")
	}
}

// Set-once means Do only ever reads the pointer, so concurrent Do calls are
// race-free without an atomic. Run this under -race.
func TestHooksConcurrentDo(t *testing.T) {
	l := New(Config{MaxRate: 1_000_000, InitialRate: 1_000_000})
	if err := l.SetHooks(&Hooks{Admit: []AdmitHook{func() error { return nil }}}); err != nil {
		t.Fatalf("SetHooks: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = l.Do(func() error { return nil })
			}
		}()
	}
	wg.Wait()
}

func TestConcurrencyHooksAdmitReject(t *testing.T) {
	fc := &fakeClock{now: time.Unix(1_000_000, 0)}
	sentinel := errors.New("denied")
	fnCalled := false
	l := NewConcurrency(ConcurrencyConfig{MaxLimit: 10, InitialLimit: 10, Clock: fc})
	if err := l.SetHooks(&Hooks{Admit: []AdmitHook{func() error { return sentinel }}}); err != nil {
		t.Fatalf("SetHooks: %v", err)
	}

	if err := l.Do(func() error { fnCalled = true; return nil }); !errors.Is(err, sentinel) {
		t.Fatalf("Do error = %v, want %v", err, sentinel)
	}
	if fnCalled {
		t.Fatal("fn ran on a rejected request")
	}
	if got := l.InFlight(); got != 0 {
		t.Fatalf("in-flight = %d, want 0: a rejection must not take a slot", got)
	}
	if got := l.hist.total.Load(); got != 0 {
		t.Fatalf("observations = %d, want 0: a rejection must not be observed", got)
	}
}

func TestConcurrencyHooksObserveSeesRequest(t *testing.T) {
	fc := &fakeClock{now: time.Unix(1_000_000, 0)}
	var got Observation
	l := NewConcurrency(ConcurrencyConfig{MaxLimit: 10, InitialLimit: 10, Clock: fc})
	if err := l.SetHooks(&Hooks{Observe: []ObserveHook{func(o Observation) { got = o }}}); err != nil {
		t.Fatalf("SetHooks: %v", err)
	}

	if err := l.Do(func() error {
		fc.now = fc.now.Add(3 * time.Millisecond)
		return nil
	}); err != nil {
		t.Fatalf("Do returned %v", err)
	}
	if got.Duration != 3*time.Millisecond {
		t.Fatalf("Duration = %v, want 3ms", got.Duration)
	}
	if got.Err != nil || got.Outcome != Reached {
		t.Fatalf("Observation = %+v, want nil error and Reached", got)
	}
	if got := l.hist.total.Load(); got != 1 {
		t.Fatalf("observations = %d, want 1", got)
	}
}

func TestConcurrencyTryTakeBypassesHooks(t *testing.T) {
	l := NewConcurrency(ConcurrencyConfig{MaxLimit: 10, InitialLimit: 10})
	called := false
	if err := l.SetHooks(&Hooks{Admit: []AdmitHook{func() error { called = true; return nil }}}); err != nil {
		t.Fatalf("SetHooks: %v", err)
	}

	if !l.TryTake() {
		t.Fatal("TryTake refused an empty limiter")
	}
	defer l.Release()
	if called {
		t.Fatal("TryTake ran the hooks; hooks belong to Do")
	}
}

func TestConcurrencySetHooksOnlyOnce(t *testing.T) {
	first := &Hooks{}
	l := NewConcurrency(ConcurrencyConfig{MaxLimit: 10, InitialLimit: 10})
	if err := l.SetHooks(nil); err == nil {
		t.Fatal("SetHooks(nil) = nil, want an error")
	}
	if err := l.SetHooks(first); err != nil {
		t.Fatalf("first SetHooks: %v", err)
	}
	if err := l.SetHooks(&Hooks{}); err == nil {
		t.Fatal("second SetHooks = nil, want an error")
	}
	if l.hooks != first {
		t.Fatal("second SetHooks replaced the hooks")
	}
}
