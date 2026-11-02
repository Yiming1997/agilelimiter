package agilelimiter

import (
	"testing"
	"time"
)

func newSimpleLimiter(cfg Config) *Limiter {
	cfg.Simple = true
	if cfg.Tick <= 0 {
		cfg.Tick = time.Second
	}
	if cfg.BackoffTicks <= 0 {
		cfg.BackoffTicks = 2
	}
	if cfg.RecoverTicks <= 0 {
		cfg.RecoverTicks = 2
	}
	return New(cfg)
}

func TestSimpleControllerClimbsAndBacksOff(t *testing.T) {
	l := newSimpleLimiter(Config{
		MinRate:     10,
		MaxRate:     1000,
		InitialRate: 100,
	})
	for i := 0; i < 20; i++ {
		feed(l, 10*time.Millisecond, 100)
	}
	healthy := l.Rate()
	if healthy <= 100 {
		t.Fatalf("simple controller did not climb: %v", healthy)
	}

	for i := 0; i < 20; i++ {
		feed(l, 100*time.Millisecond, 100)
	}
	if got := l.Rate(); got >= healthy {
		t.Fatalf("simple controller did not back off: healthy=%v congested=%v", healthy, got)
	}
	if got := l.Rate(); got < 10 {
		t.Fatalf("rate fell below MinRate: %v", got)
	}
}

func TestSimpleControllerRecovers(t *testing.T) {
	l := newSimpleLimiter(Config{
		MinRate:     10,
		MaxRate:     1000,
		InitialRate: 200,
	})
	for i := 0; i < 15; i++ {
		feed(l, 10*time.Millisecond, 100)
	}
	for i := 0; i < 15; i++ {
		feed(l, 100*time.Millisecond, 100)
	}
	low := l.Rate()

	for i := 0; i < 40; i++ {
		feed(l, 10*time.Millisecond, 100)
	}
	if l.Rate() <= low {
		t.Fatalf("simple controller did not recover: low=%v now=%v", low, l.Rate())
	}
}

func TestSimpleControllerTailGuard(t *testing.T) {
	l := newSimpleLimiter(Config{
		MinRate:     1,
		MaxRate:     1000,
		InitialRate: 100,
		SLOTarget:   50 * time.Millisecond,
	})
	for i := 0; i < 3; i++ {
		feedMixed(l)
	}
	if l.Rate() >= 100 {
		t.Fatalf("simple SLO tail guard did not back off, rate=%v", l.Rate())
	}
}

func TestSimpleControllerStaysWithinBounds(t *testing.T) {
	l := newSimpleLimiter(Config{
		MinRate:     20,
		MaxRate:     80,
		InitialRate: 40,
	})
	seq := []time.Duration{10 * time.Millisecond, 500 * time.Millisecond, 5 * time.Millisecond}
	for i := 0; i < 200; i++ {
		feed(l, seq[i%len(seq)], 50)
		if r := l.Rate(); r < 20 || r > 80 {
			t.Fatalf("rate %v out of [20,80]", r)
		}
	}
}

func TestSimpleControllerColdStartEndsOnBackoff(t *testing.T) {
	l := newSimpleLimiter(Config{
		MinRate:       10,
		MaxRate:       1000,
		InitialRate:   100,
		SlowStartGain: 2,
	})
	feed(l, 10*time.Millisecond, 100) // seed
	if !l.Stats().ColdStart {
		t.Fatal("fresh simple limiter must report ColdStart")
	}
	feed(l, 10*time.Millisecond, 100)
	if !l.Stats().ColdStart {
		t.Fatal("healthy probe must keep ColdStart")
	}
	feed(l, 500*time.Millisecond, 100)
	feed(l, 500*time.Millisecond, 100)
	if l.Stats().ColdStart {
		t.Fatal("backoff must end ColdStart")
	}
}

func TestSimpleModeReachesConcurrencyLimiter(t *testing.T) {
	l := NewConcurrency(ConcurrencyConfig{
		Simple:       true,
		MinLimit:     2,
		MaxLimit:     100,
		InitialLimit: 10,
	})
	defer l.Stop()
	if !l.Config().Simple {
		t.Fatal("ConcurrencyConfig.Simple was not propagated")
	}
}
