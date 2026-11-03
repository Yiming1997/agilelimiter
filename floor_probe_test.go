package agilelimiter

import (
	"strings"
	"testing"
	"time"
)

func TestFloorRateIsTheNormalFloor(t *testing.T) {
	l := New(Config{
		MinRate:     1,
		FloorRate:   100,
		ProbeRate:   5,
		MaxRate:     400,
		InitialRate: 100,
		Tick:        time.Second,
	})
	for i := 0; i < 20; i++ {
		feed(l, 10*time.Millisecond, 100)
	}
	if got := l.Rate(); got < 100 {
		t.Fatalf("healthy rate %v fell below FloorRate 100", got)
	}

	for i := 0; i < 20; i++ {
		feed(l, 500*time.Millisecond, 100)
	}
	if got := l.Rate(); got < 100 {
		t.Fatalf("congested rate %v fell below FloorRate 100 (ProbeRate is 5)", got)
	}
}

func TestProbeRateDuringOutageAndRecovery(t *testing.T) {
	l := New(Config{
		MinRate:     1,
		FloorRate:   100,
		ProbeRate:   5,
		MaxRate:     400,
		InitialRate: 100,
		Tick:        time.Second,
	})
	feed(l, 10*time.Millisecond, 100) // seed the baseline

	for i := 0; i < 20; i++ {
		l.Unavailable()
		l.ctrl.step()
	}
	if got := l.Rate(); got > 20 {
		t.Fatalf("outage rate %v did not approach ProbeRate 5", got)
	}
	if got := l.Rate(); got < 5 {
		t.Fatalf("outage rate %v fell below ProbeRate 5", got)
	}

	// A normal observed call clears probe mode; the next tick is clamped back
	// to the normal floor.
	feed(l, 10*time.Millisecond, 100)
	if got := l.Rate(); got < 100 {
		t.Fatalf("normal rate %v did not return to FloorRate 100", got)
	}
}

func TestFloorProbeDefaultsFromMinRate(t *testing.T) {
	l := New(Config{MinRate: 42, MaxRate: 100}).Config()
	if l.FloorRate != 42 || l.ProbeRate != 42 {
		t.Fatalf("FloorRate/ProbeRate = %d/%d, want 42/42", l.FloorRate, l.ProbeRate)
	}
}

func TestConcurrencyFloorProbeMapping(t *testing.T) {
	l := NewConcurrency(ConcurrencyConfig{
		MinLimit:   1,
		FloorLimit: 50,
		ProbeLimit: 5,
		MaxLimit:   100,
	})
	defer l.Stop()
	r := l.Config()
	if r.FloorRate != 50 || r.ProbeRate != 5 {
		t.Fatalf("concurrency Floor/Probe = %d/%d, want 50/5", r.FloorRate, r.ProbeRate)
	}
}

func TestFloorProbeValidation(t *testing.T) {
	badFloor := Config{MaxRate: 100, FloorRate: 200}
	if err := badFloor.Validate(); err == nil || !strings.Contains(err.Error(), "FloorRate") {
		t.Fatalf("FloorRate above MaxRate: %v", err)
	}
	badProbe := Config{MaxRate: 100, FloorRate: 50, ProbeRate: 80}
	if err := badProbe.Validate(); err == nil || !strings.Contains(err.Error(), "ProbeRate") {
		t.Fatalf("ProbeRate above FloorRate: %v", err)
	}
}
