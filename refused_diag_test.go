package agilelimiter

// Refused connections and the synthetic outage sample.
//
// Unavailable() counts a call that never arrived, so a refused connection does
// not read as a fast, healthy response; a tick those calls dominate is an outage
// and records one synthetic "congested" sample. The sample is a made-up number,
// not a measurement, so how MANY of them a tick records carries no information —
// but it used to scale with the number of failed calls, which let the ratio-mode
// tail guard read "10% of calls were refused" as "10% of the traffic was slow".
// Above the default 5% tolerance, the guard fired every tick and pinned the rate
// to the floor. These tests pin both halves of the contract: refusal volume that
// does not dominate must not collapse the limiter, in either tail mode, and an
// outage must still back it off.

import (
	"fmt"
	"math"
	"testing"
	"time"
)

const (
	refCap    = 400.0
	refBaseMS = 24.0
	refSLO    = 100.0
	refTicks  = 250
)

func refCfg() Config {
	return Config{
		MinRate: 1, FloorRate: 5, ProbeRate: 5,
		MaxRate:       500,
		InitialRate:   50,
		SLOTarget:     refSLO * time.Millisecond,
		TailTolerance: 0.05,
		Tick:          time.Second,

		SlowHalfLife:    45 * time.Second,
		TargetInflation: 2.5,
	}
}

// driveRefused runs the limiter against a saturating plant where pct percent of
// calls are refused. Every k-th call is a refusal, which is deterministic and
// therefore reproducible (a random draw made the earlier diagnostic flaky).
func driveRefused(cfg Config, pct int) (settled, goodput, minRate float64) {
	return driveRefusedOn(cfg, pct, refCap)
}

// driveRefusedOn is driveRefused against a plant of the given capacity.
func driveRefusedOn(cfg Config, pct int, refCap float64) (settled, goodput, minRate float64) {
	l := New(cfg)
	var good, att float64
	minRate = math.Inf(1)
	every := 0
	if pct > 0 && pct <= 100 {
		every = 100 / pct
	}

	for t := 0; t < refTicks; t++ {
		r := l.Rate()
		util := math.Min(r/refCap, 0.95)
		trueMS := refBaseMS / (1 - util)

		att += r
		if trueMS <= refSLO {
			good += r
		}

		n := int(math.Max(1, r))
		for k := 0; k < n; k++ {
			if pct == 100 || (every > 0 && k%every == 0) {
				l.Unavailable()
			} else {
				l.Observe(time.Duration(trueMS * 1e6))
			}
		}
		l.ctrl.step()

		if l.Rate() < minRate {
			minRate = l.Rate()
		}
	}
	return l.Rate(), good / refTicks, minRate
}

// A refusal rate that does not dominate the traffic must not move the rate.
//
// Before the per-tick cap this test collapsed: 10% refused held 9 req/s against
// the clean 227, while 5% held 227 — a cliff between 5% and 10%.
func TestRefusedVolumeDoesNotCollapse(t *testing.T) {
	clean, cleanGood, _ := driveRefused(refCfg(), 0)

	if clean < 200 {
		t.Fatalf("clean run settled at %v, expected the normal ~227: the plant is wrong, not the fix", clean)
	}

	for _, pct := range []int{1, 2, 5, 10, 20, 50} {
		settled, good, _ := driveRefused(refCfg(), pct)
		if settled < 0.9*clean {
			t.Errorf("%d%% refused settled at %.0f, want >= %.0f (clean %.0f): "+
				"refusal count is being read as a slow-traffic ratio",
				pct, settled, 0.9*clean, clean)
		}
		if good < 0.9*cleanGood {
			t.Errorf("%d%% refused goodput %.0f, want >= %.0f (clean %.0f)",
				pct, good, 0.9*cleanGood, cleanGood)
		}
	}
}

// The same contract in quantile mode at low volume. With fewer than 100 samples a
// tick the nearest-rank p99 is the tick's maximum, so a synthetic sample — above
// SLOTarget by construction — is the p99 whenever one is recorded. Recording one
// for any tick that saw a refusal made a single refused call fire the tail guard.
func TestSporadicRefusalsDoNotFireQuantileGuard(t *testing.T) {
	cfg := refCfg()
	cfg.TailTolerance = 0 // quantile mode, the default
	const capacity = 100  // settles near 70 req/s: under 100 samples a tick
	clean, _, _ := driveRefusedOn(cfg, 0, capacity)
	if clean >= 100 {
		t.Fatalf("clean run settled at %.0f; the test needs fewer than 100 samples a tick", clean)
	}
	for _, pct := range []int{1, 2, 5, 10, 20} {
		if settled, _, _ := driveRefusedOn(cfg, pct, capacity); settled < 0.9*clean {
			t.Errorf("%d%% refused settled at %.0f, want >= %.0f (clean %.0f)", pct, settled, 0.9*clean, clean)
		}
	}
}

// When most of a tick's calls never arrive, that tick is an outage, whatever the
// volume: the limiter must back off toward ProbeRate even though some calls
// still get through.
func TestMajorityRefusedBacksOff(t *testing.T) {
	for _, tol := range []float64{0, 0.05} {
		cfg := refCfg()
		cfg.TailTolerance = tol
		cfg.InitialRate = 60
		l := New(cfg)
		for i := 0; i < 40; i++ {
			for k, n := 0, int(math.Max(1, l.Rate())); k < n; k++ {
				if k%5 == 0 {
					l.Observe(time.Duration(refBaseMS * 1e6))
				} else {
					l.Unavailable() // 80% refused
				}
			}
			l.ctrl.step()
		}
		if got := l.Rate(); got > float64(cfg.ProbeRate)+1 {
			t.Errorf("tol=%v: 80%% refused settled at %.0f, want ProbeRate=%d", tol, got, cfg.ProbeRate)
		}
	}
}

// The other half: a downstream that is really down must still back off, all the
// way to ProbeRate. Capping the sample count must not blunt the safety valve.
func TestRefusedTotalOutageStillBacksOff(t *testing.T) {
	cfg := refCfg()
	cfg.InitialRate = 300
	cfg.FloorRate = 20
	cfg.ProbeRate = 5

	l := New(cfg)
	var trace []float64
	for t := 0; t < 60; t++ {
		n := int(math.Max(1, l.Rate()))
		for k := 0; k < n; k++ {
			l.Unavailable() // every call refused
		}
		l.ctrl.step()
		trace = append(trace, l.Rate())
	}

	if trace[0] < 290 {
		t.Fatalf("did not start high: %.0f", trace[0])
	}
	if last := trace[len(trace)-1]; last > float64(cfg.ProbeRate)+1 {
		t.Fatalf("total outage settled at %.0f, want ProbeRate=%d", last, cfg.ProbeRate)
	}
	// It must actually descend, not jump to the floor in one step.
	if trace[3] >= trace[0] {
		t.Fatalf("rate did not fall within 4 ticks: %.0f -> %.0f", trace[0], trace[3])
	}
}

// The diagnostic that identified the cause: with the cap in place, the injected
// value must not be able to collapse the rate. Before the fix every arm pinned
// the rate near the floor whatever the magnitude; now none of them may.
//
// The arms do not converge to identical rates, and should not be expected to:
// changing SLOTarget also moves the absolute tail threshold, so it changes what
// counts as slow quite apart from the refusal handling.
func TestRefusedMagnitudeCannotCollapse(t *testing.T) {
	fmt.Println()
	for _, slo := range []time.Duration{50, 100, 300, 1000} {
		cfg := refCfg()
		cfg.SLOTarget = slo * time.Millisecond
		settled, good, _ := driveRefused(cfg, 10)
		fmt.Printf("  SLO=%-6v 10%% refused -> settled %.0f goodput %.0f\n", slo, settled, good)

		// The floor is 5 and the old bug pinned this to 9; anything near the
		// floor means the refusal count is still being read as a slow ratio.
		if settled < 100 {
			t.Errorf("SLO=%v: 10%% refused settled at %.0f, which is near the floor: "+
				"the injected magnitude is being read as a ratio", slo, settled)
		}
	}
}