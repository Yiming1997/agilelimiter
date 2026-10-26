// A/B: the same simulated downstream, the same SLO, four rate controllers.
//
// The question this answers: does a latency-driven limiter throttle when the
// downstream is healthy but slightly slower? Scenario A is exactly that — the
// idle latency goes 20ms -> 70ms (still under the 100ms SLO, with the queueing
// curve flat at these utilizations) while capacity stays at 150 req/s. A
// relative controller compares 70ms against its own baseline (2x of ~25ms) and
// calls it congestion. An SLO-anchored one compares 70ms against 0.8 x 100ms
// and sees nothing wrong.
//
// The bump is permanent after t=5s rather than transient, because a transient
// one is the easy case: it is the sustained-but-benign slowdown that a relative
// controller cannot recover from, since its baseline may only crawl up
// MaxBaselineRise per tick and the ramp back is UpStep per tick.
//
// The downstream uses sharpKnee, not the M/M/1 curve the other cases use. That
// matters: under 1/(1-util) the controller's own queueing is already 5x the idle
// latency at 80% utilization, so 2x its baseline is always past the SLO cap and
// the two anchors coincide. The knob only matters when the observed latency is
// mostly the dependency's own service time — which is the case being asked
// about, and which a sharp knee models.
//
// Run it with:
//
//	cd compare && go test -run TestSLOAnchorVsUber -v
package main

import (
	"fmt"
	"testing"
	"time"

	"agilelimiter"
)

const (
	// phase is how long each run lasts: 5s healthy, then wherever the schedule
	// takes it. Long enough for cold start to wash out.
	phase = 20 * time.Second
	// capacity of the simulated downstream.
	capacity = 150.0
	// agileCeiling is the rate an operator would actually promise a downstream
	// with ~150 req/s of capacity. Starting at 40 and doubling, it reaches the
	// ceiling without ever overshooting capacity — no cold-start excursion to
	// pollute the baseline, so the two anchors differ for one reason only.
	agileCeiling = 120
)

// sharpKnee is 1/(1-u^8): latency is flat until the downstream is nearly
// saturated, then it explodes. That is what a thread-pool or connection-pool
// bound dependency looks like, and it is what keeps the baseline an honest
// measure of the dependency's own service time rather than of our queueing.
func sharpKnee(util float64) float64 {
	if util < 0 {
		util = 0
	}
	if util > 1 {
		util = 1
	}
	u2 := util * util
	u8 := u2 * u2 * u2 * u2
	if u8 > 0.98 {
		u8 = 0.98
	}
	return 1 / (1 - u8)
}

// baseBump is the benign bump: the downstream answers 70ms instead of 20ms from
// t=5s on, and nobody offers it any more work.
func baseBump(t time.Duration) time.Duration {
	if t < 5*time.Second {
		return 20 * time.Millisecond
	}
	return 70 * time.Millisecond
}

// capacityDrop is a real, load-driven degradation: 150 -> 40 req/s at t=5s.
func capacityDrop(t time.Duration) float64 {
	if t < 5*time.Second {
		return capacity
	}
	return 40
}

// flat is a constant capacity.
func flat(v float64) func(time.Duration) float64 {
	return func(time.Duration) float64 { return v }
}

// agileAnchor builds an agilelimiter row. inflation is ignored under AnchorSLO;
// ticks of 0 take the default for the chosen anchor.
func agileAnchor(anchor agilelimiter.Anchor, inflation float64, ticks int) func() (limiter, func()) {
	return func() (limiter, func()) {
		l := agilelimiter.New(agilelimiter.Config{
			MinRate:         5,
			MaxRate:         agileCeiling,
			InitialRate:     40,
			SLOTarget:       slo,
			FastHalfLife:    750 * time.Millisecond,
			SlowHalfLife:    45 * time.Second,
			TargetInflation: inflation,
			Tick:            250 * time.Millisecond,
			Anchor:          anchor,
			BackoffTicks:    ticks,
			RecoverTicks:    ticks,
		})
		l.Start()
		return &agileLim{l: l}, l.Stop
	}
}

func TestSLOAnchorVsUber(t *testing.T) {
	rows := func(ds downstream, cap func(time.Duration) float64) []result {
		return []result{
			runOn("uber(100)", newUber(100), ds, cap, phase, 50),
			runOn("agile-2x", agileAnchor(agilelimiter.AnchorBaseline, 1.0, 0), ds, cap, phase, 60),
			runOn("agile-3.5x", agileAnchor(agilelimiter.AnchorBaseline, 2.5, 0), ds, cap, phase, 60),
			runOn("agile-slo", agileAnchor(agilelimiter.AnchorSLO, 0, 2), ds, cap, phase, 60),
		}
	}

	fmt.Println("agilelimiter anchor modes vs go.uber.org/ratelimit")
	fmt.Printf("simulated downstream: latency = base(t) * 1/(1-util^8), SLO %s, capacity %.0f/s\n", sloString, capacity)
	fmt.Printf("each run: %s (5s healthy, then the schedule below for the rest)\n", phase)
	fmt.Printf("agile arms: MinRate 5, MaxRate %d — a promise below the real capacity, so\n", agileCeiling)
	fmt.Println("cold start never overshoots. uber(100) is the best fixed rate you could")
	fmt.Println("pick if you already knew the healthy-phase capacity.")

	report(
		"A — BENIGN bump at t=5s: idle latency 20ms -> 70ms (under the SLO), capacity 150/s",
		rows(downstream{base: baseBump, wait: sharpKnee}, flat(capacity)))

	report(
		"B — REAL degradation at t=5s: capacity 150 -> 40/s, idle latency 20ms",
		rows(downstream{base: steadyBase(20 * time.Millisecond), wait: sharpKnee}, capacityDrop))

	fmt.Println("\ngoodput/s = requests completing within the SLO each second;")
	fmt.Println("finalRate = the controller's rate at the end of the run, once settled.")
	fmt.Println()
	fmt.Println("A: the relative law at its default 2x window backs off on a latency that")
	fmt.Println("   never breaks the SLO, and a permanent bump costs it the rest of the")
	fmt.Println("   run. The SLO anchor reads the same 70ms as healthy and keeps its rate.")
	fmt.Println("B: when capacity really drops, the SLO anchor backs off like everyone")
	fmt.Println("   else — an absolute threshold does not mean 'never throttle'. The fixed")
	fmt.Println("   rate has no defence at all: it keeps sending 100/s into 40/s.")
}
