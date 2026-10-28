// Robustness experiments: does the limiter survive a sample stream that is only
// partly about the resource it is protecting, and does it come back afterwards?
//
// Every case here runs against a downstream whose latency does NOT depend on the
// rate (or, for the one congestion case, does — and says so). That makes the
// question answerable: with nothing to detect, a rate reduction is a false
// positive by construction, so "did the rate move" is a clean measurement.
//
// The cases are classified by what the *policy* says should happen, not by what
// is convenient for the implementation:
//
//	hold      the contamination is fractional and at or below the tolerance:
//	          the rate must not move
//	throttle  the stream is genuinely over the policy line (or over the SLO for
//	          every request): the rate must move
//	recover   the stream goes back to clean: the rate must come back
//
// Run:
//
//	cd compare && go test -run 'TestRobust' -v -timeout 40m
package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agilelimiter"
)

// probeCfg is one limiter under one policy, driven at a fixed ceiling so that
// "did the rate move" needs no interpretation.
type probeCfg struct {
	rate     int
	tol      float64 // TailTolerance: 0 = the historical quantile guard
	anchor   agilelimiter.Anchor
	preset   agilelimiter.Preset
	slo      time.Duration
	capacity float64 // 0 = flat downstream (no queueing); >0 = mm1-shaped

	workers int
}

// probeOut is what one run measured. rateAt is sampled every 100ms.
type probeOut struct {
	rateAt     []float64
	overAt     []float64
	threshAt   []float64
	samplesAt  []float64
	completed  int
	meanLatency time.Duration
}

// minAfter is the lowest target rate once the cold start is behind us.
func (o probeOut) minAfter(after time.Duration) float64 {
	from := int(after / (100 * time.Millisecond))
	if from >= len(o.rateAt) {
		return 0
	}
	m := o.rateAt[from]
	for _, r := range o.rateAt[from:] {
		if r < m {
			m = r
		}
	}
	return m
}

func (o probeOut) last() float64 {
	if len(o.rateAt) == 0 {
		return 0
	}
	return o.rateAt[len(o.rateAt)-1]
}

func (o probeOut) meanOver() float64 { return meanTail(o.overAt, 1*time.Second) }
func (o probeOut) meanThresh() float64 {
	return meanTail(o.threshAt, 1*time.Second)
}
func (o probeOut) meanSamples() float64 { return meanTail(o.samplesAt, 1*time.Second) }

func meanTail(v []float64, skip time.Duration) float64 {
	from := int(skip / (100 * time.Millisecond))
	if from >= len(v) {
		return 0
	}
	var sum float64
	for _, x := range v[from:] {
		sum += x
	}
	return sum / float64(len(v)-from)
}

// shapeAt returns the latency to sleep for one request. el is the elapsed time
// since the run began, util the offered/capacity ratio (0 on a flat downstream).
type shapeAt func(el time.Duration, util float64, seq uint64) time.Duration

// driveProbe runs one limiter against one shape for dur and returns the trace.
func driveProbe(cfg probeCfg, shape shapeAt, dur time.Duration) probeOut {
	workers := cfg.workers
	if workers == 0 {
		workers = 120
	}
	lim := agilelimiter.New(agilelimiter.Config{
		MinRate:       pollMin,
		MaxRate:       cfg.rate,
		InitialRate:   cfg.rate,
		SLOTarget:     cfg.slo,
		TailTolerance: cfg.tol,
		Anchor:        cfg.anchor,
		Preset:        cfg.preset,
		FastHalfLife:  750 * time.Millisecond,
		SlowHalfLife:  45 * time.Second,
		Tick:          250 * time.Millisecond,
	})
	lim.Start()
	defer lim.Stop()

	begin := time.Now()
	deadline := begin.Add(dur)

	var completed atomic.Int64
	var latSum atomic.Int64
	var seq atomic.Uint64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				lim.Take()
				start := time.Now()
				el := start.Sub(begin)
				var util float64
				if cfg.capacity > 0 {
					util = lim.Rate() / cfg.capacity
				}
				time.Sleep(shape(el, util, seq.Add(1)))
				d := time.Since(start)
				lim.Observe(d)
				completed.Add(1)
				latSum.Add(int64(d))
			}
		}()
	}

	out := probeOut{}
	for time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		st := lim.Stats()
		out.rateAt = append(out.rateAt, st.Rate)
		out.overAt = append(out.overAt, st.TailRatio)
		out.threshAt = append(out.threshAt, st.TailThreshold)
		out.samplesAt = append(out.samplesAt, float64(st.Samples))
	}
	wg.Wait()
	out.completed = int(completed.Load())
	if n := completed.Load(); n > 0 {
		out.meanLatency = time.Duration(latSum.Load() / n)
	}
	return out
}

// --- shapes -------------------------------------------------------------

func constShape(d time.Duration) shapeAt {
	return func(time.Duration, float64, uint64) time.Duration { return d }
}

// cohortAt puts `pct` percent of requests at `slow`, whatever the load.
func cohortAt(base time.Duration, pct int, slow time.Duration) shapeAt {
	return func(_ time.Duration, _ float64, seq uint64) time.Duration {
		if inCohort(seq, pct) {
			return slow
		}
		return base
	}
}

// cohortRampAt grows the cohort from 0 to `pct` over the run: a dependency
// getting slower by degrees, or a cache warming up in reverse.
func cohortRampAt(base time.Duration, pct int, slow time.Duration, dur time.Duration) shapeAt {
	return func(el time.Duration, _ float64, seq uint64) time.Duration {
		share := int(float64(pct) * el.Seconds() / dur.Seconds())
		if inCohort(seq, share) {
			return slow
		}
		return base
	}
}

// flatReporting is a downstream that never queues: the rate cannot make it
// faster, so any reduction in rate is a false positive.
func flatReporting(base time.Duration) shapeAt { return constShape(base) }

// mm1Shape is a downstream that does queue: latency = base/(1-util), so a rate
// reduction is the correct response and a rate increase is punished.
func mm1Shape(base time.Duration) shapeAt {
	return func(_ time.Duration, util float64, _ uint64) time.Duration {
		return time.Duration(float64(base) * mm1(util))
	}
}

// --- reporting ----------------------------------------------------------

type verdict int

const (
	holdOK verdict = iota
	throttleOK
	recoverOK
	settleOK
)

func (v verdict) String() string {
	switch v {
	case holdOK:
		return "hold"
	case throttleOK:
		return "throttle"
	case settleOK:
		return "settle"
	default:
		return "recover"
	}
}

// judge turns a run into a pass/fail against the policy expectation.
func judge(v verdict, cfg probeCfg, out probeOut) (bool, string) {
	ceiling := float64(cfg.rate)
	moved := out.minAfter(time.Second) < 0.90*ceiling
	switch v {
	case holdOK:
		if moved {
			return false, "moved"
		}
		return true, "held"
	case throttleOK:
		if !moved {
			return false, "ignored"
		}
		return true, "throttled"
	case settleOK:
		// Contamination that crosses the policy line part-way through: it must not
		// kill the rate early, and it must have moved it by the end.
		if out.minAfter(time.Second) < 0.50*ceiling {
			return false, "died early"
		}
		if out.last() > 0.90*ceiling {
			return false, "ignored"
		}
		return true, "settled"
	default: // recoverOK
		if out.last() < 0.80*ceiling {
			return false, "stuck"
		}
		return true, "recovered"
	}
}

func row(cfg probeCfg, label string, v verdict, out probeOut) string {
	ok, note := judge(v, cfg, out)
	mark := "ok  "
	if !ok {
		mark = "FAIL"
	}
	return fmt.Sprintf("  %-44s %-9s %8.0f %8.0f %9.3f %9.3f %7.0f  %s %s",
		label, v, out.minAfter(time.Second), out.last(),
		out.meanOver(), out.meanThresh(), out.meanSamples(), mark, note)
}

func headerRow() {
	fmt.Printf("  %-44s %-9s %8s %8s %9s %9s %7s  %s\n",
		"case", "expect", "minRate", "endRate", "meanOver", "threshold", "n/tick", "verdict")
}

// --- TestRobustContamination -------------------------------------------

// TestRobustContamination is the main sweep: nine ways a sample stream can lie,
// against the default guard and against a ratio guard at two tolerances. The
// question is not "does the fix work" but "which of these can no amount of
// tolerance fix".
func TestRobustContamination(t *testing.T) {
	type cs struct {
		label  string
		shape  shapeAt
		expect verdict
	}
	cases := []cs{
		{"cohort 5% at 300ms (3x SLO)", cohortAt(24*time.Millisecond, 5, 300*time.Millisecond), holdOK},
		{"cohort 2% at 5s (50x SLO)", cohortAt(24*time.Millisecond, 2, 5*time.Second), holdOK},
		{"cache hits: 80% at 1ms, misses at 60ms", func(_ time.Duration, _ float64, seq uint64) time.Duration {
			if inCohort(seq, 80) {
				return time.Millisecond
			}
			return 60 * time.Millisecond
		}, holdOK},
		{"cohort ramps 0 -> 25% at 300ms", cohortRampAt(24*time.Millisecond, 25, 300*time.Millisecond, 5*time.Second), settleOK},
		{"retry x2: 60ms -> 120ms (uniform)", constShape(120 * time.Millisecond), throttleOK},
		{"retry x3: 40ms -> 120ms (uniform)", constShape(120 * time.Millisecond), throttleOK},
		{"local +50ms: 60ms -> 110ms (uniform)", constShape(110 * time.Millisecond), throttleOK},
		{"local jitter 0..80ms on 60ms", func(_ time.Duration, _ float64, seq uint64) time.Duration {
			return 60*time.Millisecond + time.Duration(seq%80)*time.Millisecond
		}, throttleOK},
		{"burst: 400ms for 1s every 3s", func(el time.Duration, _ float64, _ uint64) time.Duration {
			if int(el.Seconds())%3 == 1 {
				return 400 * time.Millisecond
			}
			return 24 * time.Millisecond
		}, throttleOK}, // a 1-in-3 duty cycle has no clean window to recover in: see TestRobustTransient
	}

	fmt.Println("contamination sweep: flat downstream, so no rate change can be congestion")
	fmt.Printf("ceiling %d req/s, SLO %s, 5s per run, %d runs\n", pollCeiling, pollSLO, len(cases)*3)
	fmt.Println("expect: hold = must not move; throttle = must move (policy says so); recover = must come back")
	fmt.Println()

	for _, tol := range []struct {
		label string
		v     float64
	}{{"default (tol 0)", 0}, {"tol 5%", 0.05}, {"tol 10%", 0.10}} {
		fmt.Printf("--- %s ---\n", tol.label)
		headerRow()
		for _, c := range cases {
			cfg := probeCfg{rate: pollCeiling, tol: tol.v, slo: pollSLO}
			out := driveProbe(cfg, c.shape, 5*time.Second)
			fmt.Println(row(cfg, c.label, c.expect, out))
		}
		fmt.Println()
	}
}

// --- TestRobustCohortShare ---------------------------------------------

// TestRobustCohortShare maps where the cliff is: the guard should hold exactly
// while the cohort share is inside the tolerance and move once it is outside.
// A policy you cannot predict is not a policy.
func TestRobustCohortShare(t *testing.T) {
	const tol = 0.05
	fmt.Printf("cohort share sweep: tol %.0f%%, cohort at 300ms, flat 24ms downstream\n", tol*100)
	fmt.Println("share <= tolerance must hold; share >= 2x tolerance must move")
	fmt.Println()

	for _, rate := range []int{60, 300} {
		fmt.Printf("--- ceiling %d req/s (%.0f samples per 250ms tick) ---\n", rate, float64(rate)/4)
		headerRow()
		for _, share := range []int{1, 2, 3, 4, 5, 8, 12, 20} {
			v := holdOK
			if float64(share) >= 2*tol*100 {
				v = throttleOK
			}
			cfg := probeCfg{rate: rate, tol: tol, slo: pollSLO}
			out := driveProbe(cfg, cohortAt(24*time.Millisecond, share, 300*time.Millisecond), 5*time.Second)
			label := fmt.Sprintf("cohort %d%% of the stream", share)
			fmt.Println(row(cfg, label, v, out))
		}
		fmt.Println()
	}
}

// --- TestRobustAmplitude -----------------------------------------------

// TestRobustAmplitude asks whether the *size* of the slow cohort matters: a
// ratio test should be indifferent to how slow the slow ones are, and that is
// the whole difference from a quantile test.
func TestRobustAmplitude(t *testing.T) {
	const tol = 0.05
	fmt.Printf("cohort amplitude: 5%% of requests at 2x..50x the SLO, tol %.0f%%\n", tol*100)
	fmt.Println("a ratio test must not care how slow the slow ones are")
	fmt.Println()

	for _, arm := range []struct {
		label string
		v     float64
	}{{"default (tol 0)", 0}, {"tol 5%", tol}} {
		fmt.Printf("--- %s ---\n", arm.label)
		headerRow()
		for _, slow := range []time.Duration{150 * time.Millisecond, 300 * time.Millisecond, 1 * time.Second, 5 * time.Second, 30 * time.Second} {
			cfg := probeCfg{rate: pollCeiling, tol: arm.v, slo: pollSLO}
			out := driveProbe(cfg, cohortAt(24*time.Millisecond, 5, slow), 5*time.Second)
			label := fmt.Sprintf("5%% at %s (%.0fx SLO)", slow, float64(slow)/float64(pollSLO))
			fmt.Println(row(cfg, label, holdOK, out))
		}
		fmt.Println()
	}
}

// --- TestRobustRecovery ------------------------------------------------

// TestRobustRecovery is the question behind "does it get killed": a run that is
// polluted for the first half and clean for the second. A limiter that holds the
// rate through the pollution and comes back to the ceiling afterwards is usable;
// one that leaves the rate at MinRate is not.
func TestRobustRecovery(t *testing.T) {
	fmt.Println("recovery: 4s polluted, then 4s clean — does the rate come back?")
	fmt.Println()

	cases := []struct {
		label string
		dirty shapeAt
	}{
		{"cohort 5% at 300ms", cohortAt(24*time.Millisecond, 5, 300*time.Millisecond)},
		{"cohort 20% at 300ms (over policy)", cohortAt(24*time.Millisecond, 20, 300*time.Millisecond)},
		{"all requests at 400ms (blowout)", constShape(400 * time.Millisecond)},
	}
	for _, tol := range []struct {
		label string
		v     float64
	}{{"default (tol 0)", 0}, {"tol 5%", 0.05}} {
		fmt.Printf("--- %s ---\n", tol.label)
		headerRow()
		for _, c := range cases {
			cfg := probeCfg{rate: pollCeiling, tol: tol.v, slo: pollSLO}
			clean := constShape(24 * time.Millisecond)
			mixed := func(el time.Duration, util float64, seq uint64) time.Duration {
				if el < 4*time.Second {
					return c.dirty(el, util, seq)
				}
				return clean(el, util, seq)
			}
			out := driveProbe(cfg, mixed, 8*time.Second)
			fmt.Println(row(cfg, c.label, recoverOK, out))
		}
		fmt.Println()
	}
}

// --- TestRobustCongestion ----------------------------------------------

// TestRobustCongestion is the other half of the contract: a downstream that DOES
// queue must still be detected. Everything above would pass trivially if the
// limiter had simply stopped listening, and this is the test that says it did
// not.
func TestRobustCongestion(t *testing.T) {
	const capacity = 100.0
	fmt.Printf("congestion: a downstream with capacity %.0f req/s (mm1), ceiling above it\n", capacity)
	fmt.Println("the rate must come down — a limiter that ignores this is not a limiter")
	fmt.Println()

	for _, tol := range []struct {
		label string
		v     float64
	}{{"default (tol 0)", 0}, {"tol 5%", 0.05}, {"tol 10%", 0.10}} {
		cfg := probeCfg{rate: 300, tol: tol.v, slo: pollSLO, capacity: capacity}
		// The shape gets util = offered/capacity, so 300/100 = 3: hard overload.
		out := driveProbe(cfg, mm1Shape(20*time.Millisecond), 6*time.Second)
		ok := out.minAfter(time.Second) < 0.9*capacity
		mark := "ok  "
		if !ok {
			mark = "FAIL"
		}
		fmt.Printf("  %-22s minRate %6.0f  endRate %6.0f  meanLatency %6s  %s\n",
			tol.label, out.minAfter(time.Second), out.last(),
			out.meanLatency.Round(time.Millisecond), mark)
		if !ok {
			t.Errorf("%s: rate %.0f did not come down below capacity %.0f under a real overload",
				tol.label, out.minAfter(time.Second), capacity)
		}
	}
}

// --- TestRobustTransient -----------------------------------------------

// TestRobustTransient is the question the periodic-burst case cannot answer: one
// second of total blowout, then five seconds of health. Detection is not the
// interesting part (every request is over the SLO, so every mode fires); the
// interesting part is the bill — how far the rate falls and how long it takes to
// undo, because the climb is UpStep per tick and that is not negotiable.
func TestRobustTransient(t *testing.T) {
	const ceiling = 300.0
	fmt.Println("transient: 1s of 400ms (total blowout), then 5s at 24ms")
	fmt.Println("the cost of one bad second, and the wall clock needed to undo it")
	fmt.Println()

	for _, tol := range []struct {
		label string
		v     float64
	}{{"default (tol 0)", 0}, {"tol 5%", 0.05}} {
		cfg := probeCfg{rate: int(ceiling), tol: tol.v, slo: pollSLO}
		dirty := func(el time.Duration, _ float64, _ uint64) time.Duration {
			if el >= time.Second && el < 2*time.Second {
				return 400 * time.Millisecond
			}
			return 24 * time.Millisecond
		}
		out := driveProbe(cfg, dirty, 6*time.Second)

		at := func(d time.Duration) float64 {
			i := int(d / (100 * time.Millisecond))
			if i >= len(out.rateAt) {
				return 0
			}
			return out.rateAt[i]
		}

		// Time from the end of the burst until the rate is back at 90% of the ceiling.
		back := -1.0
		for i := int(2 * time.Second / (100 * time.Millisecond)); i < len(out.rateAt); i++ {
			if out.rateAt[i] >= 0.90*ceiling {
				back = float64(i)*0.1 - 2.0
				break
			}
		}

		fmt.Printf("  %-16s  t=1s %5.0f | t=2s %5.0f | t=3s %5.0f | t=4s %5.0f | t=6s %5.0f | back to 90%%: ",
			tol.label, at(1*time.Second), at(2*time.Second), at(3*time.Second), at(4*time.Second), at(6*time.Second))
		if back < 0 {
			fmt.Printf("> %.0fs (< %.0f of %0.f at the end)\n", 4.0, out.last(), ceiling)
		} else {
			fmt.Printf("%.1fs\n", back)
		}

		if out.last() <= out.minAfter(time.Second)*1.2 {
			t.Errorf("%s: rate stuck at %.0f after the burst; recovery failed", tol.label, out.last())
		}
	}

	fmt.Println()
	fmt.Println("MinRate -> MaxRate is ln(MaxRate/MinRate)/ln(1+UpStep) ticks: 84 ticks at")
	fmt.Println("the shipped defaults (5 -> 300), i.e. 21s at Tick=250ms and 84s at Tick=1s.")
}
