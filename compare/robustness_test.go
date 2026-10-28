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

	// tailFactor moves the guard's threshold away from the SLO. Setting it very
	// high disables the guard without disabling the median law, which is how the
	// ablation below tells the two apart.
	tailFactor float64

	// backoffTicks is the median law's time hysteresis: 1 (the default) spends a
	// step on the first bad tick, 2 requires agreement.
	backoffTicks int

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
		TailFactor:    cfg.tailFactor,
		BackoffTicks:  cfg.backoffTicks,
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
)

func (v verdict) String() string {
	switch v {
	case holdOK:
		return "hold"
	case throttleOK:
		return "throttle"
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
		// A cohort that ramps through the policy line: by the end it is far over, so
		// the rate must have come down. That it does not die *before* the line is
		// crossed is TestRobustCohortShare's job — a minimum over the whole run
		// cannot express it, because for a monotonic ramp the minimum is the end.
		{"cohort ramps 0 -> 25% at 300ms", cohortRampAt(24*time.Millisecond, 25, 300*time.Millisecond, 5*time.Second), throttleOK},
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

// --- TestRobustLongStall ------------------------------------------------

// TestRobustLongStall models the thing a one-second stall does not: a downstream
// that stops answering for seconds at a time — a wedged lock, a stop-the-world
// pause, a network partition. A request that arrives during the block returns
// when the block ends, so its latency is "time left in the block", and the
// sample stream dries up after the first tick because every worker is inside it.
//
// Two questions, and they have different answers: what the rate does *during*
// the block (the controller goes blind when no samples arrive), and how long it
// takes to come back afterwards (the climb is UpStep per tick, and fast has to
// decay first).
func TestRobustLongStall(t *testing.T) {
	const ceiling = 300.0
	const blockSLO = pollSLO

	fmt.Println("long stall: the downstream stops answering for D seconds (2s warmup, 8s after)")
	fmt.Println("latency during the block = time left in the block, so p50 ~= D/2")
	fmt.Println()
	fmt.Printf("  %6s %14s %10s %10s %10s %10s  %s\n",
		"block", "rate@end", "+1s", "+2s", "+4s", "+8s", "back to 90%")

	for _, block := range []time.Duration{0, 1 * time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second} {
		start := 2 * time.Second
		end := start + block
		shape := func(el time.Duration, _ float64, _ uint64) time.Duration {
			if el >= start && el < end {
				return end - el + 24*time.Millisecond // wedged until the block ends
			}
			return 24 * time.Millisecond
		}
		cfg := probeCfg{rate: int(ceiling), tol: 0.05, slo: blockSLO}
		out := driveProbe(cfg, shape, end+8*time.Second)

		at := func(d time.Duration) float64 {
			i := int(d / (100 * time.Millisecond))
			if i < 0 || i >= len(out.rateAt) {
				return 0
			}
			return out.rateAt[i]
		}

		back := "never in window"
		for i := int(end / (100 * time.Millisecond)); i < len(out.rateAt); i++ {
			if out.rateAt[i] >= 0.90*ceiling {
				back = fmt.Sprintf("%.1fs", float64(i)*0.1-end.Seconds())
				break
			}
		}

		label := "none"
		if block > 0 {
			label = fmt.Sprintf("%4.0fs", block.Seconds())
		}
		fmt.Printf("  %6s %14.0f %10.0f %10.0f %10.0f %10.0f  %s\n",
			label, at(end), at(end+1*time.Second), at(end+2*time.Second),
			at(end+4*time.Second), at(end+8*time.Second), back)
	}

	fmt.Println()
	fmt.Println("a block is not one event: it is one cut, then a blind hold while fast decays,")
	fmt.Println("then the ordinary additive climb. The climb is the expensive part.")

	// Repeated blocks: the climb gets 5s between them, which is roughly the
	// recovery time of a single block. If the damage accumulated, the rate would
	// ratchet down with a flapping downstream.
	fmt.Println()
	fmt.Println("repeated: 5s block, 5s clean, three times")
	repeat := func(el time.Duration, _ float64, _ uint64) time.Duration {
		// blocks at [2,7), [12,17), [22,27)
		if s := el.Seconds(); s >= 2 && s < 27 && int(s)%10 >= 2 && int(s)%10 < 7 {
			end := time.Duration(int(s)/10*10+7) * time.Second
			return end - el + 24*time.Millisecond
		}
		return 24 * time.Millisecond
	}
	out := driveProbe(probeCfg{rate: int(ceiling), tol: 0.05, slo: blockSLO}, repeat, 34*time.Second)
	fmt.Printf("  %-6s", "t")
	for s := 0; s <= 33; s++ {
		fmt.Printf("%5d", s)
	}
	fmt.Printf("\n  %-6s", "rate")
	for s := 0; s <= 33; s++ {
		i := s * 10
		if i >= len(out.rateAt) {
			break
		}
		fmt.Printf("%5.0f", out.rateAt[i])
	}
	fmt.Println("\n  (blocks run t=2..7, 12..17, 22..27)")
}

// --- TestRobustFlapping ------------------------------------------------

// blocks returns a shape where the downstream is wedged for `length` out of
// every `period`, starting at `start`. A request that arrives during a block
// returns when the block ends.
func blocks(start, period, length time.Duration) shapeAt {
	return func(el time.Duration, _ float64, _ uint64) time.Duration {
		if el < start {
			return 24 * time.Millisecond
		}
		in := (el - start) % period
		if in < length {
			return length - in + 24*time.Millisecond
		}
		return 24 * time.Millisecond
	}
}

// partialBlocks wedges only `pct` percent of requests: half the shards down, a
// lock that one code path takes, a cohort that shares a saturated dependency.
func partialBlocks(start, period, length time.Duration, pct int) shapeAt {
	full := blocks(start, period, length)
	return func(el time.Duration, util float64, seq uint64) time.Duration {
		if inCohort(seq, pct) {
			return full(el, util, seq)
		}
		return 24 * time.Millisecond
	}
}

// TestRobustFlapping asks whether blocks that are too frequent to recover from
// ratchet the rate down, and where the boundary is. One block costs one -30%
// cut and about 5s of reduced throughput; if blocks arrive faster than that, the
// cuts should accumulate instead of cancelling out.
func TestRobustFlapping(t *testing.T) {
	const ceiling = 300.0
	fmt.Println("flapping: one 1s block every P seconds, 20s run")
	fmt.Println("each block used to cost one cut; the question is whether the climb keeps up")
	fmt.Println()

	for _, period := range []time.Duration{2 * time.Second, 3 * time.Second, 4 * time.Second, 6 * time.Second} {
		out := driveProbe(probeCfg{rate: int(ceiling), tol: 0.05, slo: pollSLO},
			blocks(2*time.Second, period, 1*time.Second), 22*time.Second)

		fmt.Printf("  P=%2.0fs  rate:", period.Seconds())
		for s := 2; s <= 21; s++ {
			i := s * 10
			if i >= len(out.rateAt) {
				break
			}
			fmt.Printf("%4.0f", out.rateAt[i])
		}
		fmt.Printf("   min %.0f  end %.0f\n", out.minAfter(2*time.Second), out.last())
	}

	fmt.Println()
	fmt.Println("a cut costs ln(1/0.7)/ln(1.05) = 7.3 ticks (1.8s) to undo, plus ~3s while")
	fmt.Println("fast decays; below that period the rate must ratchet.")

	// A block produces exactly one bad tick — the burst of samples that arrives
	// when it ends — so requiring two consecutive bad ticks should make it free.
	// That is the whole question: does BackoffTicks=2 buy the flapping case, and
	// what does it cost on a real overload?
	fmt.Println()
	fmt.Println("same, with BackoffTicks=2 (two consecutive bad ticks required)")
	for _, period := range []time.Duration{2 * time.Second, 3 * time.Second, 4 * time.Second} {
		out := driveProbe(probeCfg{rate: int(ceiling), tol: 0.05, slo: pollSLO, backoffTicks: 2},
			blocks(2*time.Second, period, 1*time.Second), 22*time.Second)
		fmt.Printf("  P=%2.0fs  min %3.0f  end %3.0f\n", period.Seconds(), out.minAfter(2*time.Second), out.last())
	}
}

// TestRobustPartialBlock is the case a latency-only controller cannot win: part
// of the traffic is stuck for reasons that have nothing to do with load, so the
// rate cut cannot fix it and every tick is over the policy line. The interesting
// question is where the cliff is, and what it costs afterwards.
func TestRobustPartialBlock(t *testing.T) {
	const ceiling = 300.0
	fmt.Println("partial block: 5s wedged every 12s, only `pct` percent of requests affected")
	fmt.Println()
	fmt.Printf("  %6s %10s %10s %10s %10s %10s\n",
		"cohort", "rate@end", "+2s", "+5s", "+8s", "end")

	for _, pct := range []int{5, 10, 20, 50} {
		start := 2 * time.Second
		end := start + 5*time.Second
		out := driveProbe(probeCfg{rate: int(ceiling), tol: 0.05, slo: pollSLO},
			partialBlocks(start, 12*time.Second, 5*time.Second, pct), 22*time.Second)

		at := func(d time.Duration) float64 {
			i := int(d / (100 * time.Millisecond))
			if i < 0 || i >= len(out.rateAt) {
				return 0
			}
			return out.rateAt[i]
		}
		fmt.Printf("  %5d%% %10.0f %10.0f %10.0f %10.0f %10.0f   meanOver %.2f\n",
			pct, at(end), at(end+2*time.Second), at(end+5*time.Second),
			at(end+8*time.Second), out.last(), out.meanOver())
	}

	fmt.Println()
	fmt.Println("the cliff is the same one as everywhere else: tol + 3*sigma. Below it the")
	fmt.Println("policy says ignore; above it the policy says back off, and the rate cut cannot")
	fmt.Println("help because the slowness is not caused by load.")
}

// TestRobustFlappingAblation asks which component does the cutting when blocks
// arrive faster than the climb. BackoffTicks=2 changes nothing, which already
// says it is not the median law; muting the guard should say the rest.
func TestRobustFlappingAblation(t *testing.T) {
	const ceiling = 300.0
	fmt.Println("flapping at P=2s, one 1s block every 2s: which component cuts?")
	fmt.Println()

	arms := []struct {
		label string
		cfg   probeCfg
	}{
		{"tol 5% (baseline)", probeCfg{rate: int(ceiling), tol: 0.05, slo: pollSLO}},
		{"tol 5% + BackoffTicks 2", probeCfg{rate: int(ceiling), tol: 0.05, slo: pollSLO, backoffTicks: 2}},
		{"tol 5% + guard muted", probeCfg{rate: int(ceiling), tol: 0.05, slo: pollSLO, tailFactor: 100}},
		{"tol 5% + both muted", probeCfg{rate: int(ceiling), tol: 0.05, slo: pollSLO, tailFactor: 100, backoffTicks: 2}},
		{"tol 0 (quantile guard)", probeCfg{rate: int(ceiling), tol: 0, slo: pollSLO}},
	}
	for _, a := range arms {
		out := driveProbe(a.cfg, blocks(2*time.Second, 2*time.Second, 1*time.Second), 22*time.Second)
		fmt.Printf("  %-22s min %3.0f  end %3.0f\n", a.label, out.minAfter(2*time.Second), out.last())
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
		label  string
		v      float64
		factor float64
	}{
		{"default (tol 0)", 0, 0},
		{"tol 5%", 0.05, 0},
		// Ablation: the same ratio guard with a threshold no latency can reach,
		// i.e. the guard cannot fire and only the median law acts. If this arm
		// recovers and the one above does not, the guard is what keeps cutting.
		{"tol 5%, guard off", 0.05, 100},
	} {
		cfg := probeCfg{rate: int(ceiling), tol: tol.v, slo: pollSLO, tailFactor: tol.factor}
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

		fmt.Printf("  %-16s  t=1s %5.0f | t=2s %5.0f | t=3s %5.0f | t=4s %5.0f | t=5.5s %5.0f | back to 90%%: ",
			tol.label, at(1*time.Second), at(2*time.Second), at(3*time.Second), at(4*time.Second), at(5500*time.Millisecond))
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
