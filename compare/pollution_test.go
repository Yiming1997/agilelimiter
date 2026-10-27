// Pollution experiments: what the controller does when the sample stream
// contains durations that do not measure the resource it is protecting.
//
// The downstream here is deliberately *flat*: its latency does not rise with the
// rate. That is the point — with nothing to detect, any rate reduction is a
// false positive by construction, so the control arm (same limiter, same
// downstream, pollution removed) and the polluted arm differ for exactly one
// reason.
//
//	cd compare && go test -run TestPollution -v -timeout 20m
//
// uber(100) is the blindness control: it ignores the sample stream entirely, so
// none of these pathologies can touch it — and none can help it either.
package main

import (
	"fmt"
	"testing"
	"time"

	"agilelimiter"
)

const (
	pollDur      = 8 * time.Second
	pollCeiling  = 300
	pollInit     = 40
	pollMin      = 5
	pollSLO      = 100 * time.Millisecond
	pollCapacity = 400.0 // above the ceiling on purpose: capacity is not the variable
)

// flatLatency is a downstream whose latency does not depend on load.
func flatLatency(d time.Duration) func(util float64, t time.Duration, seq uint64) time.Duration {
	return func(float64, time.Duration, uint64) time.Duration { return d }
}

// inCohort reports whether this request belongs to the first `pct` percent of
// the population. Deterministic on purpose: a run has to be repeatable.
func inCohort(seq uint64, pct int) bool { return int(seq%100) < pct }

// withFastPath answers `pct` percent of requests from somewhere else: a local
// cache, a cheap endpoint, a fast error path.
func withFastPath(
	real func(float64, time.Duration, uint64) time.Duration,
	pct int,
	fast time.Duration,
) func(float64, time.Duration, uint64) time.Duration {
	return func(util float64, t time.Duration, seq uint64) time.Duration {
		if inCohort(seq, pct) {
			return fast
		}
		return real(util, t, seq)
	}
}

// withSlowCohort makes `pct` percent of requests always cost `slow`, whatever
// the load: a report endpoint, a large payload, a different route sharing the
// budget.
func withSlowCohort(
	real func(float64, time.Duration, uint64) time.Duration,
	pct int,
	slow time.Duration,
) func(float64, time.Duration, uint64) time.Duration {
	return func(util float64, t time.Duration, seq uint64) time.Duration {
		if inCohort(seq, pct) {
			return slow
		}
		return real(util, t, seq)
	}
}

// withRetry multiplies the measured round trip, as an internal retry inside Do
// does: two attempts, one sample.
func withRetry(
	real func(float64, time.Duration, uint64) time.Duration,
	factor float64,
) func(float64, time.Duration, uint64) time.Duration {
	return func(util float64, t time.Duration, seq uint64) time.Duration {
		return time.Duration(float64(real(util, t, seq)) * factor)
	}
}

// withLocal adds caller-side work to the sample — serialization, a mutex, a
// scheduled wait. It consumes none of the downstream's capacity.
func withLocal(
	real func(float64, time.Duration, uint64) time.Duration,
	extra time.Duration,
) func(float64, time.Duration, uint64) time.Duration {
	return func(util float64, t time.Duration, seq uint64) time.Duration {
		return real(util, t, seq) + extra
	}
}

// agilePolluted is the shipped default law, with a ceiling it can reach so that
// "did the rate move?" is a clean question.
func agilePolluted() (limiter, func()) {
	l := agilelimiter.New(agilelimiter.Config{
		MinRate:      pollMin,
		MaxRate:      pollCeiling,
		InitialRate:  pollInit,
		SLOTarget:    pollSLO,
		FastHalfLife: 750 * time.Millisecond,
		SlowHalfLife: 45 * time.Second,
		Tick:         250 * time.Millisecond,
	})
	l.Start()
	return &agileLim{l: l}, l.Stop
}

// agileTolerance is the shipped law with the ratio-mode tail guard.
func agileTolerance(tol float64) func() (limiter, func()) {
	return func() (limiter, func()) {
		l := agilelimiter.New(agilelimiter.Config{
			MinRate:       pollMin,
			MaxRate:       pollCeiling,
			InitialRate:   pollInit,
			SLOTarget:     pollSLO,
			TailTolerance: tol,
			FastHalfLife:  750 * time.Millisecond,
			SlowHalfLife:  45 * time.Second,
			Tick:          250 * time.Millisecond,
		})
		l.Start()
		return &agileLim{l: l}, l.Stop
	}
}

// TestTailTolerance is the fix under test: the guard compares a *fraction* of the
// population against a tolerance instead of comparing one order statistic against
// a threshold. A fixed cohort should stop costing the rate; a real blowout should
// still be caught.
func TestTailTolerance(t *testing.T) {
	cohort := withSlowCohort(flatLatency(24*time.Millisecond), 5, 300*time.Millisecond)
	blowout := flatLatency(300 * time.Millisecond)

	fmt.Println("tail guard: quantile test vs ratio test, flat downstream again")
	fmt.Printf("limiter: MinRate %d, MaxRate %d, SLOTarget %s; one %s run per arm\n",
		pollMin, pollCeiling, pollSLO, pollDur)
	fmt.Println()
	fmt.Println("tol 0   = the historical test: one slow sample in the tick fires it")
	fmt.Println("tol 5%/10% = fire only when that share of the tick is over the target")

	for _, sc := range []struct {
		label string
		shape func(float64, time.Duration, uint64) time.Duration
	}{
		{"A — 5% of requests always cost 300ms (a cohort, not congestion)", cohort},
		{"B — every request costs 300ms (a real blowout)", blowout},
	} {
		rows := []result{
			runOn("tol 0", agileTolerance(0), downstream{shape: sc.shape}, flat(pollCapacity), pollDur, 120),
			runOn("tol 5%", agileTolerance(0.05), downstream{shape: sc.shape}, flat(pollCapacity), pollDur, 120),
			runOn("tol 10%", agileTolerance(0.10), downstream{shape: sc.shape}, flat(pollCapacity), pollDur, 120),
		}
		report("=== "+sc.label+" ===", rows)
	}
}

func TestPollution(t *testing.T) {
	cases := []struct {
		label string
		base  time.Duration
		harm  string
		shape func(real func(float64, time.Duration, uint64) time.Duration) func(float64, time.Duration, uint64) time.Duration
	}{
		{
			label: "A — 80% of requests answer in 1ms (cache hits)",
			base:  60 * time.Millisecond,
			harm:  "p50 is the cache; the miss path is invisible to the median",
			shape: func(real func(float64, time.Duration, uint64) time.Duration) func(float64, time.Duration, uint64) time.Duration {
				return withFastPath(real, 80, time.Millisecond)
			},
		},
		{
			label: "B — 5% of requests always cost 300ms (a slow cohort)",
			base:  24 * time.Millisecond,
			harm:  "p99 is that cohort on most ticks, so the tail guard reads a fixed population as congestion",
			shape: func(real func(float64, time.Duration, uint64) time.Duration) func(float64, time.Duration, uint64) time.Duration {
				return withSlowCohort(real, 5, 300*time.Millisecond)
			},
		},
		{
			label: "C — an internal retry doubles the sample (60ms -> 120ms)",
			base:  60 * time.Millisecond,
			harm:  "the absolute SLO check compares twice the real latency",
			shape: func(real func(float64, time.Duration, uint64) time.Duration) func(float64, time.Duration, uint64) time.Duration {
				return withRetry(real, 2)
			},
		},
		{
			label: "D1 — 20ms of caller-side work, sample stays under the SLO (20ms -> 40ms)",
			base:  20 * time.Millisecond,
			harm:  "non-downstream time is read as downstream slowness",
			shape: func(real func(float64, time.Duration, uint64) time.Duration) func(float64, time.Duration, uint64) time.Duration {
				return withLocal(real, 20*time.Millisecond)
			},
		},
		{
			label: "D2 — 50ms of caller-side work, sample crosses the SLO (60ms -> 110ms)",
			base:  60 * time.Millisecond,
			harm:  "same pollution, but now it trips the tail guard",
			shape: func(real func(float64, time.Duration, uint64) time.Duration) func(float64, time.Duration, uint64) time.Duration {
				return withLocal(real, 50*time.Millisecond)
			},
		},
	}

	fmt.Println("pollution: what does a sample that does not measure the downstream cost?")
	fmt.Printf("downstream: FLAT latency (no queueing), so no rate change can be congestion\n")
	fmt.Printf("limiter: MinRate %d, MaxRate %d, SLO %s; one %s run per arm\n",
		pollMin, pollCeiling, pollSLO, pollDur)
	fmt.Println()
	fmt.Println("control  = the same limiter and downstream, pollution removed")
	fmt.Println("polluted = the pathology present; any lower rate is a false positive")

	for _, c := range cases {
		real := flatLatency(c.base)
		rows := make([]result, 0, 3)
		for _, arm := range []struct {
			label string
			mk    func() (limiter, func())
			shape func(float64, time.Duration, uint64) time.Duration
		}{
			{"control", agilePolluted, real},
			{"polluted", agilePolluted, c.shape(real)},
			{"uber(100)", newUber(100), c.shape(real)},
		} {
			rows = append(rows, runOn(arm.label, arm.mk, downstream{shape: arm.shape}, flat(pollCapacity), pollDur, 120))
		}
		report("=== "+c.label+" — "+c.harm+" ===", rows)
	}
}
