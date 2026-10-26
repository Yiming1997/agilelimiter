// A matrix sweep: six downstream scenarios x five controllers x two rounds.
//
// The A/B in sloanchor_test.go answers "why the anchor matters". This one asks
// the operational question: across every shape a downstream can take, which
// controller holds up, and where does each one break?
//
//	cd compare && go test -run TestSLOAnchorMatrix -v -timeout 30m
//
// Each run is 12s: 5s healthy, then the scenario for the remaining 7s. Round
// means are reported with the observed spread, because an adaptive controller
// that wins on average but flips between MinRate and MaxRate has not held up.
package main

import (
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"agilelimiter"
)

const (
	mPhase  = 12 * time.Second
	mRounds = 2
	mPool   = 200 // high enough that the limiter, not the worker pool, is the limit
)

// baseStep jumps from one idle latency to another at t=5s.
func baseStep(from, to time.Duration) func(time.Duration) time.Duration {
	return func(t time.Duration) time.Duration {
		if t < 5*time.Second {
			return from
		}
		return to
	}
}

// baseFlap alternates between two idle latencies every period.
func baseFlap(lo, hi, period time.Duration) func(time.Duration) time.Duration {
	return func(t time.Duration) time.Duration {
		if int64(t/period)%2 == 0 {
			return lo
		}
		return hi
	}
}

// capacityStep jumps from one capacity to another at t=5s.
func capacityStep(from, to float64) func(time.Duration) float64 {
	return func(t time.Duration) float64 {
		if t < 5*time.Second {
			return from
		}
		return to
	}
}

// capacitySine is a diurnal-style capacity curve.
func capacitySine(mean, amp float64, period time.Duration) func(time.Duration) float64 {
	return func(t time.Duration) float64 {
		return mean + amp*math.Sin(2*math.Pi*t.Seconds()/period.Seconds())
	}
}

type matrixArm struct {
	label string
	mk    func() (limiter, func())
}

func matrixArms() []matrixArm {
	return []matrixArm{
		{"uber(60)", newUber(60)},
		{"uber(100)", newUber(100)},
		{"uber(140)", newUber(140)},
		{"agile-2x", agileAnchor(agilelimiter.AnchorBaseline, 1.0, 0)},
		{"agile-slo", agileAnchor(agilelimiter.AnchorSLO, 0, 2)},
	}
}

type matrixScenario struct {
	label string
	ds    downstream
	cap   func(time.Duration) float64
}

func matrixScenarios() []matrixScenario {
	steadyDS := downstream{base: steadyBase(20 * time.Millisecond), wait: sharpKnee}
	return []matrixScenario{
		{"0 steady, UNKNOWN capacity 150/s", steadyDS, flat(capacity)},
		{"1 benign bump 20ms -> 60ms at t=5s (under the SLO)", downstream{base: baseStep(20*time.Millisecond, 60*time.Millisecond), wait: sharpKnee}, flat(capacity)},
		{"2 SLO breaking 20ms -> 150ms at t=5s (over the SLO)", downstream{base: baseStep(20*time.Millisecond, 150*time.Millisecond), wait: sharpKnee}, flat(capacity)},
		{"3 capacity DROPS 150 -> 40/s at t=5s", steadyDS, capacityStep(150, 40)},
		{"4 capacity RECOVERS 40 -> 150/s at t=5s", steadyDS, capacityStep(40, 150)},
		{"5 flapping latency 20ms <-> 70ms every 2s", downstream{base: baseFlap(20*time.Millisecond, 70*time.Millisecond, 2*time.Second), wait: sharpKnee}, flat(capacity)},
		{"6 diurnal capacity 100 +/- 60/s over a 6s period", steadyDS, capacitySine(100, 60, 6*time.Second)},
	}
}

type cell struct {
	label    string
	goodput  float64 // mean across rounds
	gLo, gHi float64
	achieved float64
	okPct    float64
	p99      time.Duration
	rate     float64
}

func mean(v []float64) float64 {
	var s float64
	for _, x := range v {
		s += x
	}
	return s / float64(len(v))
}

func measure(a matrixArm, sc matrixScenario) cell {
	g := make([]float64, 0, mRounds)
	ach := make([]float64, 0, mRounds)
	ok := make([]float64, 0, mRounds)
	rt := make([]float64, 0, mRounds)
	var p99 time.Duration

	for i := 0; i < mRounds; i++ {
		r := runOn(a.label, a.mk, sc.ds, sc.cap, mPhase, mPool)
		g = append(g, r.goodput)
		ach = append(ach, r.achieved)
		ok = append(ok, r.okPct)
		rt = append(rt, r.finalRate)
		p99 += r.p99 / time.Duration(mRounds)
	}

	sorted := append([]float64(nil), g...)
	sort.Float64s(sorted)
	return cell{
		label: a.label, goodput: mean(g), gLo: sorted[0], gHi: sorted[len(sorted)-1],
		achieved: mean(ach), okPct: mean(ok), p99: p99, rate: mean(rt),
	}
}

func reportMatrix(title string, cells []cell) {
	fmt.Printf("\n%s\n", title)
	fmt.Printf("  %-11s %16s %11s %7s %8s %6s\n", "arm", "goodput/s", "achieved/s", "%<=SLO", "p99", "rate")
	for _, c := range cells {
		fmt.Printf("  %-11s %5.0f[%3.0f-%3.0f] %11.0f %6.0f%% %8s %6.0f\n",
			c.label, c.goodput, c.gLo, c.gHi, c.achieved, c.okPct,
			c.p99.Round(time.Millisecond), c.rate)
	}
}

func TestSLOAnchorMatrix(t *testing.T) {
	arms := matrixArms()
	scenarios := matrixScenarios()

	fmt.Println("agilelimiter anchor modes vs go.uber.org/ratelimit — scenario matrix")
	fmt.Printf("downstream: latency = base(t) * 1/(1-util^8), SLO %s, capacity 150/s unless stated\n", sloString)
	fmt.Printf("each cell: %d rounds of %s (5s healthy, then the scenario)\n", mRounds, mPhase)
	fmt.Println("agile arms: MinRate 5, MaxRate 120. Every arm gets 200 workers, so the")
	fmt.Println("limiter is always the binding constraint.")
	fmt.Println()
	fmt.Println("goodput/s  = requests inside the SLO, mean of the rounds, [min-max]")
	fmt.Println("achieved/s = all requests completed, SLO or not — what the downstream ate")
	fmt.Println("rate       = controller's rate at the end, mean of the rounds")

	type tally struct {
		sum  float64
		wins int
	}
	totals := map[string]*tally{}

	for _, sc := range scenarios {
		sc := sc
		var cells []cell
		// Subtests so a single scenario can be re-run:
		//   go test -run 'TestSLOAnchorMatrix/4' -v .
		t.Run(sc.label, func(t *testing.T) {
			for _, a := range arms {
				cells = append(cells, measure(a, sc))
			}
			reportMatrix("=== "+sc.label+" ===", cells)
		})

		best := 0.0
		for _, c := range cells {
			if c.goodput > best {
				best = c.goodput
			}
			if totals[c.label] == nil {
				totals[c.label] = &tally{}
			}
			totals[c.label].sum += c.goodput
		}
		for _, c := range cells {
			// "held up" = within 10% of the best goodput in this scenario. When the
			// whole field is near zero (an SLO nobody can meet) nothing counts as a
			// win, which is the honest reading.
			if best > 1 && c.goodput >= 0.9*best {
				totals[c.label].wins++
			}
		}
	}

	fmt.Printf("\n=== summary across %d scenarios ===\n", len(scenarios))
	fmt.Printf("  %-11s %14s %10s\n", "arm", "total goodput", "held up")
	for _, a := range arms {
		tt := totals[a.label]
		fmt.Printf("  %-11s %14.0f %6d/%-3d\n", a.label, tt.sum, tt.wins, len(scenarios))
	}
	fmt.Println("\n'held up' counts scenarios where the arm was within 10% of the best")
	fmt.Println("goodput. 'total goodput' sums goodput/s, so an arm that wins one scenario")
	fmt.Println("by 3x and loses the rest badly still scores low.")
}
