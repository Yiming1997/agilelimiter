// Diagnostic: the same 5% cohort, two guard settings, sampled every second.
//
// compare/pollution_test.go prints the end state; this prints the trajectory, so
// "did the fix work" is answerable without inferring it from a final rate.
//
//	cd compare && go test -run TestDiagTailToleranceTrace -v
package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agilelimiter"
)

func TestDiagTailToleranceTrace(t *testing.T) {
	shape := withSlowCohort(flatLatency(24*time.Millisecond), 5, 300*time.Millisecond)

	for _, tol := range []float64{0, 0.05, 0.10} {
		lim := agilelimiter.New(agilelimiter.Config{
			MinRate:       pollMin,
			MaxRate:       pollCeiling,
			InitialRate:   pollInit,
			SLOTarget:     pollSLO,
			TailTolerance: tol,
			FastHalfLife:  750 * time.Millisecond,
			SlowHalfLife:  45 * time.Second,
			Tick:          250 * time.Millisecond,
		})
		lim.Start()

		begin := time.Now()
		deadline := begin.Add(8 * time.Second)

		var done, seq atomic.Uint64
		var wg sync.WaitGroup
		for i := 0; i < 120; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(deadline) {
					lim.Take()
					start := time.Now()
					t := start.Sub(begin)
					time.Sleep(shape(lim.Rate()/pollCapacity, t, seq.Add(1)))
					elapsed := time.Since(start)
					lim.Observe(elapsed)
					done.Add(1)
				}
			}()
		}

		fmt.Printf("\n=== TailTolerance %.2f (5%% of requests always cost 300ms) ===\n", tol)
		fmt.Printf("%6s %7s %7s %9s %8s\n", "t(s)", "rate", "over", "done/s", "p99")
		var last uint64
		for i := 1; i <= 8; i++ {
			time.Sleep(time.Second)
			st := lim.Stats()
			cur := done.Load()
			fmt.Printf("%6d %7.0f %7.3f %9d %8s\n",
				i, st.Rate, st.TailRatio, cur-last, st.P99.Round(time.Millisecond))
			last = cur
		}
		wg.Wait()
		lim.Stop()
	}
}
