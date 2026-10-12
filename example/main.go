// Command example runs a self-contained simulation of agilelimiter against a
// toy downstream whose latency rises as the offered rate approaches its
// capacity, and whose capacity changes over time:
//
//	0s .. 3s   healthy   (capacity ~150 req/s)
//	3s .. 6s   degraded  (capacity drops to ~60 req/s)
//	6s .. end  recovered (capacity back to ~150 req/s)
//
// Watch the "rate" column: the controller discovers a safe rate, backs off when
// the downstream degrades, and ramps back up once it recovers. No real network
// is involved — the downstream is a formula — so this is deterministic enough to
// reason about and quick enough to run.
//
// Run it with:
//
//	go run ./example
package main

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"agilelimiter"
)

// downstream is a toy model of a dependency. Below capacity it answers in
// `base`; as the offered rate approaches capacity its response time grows
// without bound (an M/M/1-flavoured wait, 1/(1-util)).
type downstream struct {
	base     time.Duration
	capacity *atomic.Int64 // requests per second it can serve at `base`
}

func (d *downstream) rtt(offered float64) time.Duration {
	util := offered / float64(d.capacity.Load())
	// Clamp so the model stays finite instead of dividing by zero at saturation.
	util = math.Max(0, math.Min(util, 0.95))
	return time.Duration(float64(d.base) / (1 - util))
}

func main() {
	const (
		tick      = 100 * time.Millisecond
		baseRTT   = 20 * time.Millisecond
		workers   = 64
		reportFor = 10 * time.Second
	)

	var capacity atomic.Int64
	capacity.Store(150) // start healthy
	ds := &downstream{base: baseRTT, capacity: &capacity}

	lim := agilelimiter.New(agilelimiter.Config{
		MinRate:         10,  // never collapse below this
		MaxRate:         500, // hard ceiling we promise the downstream
		InitialRate:     20,  // cold-start low, let the controller ramp up
		Tick:            tick,
		FastHalfLife:    300 * time.Millisecond, // responsive to recent latency
		SlowHalfLife:    3 * time.Second,        // healthy baseline
		TargetInflation: 1.5,                    // tolerate latency up to 2.5x baseline
		SLOTarget:       500 * time.Millisecond, // tail guard
	})
	lim.Start()
	defer lim.Stop()

	// Drive load with a worker pool, not a single goroutine: one worker would be
	// blocked inside the simulated round trip and could never reach the higher
	// rates, so the limiter's pacing would never actually be exercised.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Do takes a slot, times this call, and feeds the measured
				// round trip back to the controller — no manual Take/Observe.
				_ = lim.Do(func() error {
					time.Sleep(ds.rtt(lim.Rate())) // simulate the request round trip
					return nil
				})
			}
		}()
	}

	// Phase schedule: degrade the downstream, then let it recover.
	go func() {
		time.Sleep(3 * time.Second)
		capacity.Store(60) // downstream slows down
		time.Sleep(3 * time.Second)
		capacity.Store(150) // downstream recovers
	}()

	fmt.Println("t      capacity   rate          baseline    fast")
	start := time.Now()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for i := 0; i < int(reportFor/time.Second); i++ {
		<-t.C
		st := lim.Stats()
		fmt.Printf("%-6s %-10d %-13.0f %-11s %s\n",
			fmt.Sprintf("%ds", int(time.Since(start).Seconds())),
			capacity.Load(),
			st.Rate,
			st.Baseline.Round(time.Millisecond),
			st.Fast.Round(time.Millisecond),
		)
	}

	close(stop)
	wg.Wait()

	st := lim.Stats()
	fmt.Printf("\nfinal: rate=%.0f req/s, baseline=%s, fast=%s\n",
		st.Rate, st.Baseline.Round(time.Millisecond), st.Fast.Round(time.Millisecond))
	fmt.Println("the controller settled near the point where latency starts to climb,")
	fmt.Println("not at MaxRate — that is the whole idea: it protects the downstream.")
}
