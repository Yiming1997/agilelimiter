// Command concurrency runs a self-contained simulation of the *inbound*
// ConcurrencyLimiter against a toy model of this very service, whose per-request
// handling time rises as more requests run at once, and whose capacity changes
// over time:
//
//	0s .. 3s   healthy   (can serve ~150 requests in parallel at `base`)
//	3s .. 6s   degraded  (parallel capacity drops to ~60)
//	6s .. end  recovered (capacity back to ~150)
//
// A swarm of callers hammers the service. Watch the "limit" column: with no cap,
// thousands of goroutines pile up and handling time explodes; the controller
// discovers the concurrency that keeps handling time near `base`, backs off when
// the service degrades, and ramps back up once it recovers. Requests beyond the
// limit are refused (ErrOverloaded) instead of queued — that refusal is what
// protects the process.
//
// No real network is involved — the service is a formula — so this is
// deterministic enough to reason about and quick enough to run.
//
// Run it with:
//
//	go run ./example/concurrency
package main

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"agilelimiter"
)

// service is a toy model of *this* process. Up to `parallel` concurrent
// requests it answers in `base`; beyond that, requests contend and handling time
// grows in proportion to how far the in-flight count exceeds `parallel`.
type service struct {
	base     time.Duration
	parallel *atomic.Int64
}

func (s *service) handlerRTT(inflight int) time.Duration {
	p := float64(s.parallel.Load())
	contention := math.Max(1, float64(inflight)/p)
	return time.Duration(float64(s.base) * contention)
}

func main() {
	const (
		tick      = 100 * time.Millisecond
		baseRTT   = 20 * time.Millisecond
		callers   = 500
		reportFor = 10 * time.Second
	)

	var parallel atomic.Int64
	parallel.Store(150) // start healthy
	svc := &service{base: baseRTT, parallel: &parallel}

	lim := agilelimiter.NewConcurrency(agilelimiter.ConcurrencyConfig{
		MinLimit:        10,  // never collapse below this many concurrent
		MaxLimit:        500, // hard ceiling we are willing to serve
		InitialLimit:    20,  // cold-start low, let the controller ramp up
		SLOTarget:       500 * time.Millisecond,
		Tick:            tick,
		FastHalfLife:    300 * time.Millisecond,
		SlowHalfLife:    3 * time.Second,
		TargetInflation: 0.5, // tolerate handling time up to 1.5x baseline
	})
	lim.Start()
	defer lim.Stop()

	var served, rejected atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Do admits the request if there is room, times the handler, and
				// feeds the handling time back to the controller. When the limit
				// is reached it returns ErrOverloaded at once — no queuing.
				err := lim.Do(func() error {
					served.Add(1)
					time.Sleep(svc.handlerRTT(lim.InFlight()))
					return nil
				})
				if errors.Is(err, agilelimiter.ErrOverloaded) {
					rejected.Add(1)
					// A real client backs off on a 429; without this the swarm
					// of refused callers would busy-loop and burn the very CPU
					// the limiter is trying to protect.
					time.Sleep(2 * time.Millisecond)
				}
			}
		}()
	}

	// Phase schedule: degrade the service, then let it recover.
	go func() {
		time.Sleep(3 * time.Second)
		parallel.Store(60) // service slows down
		time.Sleep(3 * time.Second)
		parallel.Store(150) // service recovers
	}()

	fmt.Println("t      capacity   limit   in-flight   baseline    fast")
	start := time.Now()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for i := 0; i < int(reportFor/time.Second); i++ {
		<-t.C
		st := lim.Stats()
		fmt.Printf("%-6s %-10d %-7d %-11d %-11s %s\n",
			fmt.Sprintf("%ds", int(time.Since(start).Seconds())),
			parallel.Load(),
			lim.Limit(),
			lim.InFlight(),
			st.Baseline.Round(time.Millisecond),
			st.Fast.Round(time.Millisecond),
		)
	}

	close(stop)
	wg.Wait()

	st := lim.Stats()
	fmt.Printf("\nfinal: limit=%d concurrent, baseline=%s, fast=%s\n",
		lim.Limit(), st.Baseline.Round(time.Millisecond), st.Fast.Round(time.Millisecond))
	fmt.Printf("served=%d refused=%d (refusals are shed load, not a failure)\n",
		served.Load(), rejected.Load())
	fmt.Println("the controller settled near the point where handling time starts to climb,")
	fmt.Println("not at MaxLimit — that is the whole idea: it protects this service.")
}
