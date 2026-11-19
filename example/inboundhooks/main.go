// Command inboundhooks is the pull-request demo for inbound hooks.
//
// PR DEMO ONLY — DO NOT MERGE THIS FILE INTO MAIN.
//
// This file is here only to demonstrate the feature added by this PR. It is not
// part of the library and must be removed before the PR is merged into main:
//
//	git rm -r example/inboundhooks
//
// The durable examples stay example/concurrency and example/httpserver, which do
// not use hooks.
//
// What it demonstrates:
//
//	ConcurrencyLimiter.SetHooks
//	agilelimiter.Hooks{Admit, Observe}
//	AdmitHook, ObserveHook, Observation
//
//	go run ./example/inboundhooks
package main

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Yiming1997/agilelimiter"
)

// errPolicy is a caller-side rejection: it is not ErrOverloaded, so the caller
// can tell "your request was not allowed" from "the limiter is full".
var errPolicy = errors.New("demo: caller not allowed")

func main() {
	const (
		workers   = 20
		callsEach = 10
		work      = 2 * time.Millisecond
	)

	lim := agilelimiter.NewConcurrency(agilelimiter.ConcurrencyConfig{
		MinLimit:     10,
		MaxLimit:     80,
		InitialLimit: 40, // above `workers`, so only policy rejects in this demo
	})
	defer lim.Stop() // safe even though Start is not called

	var attempted, policyShed, observed atomic.Int64
	var reached, aborted atomic.Int64
	var total time.Duration
	var mu sync.Mutex

	err := lim.SetHooks(&agilelimiter.Hooks{
		// Admit runs before the semaphore is touched. Inbound admission must stay
		// non-blocking, so the hook rejects rather than queues.
		Admit: []agilelimiter.AdmitHook{
			func() error {
				if attempted.Add(1)%5 == 0 {
					policyShed.Add(1)
					return errPolicy
				}
				return nil
			},
		},
		// Observe runs after the controller has been told, with exactly what it
		// was told: duration, error and classification.
		Observe: []agilelimiter.ObserveHook{
			func(o agilelimiter.Observation) {
				observed.Add(1)
				switch o.Outcome {
				case agilelimiter.Reached:
					reached.Add(1)
				case agilelimiter.Aborted:
					aborted.Add(1)
				}
				mu.Lock()
				total += o.Duration
				mu.Unlock()
			},
		},
	})
	if err != nil {
		panic(err)
	}

	var served, capacityShed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < callsEach; j++ {
				err := lim.Do(func() error {
					served.Add(1)
					time.Sleep(work)
					return nil
				})
				switch {
				case errors.Is(err, errPolicy):
					// already counted in the Admit hook
				case errors.Is(err, agilelimiter.ErrOverloaded):
					capacityShed.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	avg := time.Duration(0)
	if n := observed.Load(); n > 0 {
		avg = total / time.Duration(n)
	}

	fmt.Println("inbound hooks demo (ConcurrencyLimiter.SetHooks)")
	fmt.Printf("  attempted     = %d\n", attempted.Load())
	fmt.Printf("  served        = %d\n", served.Load())
	fmt.Printf("  policy shed   = %d  (Admit returned errPolicy)\n", policyShed.Load())
	fmt.Printf("  capacity shed = %d  (Do returned ErrOverloaded)\n", capacityShed.Load())
	fmt.Printf("  observed      = %d  (Observe hooks == served: %v)\n",
		observed.Load(), observed.Load() == served.Load())
	fmt.Printf("  outcomes      = reached %d / aborted %d\n", reached.Load(), aborted.Load())
	fmt.Printf("  avg Duration  = %s\n", avg.Round(time.Microsecond))
}
