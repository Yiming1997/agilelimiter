// Command httpserver runs a tiny net/http service whose /api/* routes are
// protected by an adaptive concurrency limiter, then hammers it with a burst of
// concurrent clients. Watch the "limit" column: the controller discovers how
// many requests the handler can serve at once without its latency blowing past
// the SLO, and requests beyond that are shed (429) instead of queued.
//
// The server runs on an httptest listener — no fixed port, no external network.
//
//	go run ./example/httpserver
package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"time"

	"agilelimiter"
	"agilelimiter/httpserver"
)

func main() {
	const (
		parallel = 40                    // requests the toy handler serves concurrently at `base`
		base     = 20 * time.Millisecond // idle handling time
		slo      = 100 * time.Millisecond
		callers  = 250
		runFor   = 8 * time.Second
	)

	// Toy handler: the more requests run at once, the slower each one gets.
	var inflight atomic.Int64
	work := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := inflight.Add(1)
		defer inflight.Add(-1)
		mult := 1.0
		if float64(n) > parallel {
			mult = float64(n) / parallel
		}
		time.Sleep(time.Duration(float64(base) * mult))
		_, _ = io.WriteString(w, "ok")
	})

	lim := agilelimiter.NewConcurrency(agilelimiter.ConcurrencyConfig{
		MinLimit:     5,
		MaxLimit:     200,
		InitialLimit: 10,
		SLOTarget:    slo,
		FastHalfLife: 500 * time.Millisecond,
		SlowHalfLife: 30 * time.Second,
		Tick:         100 * time.Millisecond,
	})
	lim.Start()
	defer lim.Stop()

	root := http.NewServeMux()
	root.Handle("/api/", httpserver.Wrap(work, lim, httpserver.WithRetryAfter(time.Second)))
	root.Handle("/debug/limit", httpserver.StatsHandler(lim)) // outside the limiter: readable under load

	srv := httptest.NewServer(root)
	defer srv.Close()

	fmt.Printf("hammering %s with %d concurrent clients for %s\n", srv.URL, callers, runFor)
	fmt.Printf("handler: %s idle, ~%d in parallel; SLO %s\n\n", base, parallel, slo)

	var served, shed atomic.Int64
	deadline := time.Now().Add(runFor)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := srv.Client()
			for time.Now().Before(deadline) {
				resp, err := client.Get(srv.URL + "/api/work")
				if err != nil {
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				switch resp.StatusCode {
				case http.StatusOK:
					served.Add(1)
				case http.StatusTooManyRequests:
					shed.Add(1)
					time.Sleep(30 * time.Millisecond) // back off on 429 so clients don't spin
				}
			}
		}()
	}

	// Report the discovered limit a few times a second.
	start := time.Now()
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				fmt.Printf("%6s  limit=%3d  inflight=%3d  served=%-6d shed=%d\n",
					time.Since(start).Round(100*time.Millisecond),
					lim.Limit(), lim.InFlight(), served.Load(), shed.Load())
			}
		}
	}()

	wg.Wait()
	close(done)

	servedN, shedN := served.Load(), shed.Load()
	total := servedN + shedN
	fmt.Printf("\ntotal=%d served=%d (%.0f%%) shed=%d (%.0f%%)\n",
		total, servedN, 100*float64(servedN)/float64(total), shedN, 100*float64(shedN)/float64(total))
	fmt.Println("shed requests are refused fast (429) — that refusal is what keeps the process healthy.")
}
