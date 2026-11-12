package torture

import (
	"context"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Yiming1997/agilelimiter"
	"github.com/Yiming1997/agilelimiter/httpclient"
)

// ---------------------------------------------------------------------------
// the outbound service under test
// ---------------------------------------------------------------------------

// originCapacity is the downstream saturation concurrency where throughput plateaus.
const originCapacity = 10

// newOrigin builds a downstream whose handling time increases with concurrency.
func newOrigin(base time.Duration) *httptest.Server {
	var inflight atomic.Int64
	return httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		n := inflight.Add(1)
		defer inflight.Add(-1)
		time.Sleep(base + time.Duration(int64(base)*(n-1)/originCapacity))
		rw.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(rw, "ok")
	}))
}

// countingRT tracks active in-flight round trips and start timestamps.
type countingRT struct {
	next http.RoundTripper
	n    atomic.Int64

	mu     sync.Mutex
	starts []time.Time
}

func (c *countingRT) RoundTrip(r *http.Request) (*http.Response, error) {
	c.n.Add(1)
	defer c.n.Add(-1)
	c.mu.Lock()
	c.starts = append(c.starts, time.Now())
	c.mu.Unlock()
	return c.next.RoundTrip(r)
}

// marks copies the start stamps taken so far.
func (c *countingRT) marks() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Time(nil), c.starts...)
}

// outboundCfg configures the rate limiter for the silence test.
func outboundCfg() agilelimiter.Config {
	return agilelimiter.Config{
		MinRate:     1,
		FloorRate:   10,
		ProbeRate:   5,
		MaxRate:     1000,
		InitialRate: 50,

		SLOTarget: 150 * time.Millisecond,

		SlowHalfLife:     2 * time.Second,
		SlowDownHalfLife: 500 * time.Millisecond,
		Tick:             50 * time.Millisecond,
		DownStep:         0.30,
		SlowStartGain:    1,
	}
}

// driveRate offers ratePerSec requests for dur through the outbound limiter,
// keeping context alive beyond the window to allow graceful draining.
func driveRate(
	client *http.Client,
	url string,
	lim *agilelimiter.Limiter,
	ratePerSec float64,
	workers int,
	dur time.Duration,
) httpResult {
	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()
	reqCtx, reqCancel := context.WithTimeout(context.Background(), dur+30*time.Second)
	defer reqCancel()

	var (
		mu      sync.Mutex
		res     httpResult
		rates   []float64
		minRate = math.Inf(1)
	)

	// Sample the rate while the load runs: it sawtooths around the knee, so the
	// mean and the trough describe the phase better than its last instant does.
	stopSampling := make(chan struct{})
	var sWG sync.WaitGroup
	sWG.Add(1)
	go func() {
		defer sWG.Done()
		t := time.NewTicker(5 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stopSampling:
				return
			case <-t.C:
				v := lim.Rate()
				mu.Lock()
				rates = append(rates, v)
				if v < minRate {
					minRate = v
				}
				mu.Unlock()
			}
		}
	}()

	interval := time.Duration(float64(time.Second) * float64(workers) / ratePerSec)
	start := time.Now()

	var loadWG sync.WaitGroup
	for w := 0; w < workers; w++ {
		loadWG.Add(1)
		go func() {
			defer loadWG.Done()
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}

				began := time.Now()
				req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
				if err != nil {
					return
				}
				resp, err := client.Do(req)
				if err != nil {
					// reqCtx deliberately outlives the offering window, so an error
					// after ctx.Done is still a real failure of an issued request.
					// Always record it instead of silently dropping drain-phase errors.
					mu.Lock()
					res.total++
					res.errors++
					mu.Unlock()
					if ctx.Err() != nil {
						return
					}
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				lat := time.Since(began)

				mu.Lock()
				res.total++
				if resp.StatusCode == http.StatusOK {
					res.ok200++
					// Includes the throttle wait: callers queue in Take instead of
					// being refused, so this is what the caller suffered, not the
					// round trip the controller sees.
					res.latencies = append(res.latencies,
						float64(lat)/float64(time.Millisecond))
				} else {
					res.errors++
				}
				mu.Unlock()
			}
		}()
	}
	loadWG.Wait()
	res.elapsed = time.Since(start)

	close(stopSampling)
	sWG.Wait()

	res.finalLimit = lim.Rate()
	res.minLimit = minRate
	mean := 0.0
	for _, v := range rates {
		mean += v
	}
	if len(rates) > 0 {
		mean /= float64(len(rates))
	}
	res.meanLimit = mean
	return res
}

// probeBurst sends originCapacity calls concurrently to verify the bucket
// spaces grants properly instead of releasing banked idle credit all at once.
func probeBurst(t *testing.T, client *http.Client, rt *countingRT, url string, rate float64) {
	t.Helper()
	const n = originCapacity
	before := len(rt.marks())
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := client.Get(url)
			if err != nil {
				t.Errorf("probe call %d failed: %v", i, err)
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("probe call %d answered %d", i, resp.StatusCode)
			}
		}(i)
	}
	wg.Wait()

	stamps := rt.marks()[before:]
	if len(stamps) != n {
		t.Fatalf("probe recorded %d sends, want %d", len(stamps), n)
	}
	lo, hi := stamps[0], stamps[0]
	for _, s := range stamps[1:] {
		if s.Before(lo) {
			lo = s
		}
		if s.After(hi) {
			hi = s
		}
	}
	span := hi.Sub(lo)
	ideal := time.Duration(float64(n-1) / rate * float64(time.Second))
	t.Logf("burst  | %d calls at once, granted over %v (ideal %v at %.0f req/s)", n, span, ideal, rate)
	if span < ideal/2 {
		t.Errorf("%d calls after the silence were granted within %v, want about %v: "+
			"the bucket handed out credit it saved up while idle", n, span, ideal)
	}
}

// ---------------------------------------------------------------------------
// outbound: the request rate
// ---------------------------------------------------------------------------

// TestSilenceDoesNotDecayTheRate verifies that zero traffic does not decay
// the outbound request rate and that idle credit is not banked into a burst.
// Phases: A (load ramp) -> B (drain) -> C (silence) -> D (burst probe) -> E (resumed load).
func TestSilenceDoesNotDecayTheRate(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end load test")
	}

	const (
		rate    = 600.0 // requests per second offered
		workers = 40
		phase   = 3 * time.Second
	)

	cfg := outboundCfg()
	origin := newOrigin(50 * time.Millisecond)
	defer origin.Close()

	lim := agilelimiter.New(cfg)
	defer lim.Stop()
	lim.Start()

	rt := &countingRT{next: &http.Transport{
		MaxIdleConns:        2000,
		MaxIdleConnsPerHost: 2000,
		MaxConnsPerHost:     0,
	}}
	client := &http.Client{Transport: httpclient.Wrap(rt, lim), Timeout: 30 * time.Second}

	subject := idleSubject{
		quota: lim.Rate,
		busy:  func() int { return int(rt.n.Load()) },
		stats: lim.Stats,
	}

	// A: Load phase to find downstream knee and establish a rate.
	load := driveRate(client, origin.URL, lim, rate, workers, phase)
	load.label = "load"
	t.Logf("load   | %s", load)

	if load.ok200 < 120 {
		t.Fatalf("only %d calls got through under load; the harness is broken, not the limiter",
			load.ok200)
	}
	if load.errors != 0 {
		t.Fatalf("load phase had %d transport or HTTP errors; fix the harness before judging capacity",
			load.errors)
	}
	if load.meanLimit <= float64(cfg.InitialRate) {
		t.Fatalf("rate never grew past its initial %d under %.0f req/s; there is nothing to hold",
			cfg.InitialRate, rate)
	}
	if load.meanLimit >= float64(cfg.MaxRate) {
		t.Fatalf("rate ran to MaxRate %d: the downstream never congested, so holding it proves nothing",
			cfg.MaxRate)
	}

	// B: Wait for in-flight requests to drain.
	if !waitIdle(subject, cfg.Tick, 4*cfg.Tick+time.Second) {
		t.Fatalf("calls never drained: inflight=%d samples=%d",
			rt.n.Load(), lim.Stats().Samples)
	}
	pre := lim.Stats()
	preQuota := lim.Rate()
	if pre.Baseline <= 0 {
		t.Fatal("no baseline was established before the silence; phase A proved nothing")
	}

	// C: Silence phase, sampled twice per control tick.
	every := cfg.Tick / 2
	idle := sampleSilence(subject, phase, every)
	if want := int(phase / every); len(idle) < want/2 {
		t.Fatalf("only %d readings in the silence window, want about %d; the harness is broken",
			len(idle), want)
	}
	first := idle[0]
	t.Logf("silence| %d readings over %v: rate=%.3f baseline=%v",
		len(idle), phase, first.quota, first.baseline)

	holdThroughSilence(t, idle, pre, preQuota, load.minLimit*(1-cfg.DownStep), cfg.FloorRate)

	// D: Verify the preserved rate is enforced and not released in a burst.
	probeBurst(t, client, rt, origin.URL, first.quota)

	// E: Resumed load to verify throughput was preserved.
	resumed := driveRate(client, origin.URL, lim, rate, workers, phase)
	resumed.label = "resumed"
	t.Logf("resumed| %s", resumed)

	if resumed.errors != 0 {
		t.Errorf("resumed phase had %d transport or HTTP errors on an always-healthy origin",
			resumed.errors)
	}
	if resumed.ok200 < load.ok200*7/10 {
		t.Errorf("only %d calls got through after the silence against %d on the cold load: "+
			"the silence cost capacity", resumed.ok200, load.ok200)
	}
	if resumed.meanLimit < load.meanLimit/2 {
		t.Errorf("rate averaged %.0f req/s after the silence against %.0f on the cold load: "+
			"the silence cost capacity", resumed.meanLimit, load.meanLimit)
	}
}
