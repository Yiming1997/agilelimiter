// Package torture holds adversarial, long-tail stress tests for the inbound side
// of the library.
//
// These are end-to-end tests over real HTTP: a real net/http server behind the
// real httpserver middleware, driven by real HTTP clients. The handler sleeps to
// imitate I/O, so the durations the controller sees come from the same code path
// an application would use — not from a simulation of one.
//
// The point of the package is a question the unit tests cannot ask: when a small
// fraction of requests is dramatically slower than the rest, does the limiter
// throttle the healthy majority?
package torture

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Yiming1997/agilelimiter"
	"github.com/Yiming1997/agilelimiter/httpserver"
)

// ---------------------------------------------------------------------------
// the service under test
// ---------------------------------------------------------------------------

// workload describes what the handler does for one request.
type workload struct {
	// base is the I/O time every request pays, imitated with time.Sleep.
	base time.Duration
	// tailEvery makes one request in every tailEvery pay `tail` instead of
	// `base`; zero means no tail at all.
	tailEvery int
	tail      time.Duration
}

// pick returns the I/O duration for the seq'th request.
func (w workload) pick(seq int) time.Duration {
	if w.tailEvery > 0 && seq%w.tailEvery == 0 {
		return w.tail
	}
	return w.base
}

func (w workload) label() string {
	if w.tailEvery == 0 {
		return fmt.Sprintf("base %v", w.base)
	}
	return fmt.Sprintf("base %v, 1/%d at %v", w.base, w.tailEvery, w.tail)
}

// tailShare is the fraction of traffic the cohort represents.
func (w workload) tailShare() float64 {
	if w.tailEvery <= 0 {
		return 0
	}
	return 1 / float64(w.tailEvery)
}

// newService builds the HTTP server under test: a handler that sleeps, wrapped
// in the real limiter middleware.
//
// The handler stamps a response header with its own verdict about the request,
// so the client can attribute each response without guessing. Send-order tagging
// was tried first and is wrong: the client's counter and the handler's counter
// advance independently once requests overlap, and a 250ms response arrives long
// after the requests that came in behind it. Measured, that mis-attributed 79 of
// 1985 "healthy" responses -- all of them exactly 250ms, i.e. real tail requests.
func newService(
	cfg agilelimiter.ConcurrencyConfig,
	w workload,
	seq *atomic.Int64,
) (*httptest.Server, *http.Client, *agilelimiter.ConcurrencyLimiter) {
	lim := agilelimiter.NewConcurrency(cfg)

	handler := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		n := int(seq.Add(1))
		isTail := w.tailEvery > 0 && n%w.tailEvery == 0
		// Tell the client what this request was. The client cannot infer it.
		if isTail {
			rw.Header().Set(tailHeader, "1")
		}
		time.Sleep(w.pick(n))
		rw.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(rw, "ok")
	})

	srv := httptest.NewServer(httpserver.Wrap(handler, lim))
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        2000,
			MaxIdleConnsPerHost: 2000,
			MaxConnsPerHost:     0,
		},
		Timeout: 30 * time.Second,
	}
	return srv, client, lim
}

// tailHeader marks a response as belonging to the tail cohort.
const tailHeader = "X-Torture-Tail"

// ---------------------------------------------------------------------------
// paced load generation
// ---------------------------------------------------------------------------

// httpResult aggregates one run.
type httpResult struct {
	label string

	ok200  int64
	shed   int64 // 429 from the middleware
	errors int64
	total  int64

	// Latency of requests answered with 200, in ms. A 429 is a deliberate shed,
	// not a slow response, so it is excluded.
	latencies []float64
	// isTail[i] reports whether latencies[i] was a tail-cohort request.
	//
	// This has to be recorded at send time, aligned with the handler's own
	// sequence number, rather than inferred later from the slice index. The
	// handler numbers requests on arrival; the client appends on completion, and
	// a 250ms request finishes after many 50ms ones, so index and arrival order
	// differ. Filtering by index was off by ~5 percentage points in measurement.
	isTail []bool

	finalLimit float64
	meanLimit  float64
	minLimit   float64

	elapsed time.Duration
}

func (r httpResult) pctOf(n int64) float64 {
	if r.total == 0 {
		return 0
	}
	return 100 * float64(n) / float64(r.total)
}

func (r httpResult) shedPct() float64 { return r.pctOf(r.shed) }

// okPct is the fraction of ALL attempts answered inside the SLO.
func (r httpResult) okPct(slo time.Duration) float64 {
	if r.total == 0 {
		return 0
	}
	sloMS := float64(slo) / float64(time.Millisecond)
	within := int64(0)
	for _, ms := range r.latencies {
		if ms <= sloMS {
			within++
		}
	}
	return 100 * float64(within) / float64(r.total)
}

// healthyOkPct is okPct restricted to requests that were NOT in the tail cohort.
//
// Membership is read from isTail, which the harness records at send time using
// the same sequence the handler uses. Do NOT infer it from the slice index: the
// client appends on completion and a slow request lands later than it arrived,
// so index order and arrival order differ exactly because of the tail.
func (r httpResult) healthyOkPct(slo time.Duration) float64 {
	sloMS := float64(slo) / float64(time.Millisecond)
	within, total := 0, 0
	for i, ms := range r.latencies {
		if i < len(r.isTail) && r.isTail[i] {
			continue // a tail request
		}
		total++
		if ms <= sloMS {
			within++
		}
	}
	if total == 0 {
		return 0
	}
	return 100 * float64(within) / float64(total)
}

func (r httpResult) latency(q float64) float64 {
	if len(r.latencies) == 0 {
		return 0
	}
	s := append([]float64(nil), r.latencies...)
	sort.Float64s(s)
	i := int(math.Ceil(float64(len(s))*q)) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(s) {
		i = len(s) - 1
	}
	return s[i]
}

func meanOf(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := 0.0
	for _, v := range xs {
		s += v
	}
	return s / float64(len(xs))
}

func (r httpResult) String() string {
	return fmt.Sprintf(
		"%s | total=%d 200=%d(%.0f%%) 429=%d(%.0f%%) err=%d | "+
			"limit final=%.0f mean=%.0f min=%.0f | "+
			"lat(200) mean=%.0fms p50=%.0fms p99=%.0fms max=%.0fms | rps=%.0f",
		r.label, r.total, r.ok200, r.pctOf(r.ok200),
		r.shed, r.shedPct(), r.errors,
		r.finalLimit, r.meanLimit, r.minLimit,
		meanOf(r.latencies), r.latency(0.50), r.latency(0.99), r.latency(1.0),
		float64(r.total)/r.elapsed.Seconds(),
	)
}

// drivePaced sends requests at a fixed rate for `dur`, using `workers`
// concurrent clients.
//
// Pacing is the whole point. An unpaced loop (each client sending as fast as it
// can) makes the offered load unbounded, so the shed rate merely reflects
// "clients are faster than the service" and says nothing about whether the tail
// was misread. With a fixed arrival rate that the service can actually carry,
// every 429 is the limiter's own decision.
//
// Cohort membership is read from the response header the handler stamps, never
// inferred from the client's own counters: see newService.
func drivePaced(
	client *http.Client,
	url string,
	lim *agilelimiter.ConcurrencyLimiter,
	ratePerSec float64,
	workers int,
	dur time.Duration,
) httpResult {
	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()

	var (
		mu     sync.Mutex
		res    httpResult
		limits []float64
		minLim = math.Inf(1)
	)

	// Sample the limit while the load runs.
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
				mu.Lock()
				v := float64(lim.Limit())
				limits = append(limits, v)
				if v < minLim {
					minLim = v
				}
				mu.Unlock()
			}
		}
	}()

	// Each worker sends one request every `interval`; together they make up the
	// requested rate.
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
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
				if err != nil {
					return
				}
				resp, err := client.Do(req)
				if err != nil {
					if ctx.Err() != nil {
						return // our own deadline
					}
					mu.Lock()
					res.total++
					res.errors++
					mu.Unlock()
					continue
				}
				// The handler stamps the cohort; read it before the body.
				isTail := resp.Header.Get(tailHeader) != ""
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				lat := time.Since(began)

				mu.Lock()
				res.total++
				switch resp.StatusCode {
				case http.StatusOK:
					res.ok200++
					res.latencies = append(res.latencies,
						float64(lat)/float64(time.Millisecond))
					res.isTail = append(res.isTail, isTail)
				case http.StatusTooManyRequests:
					res.shed++
				default:
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

	res.finalLimit = float64(lim.Limit())
	res.minLimit = minLim
	mean := 0.0
	for _, v := range limits {
		mean += v
	}
	if len(limits) > 0 {
		mean /= float64(len(limits))
	}
	res.meanLimit = mean
	return res
}

// ---------------------------------------------------------------------------
// configuration
// ---------------------------------------------------------------------------

// tortureCfg is the inbound policy under test.
//
// Tick is 50ms so a six-second run exercises ~120 control steps. SLOTarget is
// 150ms: the handler's base I/O is 50ms, so a healthy request has 3x headroom,
// and a 250ms tail is clearly over the line.
func tortureCfg() agilelimiter.ConcurrencyConfig {
	return agilelimiter.ConcurrencyConfig{
		MinLimit:     1,
		FloorLimit:   10,
		ProbeLimit:   5,
		MaxLimit:     400,
		InitialLimit: 20,

		SLOTarget: 150 * time.Millisecond,

		SlowHalfLife:     2 * time.Second,
		SlowDownHalfLife: 500 * time.Millisecond,
		Tick:             50 * time.Millisecond,
	}
}

// ---------------------------------------------------------------------------
// the first torture case
// ---------------------------------------------------------------------------

// TestLongTailDoesNotThrottleTheMajority is the first end-to-end torture case.
//
// The service is healthy: every request pays 50ms of imitated I/O and the SLO is
// 150ms, so a healthy request has 3x headroom. One request in twenty is a report
// endpoint that costs 250ms — five times the base, above the SLO, and completely
// independent of how much load is arriving.
//
// # Why the load is paced
//
// An unpaced client loop offers unbounded load, so the shed rate just says
// "clients outrun the service" and proves nothing about the tail. Here arrivals
// are paced to 600 req/s. Carrying 600 req/s at 50ms needs ~30 requests in
// flight; the limiter starts at 20 and should climb to roughly that. A 5% tail
// cannot legitimately require throttling the other 95%.
//
// # What is asserted
//
// The 5% cohort can never meet the SLO; that is what it is. So the assertions
// are about the healthy 95%, and about the limiter's own behaviour:
//
//   - the healthy majority keeps being served and stays inside the SLO;
//   - the limiter does not shed a large share of the offered load;
//   - the limit does not collapse toward the floor.
func TestLongTailDoesNotThrottleTheMajority(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end load test")
	}

	const (
		rate    = 600.0 // requests per second offered
		workers = 40
		dur     = 6 * time.Second
		slo     = 150 * time.Millisecond
	)

	// Reference: no tail at all.
	clean := workload{base: 50 * time.Millisecond}
	// Torture: 5% of traffic costs 250ms.
	tailed := workload{base: 50 * time.Millisecond, tailEvery: 20, tail: 250 * time.Millisecond}

	run := func(w workload, tol float64) httpResult {
		cfg := tortureCfg()
		cfg.TailTolerance = tol
		var seq atomic.Int64
		srv, client, lim := newService(cfg, w, &seq)
		defer srv.Close()
		defer lim.Stop()
		lim.Start()

		res := drivePaced(client, srv.URL, lim, rate, workers, dur)
		res.label = w.label()
		return res
	}

	cleanRes := run(clean, 0.05)
	tailRes := run(tailed, 0.05)

	t.Logf("offered=%.0f req/s  SLO=%v  control tick=%v", rate, slo, tortureCfg().Tick)
	t.Logf("clean  | %s", cleanRes)
	t.Logf("tailed | %s", tailRes)
	t.Logf("healthy-majority inside SLO: clean %.1f%%, tailed %.1f%%",
		cleanRes.healthyOkPct(slo), tailRes.healthyOkPct(slo))

	// 1. The tail must not cost the healthy majority its SLO.
	if got := tailRes.healthyOkPct(slo); got < 90 {
		t.Errorf("only %.1f%% of healthy requests met the SLO with a 5%% tail "+
			"(clean run: %.1f%%)", got, cleanRes.healthyOkPct(slo))
	}

	// 2. The limiter must not shed much more than the tail's own share. A little
	//    more is expected: the cohort occupies slots for 250ms, so a fixed number
	//    of slots carries slightly less throughput.
	if tailRes.shedPct() > 30 {
		t.Errorf("shed %.1f%% of a %.0f req/s offer for a 5%% tail (clean run shed "+
			"%.1f%%): the fixed cohort is being read as congestion",
			tailRes.shedPct(), rate, cleanRes.shedPct())
	}

	// 3. The limit must not collapse toward the floor.
	if tailRes.finalLimit < 10 {
		t.Errorf("limit collapsed to %.0f (floor is 10) under a 5%% tail",
			tailRes.finalLimit)
	}

	// 4. The healthy majority must actually have been served, or the run proves
	//    nothing.
	if tailRes.ok200 < 200 {
		t.Fatalf("only %d successful responses; the harness is broken, not the limiter",
			tailRes.ok200)
	}
}
