// Command compare runs agilelimiter and go.uber.org/ratelimit head-to-head
// against the same simulated downstream, whose capacity changes over time.
//
// The two limiters answer different questions. uber's is a *fixed-rate* leaky
// bucket: you hand it a rate once and it paces exactly that. agilelimiter is an
// *adaptive* one: you give it a ceiling and it discovers the rate from the
// downstream's latency. So the interesting cases are the ones where a single
// fixed rate cannot stay good — an unknown capacity, a step change, a daytime
// capacity curve. A steady, known capacity is deliberately included too, to show
// where uber is every bit as good (and simpler).
//
// The downstream model: latency = base/(1 - util), util = offered/capacity. Below
// capacity it answers in `base`; as offered approaches capacity, queuing makes it
// slower and slower (an M/M/1-flavoured wait). A request counts toward "goodput"
// when it completes within the SLO.
//
// Run it with:
//
//	cd compare && go run .
package main

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"agilelimiter"

	uberratelimit "go.uber.org/ratelimit"
)

const (
	baseRTT   = 20 * time.Millisecond // downstream latency when idle
	slo       = 100 * time.Millisecond
	runFor    = 9 * time.Second
	sloString = "100ms"
)

// maxRTT is the worst modelled response time (util is clamped to 0.95), used to
// size worker pools to Little's Law: you need ~rate x rtt workers to sustain a
// rate, and no more — an oversized pool just piles workers up inside Take() and
// makes shutdown wait for their next grant.
const maxRTT = 400 * time.Millisecond

// downstream models a dependency whose response time grows as the offered rate
// approaches its capacity, and whose idle response time may itself change over
// the run — a downstream that got slower without anybody adding load.
type downstream struct {
	base func(time.Duration) time.Duration
	// wait maps utilization (0..1) to a latency multiplier; nil means mm1.
	wait func(util float64) float64
	// slo is the contract a request is scored against. Zero means the package
	// default, which is what every case in main.go uses.
	slo time.Duration
	// shape, when set, replaces base*wait and is called once per request with a
	// per-request sequence number. It exists so the pollution experiments can run
	// two populations — a cache hit and a miss — through one limiter.
	shape func(util float64, t time.Duration, seq uint64) time.Duration
}

// mm1 is 1/(1-util), clamped so saturation stays finite instead of dividing by
// zero. It is the curve every case below uses: latency is already 20x the idle
// value at 95% utilization.
func mm1(util float64) float64 {
	if util > 0.95 {
		util = 0.95
	}
	if util < 0 {
		util = 0
	}
	return 1 / (1 - util)
}

func (d downstream) rtt(offered, capacity float64, t time.Duration, seq uint64) time.Duration {
	util := offered / capacity
	if d.shape != nil {
		return d.shape(util, t, seq)
	}
	wait := d.wait
	if wait == nil {
		wait = mm1
	}
	return time.Duration(float64(d.base(t)) * wait(util))
}

// steadyBase is an idle response time that never changes.
func steadyBase(d time.Duration) func(time.Duration) time.Duration {
	return func(time.Duration) time.Duration { return d }
}

// limiter is the common shape we compare across. Both adapters below present it.
type limiter interface {
	take() time.Time
	observe(time.Duration) // only agilelimiter uses the feedback; uber ignores it
	rate() float64
}

// --- go.uber.org/ratelimit adapter: fixed rate, no feedback ---

type uberLim struct {
	l     uberratelimit.Limiter
	rateF float64
}

func (u *uberLim) take() time.Time       { return u.l.Take() }
func (u *uberLim) observe(time.Duration) {}
func (u *uberLim) rate() float64         { return u.rateF }

func newUber(rate int) func() (limiter, func()) {
	return func() (limiter, func()) {
		return &uberLim{l: uberratelimit.New(rate), rateF: float64(rate)}, func() {}
	}
}

// --- agilelimiter adapter: adaptive rate ---

type agileLim struct{ l *agilelimiter.Limiter }

func (a *agileLim) take() time.Time         { return a.l.Take() }
func (a *agileLim) observe(d time.Duration) { a.l.Observe(d) }
func (a *agileLim) rate() float64           { return a.l.Rate() }

func newAgile() (limiter, func()) {
	l := agilelimiter.New(agilelimiter.Config{
		MinRate:         5,
		MaxRate:         500,
		InitialRate:     40,                     // cold-start low, let the controller ramp up
		SLOTarget:       100 * time.Millisecond, // == the SLO we are scored against; the tail guard protects it directly
		FastHalfLife:    750 * time.Millisecond, // responsive to recent latency
		SlowHalfLife:    45 * time.Second,       // healthy baseline (the library default: long and stable)
		TargetInflation: 2.5,                    // tolerate latency up to 3.5x baseline (~70ms), comfortably under the SLO
		Tick:            250 * time.Millisecond, // UpStep/Tick governs how fast it probes; keep it gentle
	})
	l.Start()
	return &agileLim{l: l}, l.Stop
}

type result struct {
	label     string
	achieved  float64 // completed requests per second
	goodput   float64 // requests completed within the SLO, per second
	okPct     float64
	mean, p99 time.Duration
	finalRate float64
}

// run drives one limiter against one capacity schedule for `dur` and collects
// the outcome. `pool` is the worker count.
func run(label string, mk func() (limiter, func()), sched func(time.Duration) float64, dur time.Duration, pool int) result {
	return runOn(label, mk, downstream{base: steadyBase(baseRTT)}, sched, dur, pool)
}

// runOn is run with a downstream of your own, so the idle latency (and the
// shape of the queueing curve) can change over the run.
func runOn(label string, mk func() (limiter, func()), ds downstream, sched func(time.Duration) float64, dur time.Duration, pool int) result {
	lim, cleanup := mk()
	defer cleanup()
	target := ds.slo
	if target == 0 {
		target = slo
	}

	var mu sync.Mutex
	var completed, ok int
	var sum time.Duration
	var lats []time.Duration

	begin := time.Now()
	deadline := begin.Add(dur)

	var wg sync.WaitGroup
	var seq atomic.Uint64
	for i := 0; i < pool; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				lim.take()
				start := time.Now()
				t := start.Sub(begin)
				rtt := ds.rtt(lim.rate(), sched(t), t, seq.Add(1))
				time.Sleep(rtt)
				elapsed := time.Since(start)
				lim.observe(elapsed)

				mu.Lock()
				completed++
				sum += elapsed
				lats = append(lats, elapsed)
				if elapsed <= target {
					ok++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	res := result{label: label, achieved: float64(completed) / dur.Seconds()}
	if completed > 0 {
		res.goodput = float64(ok) / dur.Seconds()
		res.okPct = 100 * float64(ok) / float64(completed)
		res.mean = sum / time.Duration(completed)
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		res.p99 = lats[int(float64(len(lats)-1)*0.99)]
	}
	res.finalRate = lim.rate()
	return res
}

func report(title string, rows []result) {
	fmt.Printf("\n%s\n", title)
	fmt.Printf("  %-14s %11s %11s %9s %8s %8s %10s\n",
		"limiter", "achieved/s", "goodput/s", "%<=SLO", "mean", "p99", "finalRate")
	for _, r := range rows {
		fmt.Printf("  %-14s %11.0f %11.0f %8.0f%% %8s %8s %10.0f\n",
			r.label, r.achieved, r.goodput, r.okPct,
			r.mean.Round(time.Millisecond), r.p99.Round(time.Millisecond), r.finalRate)
	}
}

// uberRow runs uber at a fixed rate with a pool sized to sustain it.
func uberRow(rate int, sched func(time.Duration) float64, dur time.Duration) result {
	pool := rate / 2 // >= rate*maxRTT, with margin
	if pool < 12 {
		pool = 12
	}
	return run(fmt.Sprintf("uber(%d)", rate), newUber(rate), sched, dur, pool)
}

// agileRow runs agilelimiter with a pool sized for its ceiling.
func agileRow(sched func(time.Duration) float64, dur time.Duration) result {
	return run("agile", newAgile, sched, dur, 160)
}

func main() {
	fmt.Println("agilelimiter vs go.uber.org/ratelimit — head-to-head")
	fmt.Printf("simulated downstream: idle latency %s, SLO %s, latency = base/(1-util)\n", baseRTT, sloString)
	fmt.Printf("each run: %s, worker pool sized to the limiter's rate\n", runFor)

	// A capacity that is constant but that you do NOT know. The three uber rows
	// are three guesses: too low, about right, far too high. ("about right" is the
	// best you can do if you already know the answer — see Case 2 for why that is
	// not a luxury you usually have.)
	steady := func(time.Duration) float64 { return 150 }
	report("Case 1 —steady but UNKNOWN capacity: 150 req/s",
		[]result{
			uberRow(60, steady, runFor),  // guessed too low: safe, but wastes most of the capacity
			uberRow(100, steady, runFor), // guessed (by luck) about right: the best a fixed rate can do
			uberRow(300, steady, runFor), // guessed too high: near-constant overload
			agileRow(steady, runFor),     // never told the capacity
		})

	// A step change: 150 -> 60 -> 30 req/s. uber(120) was tuned for the healthy
	// phase; uber(24) is tuned for the *worst* phase and stays safe but wastes the
	// healthy phase. No fixed rate is good throughout.
	step := func(t time.Duration) float64 {
		switch {
		case t < 2*time.Second:
			return 150
		case t < 4*time.Second:
			return 60
		default:
			return 30
		}
	}
	report("Case 2 —STEP degradation: 150 -> 60 -> 30 req/s at t=2s, 4s",
		[]result{
			uberRow(120, step, runFor),
			uberRow(24, step, runFor),
			agileRow(step, runFor),
		})

	// A diurnal curve: capacity oscillates 40..160 req/s over the run.
	sine := func(t time.Duration) float64 {
		return 100 + 60*math.Sin(2*math.Pi*t.Seconds()/6.0)
	}
	report("Case 3 —DIURNAL capacity: 100 +/- 60 req/s over a 6s period",
		[]result{
			uberRow(80, sine, runFor),
			uberRow(32, sine, runFor),
			agileRow(sine, runFor),
		})

	fmt.Println("\ngoodput/s = requests completing within the SLO each second (the business metric).")
	fmt.Println("A fixed rate must be tuned for the healthy phase (and melts when the")
	fmt.Println("downstream slows) or for the worst phase (and leaves capacity idle the")
	fmt.Println("rest of the time); it cannot do both. The adaptive limiter tracks whichever")
	fmt.Println("capacity is currently true.")
}
