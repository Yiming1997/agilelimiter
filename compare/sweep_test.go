// Sweep experiments: where does agilelimiter beat a fixed-rate limiter
// (go.uber.org/ratelimit), and where does it not?
//
// Two symmetric harnesses, one per deployment side:
//
//	inbound   ConcurrencyLimiter (TryTake/Release, refuses excess) vs uber's
//	          Take() (blocks, so excess demand is queued by the caller).
//	              handling time = base * max(1, inflight/parallel)
//	          Throughput saturates at parallel/base; latency grows linearly past it.
//
//	outbound  Limiter (rate) vs uber's Take(). Both block, so the ONLY thing that
//	          differs is the rate each one chooses. The downstream is the
//	          M/M/1-flavoured model from main.go:
//	              latency = base / (1 - util),  util = target/capacity
//	          where `target` is the rate that arm is currently aiming at. Both
//	          arms block until granted, so both really do offer that rate; using
//	          the target keeps the loop free of sampling lag that would dominate
//	          a run this short. Neither arm is told the capacity.
//
// Methodology notes (these matter for reading the numbers):
//
//   - Every arm runs a 3s SETTLE phase that is not measured; measurement covers
//     the following 6s. A controller that needs tens of seconds to converge
//     (agilelimiter does, by design — probing a rate is dangerous) would
//     otherwise be scored on its ramp.
//   - The adaptive arms are warm-started at InitialRate/InitialLimit = 100,
//     which is neither the true capacity nor the optimum: a neutral start.
//   - The adaptive control loop is deliberately gentle (Tick 250ms, UpStep 5%,
//     FastHalfLife 750ms) — near the library defaults. The first version of
//     this sweep used 10% per 100ms and the loop simply went unstable, which
//     measures the tuning, not the algorithm.
//   - "good%" counts requests whose *service* time met the SLO. Queue time spent
//     blocked in Take() is excluded for both arms, which favours uber: a fixed
//     rate that is too high queues its excess demand in the caller.
//
// Every row is emitted as one pipe-delimited line:
//
//	CSV|kind|scenario|arm|phase|adm/s|good/s|good%|mean_ms|p99_ms|peak|refuse/s|final
//
// Run with:
//
//	go test -run '^TestSweep' -v -timeout 40m .
package main

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agilelimiter"
	uberratelimit "go.uber.org/ratelimit"
)

const (
	sweepRefuse = 2 * time.Millisecond
	// warmStartIn / warmStartOut are neutral starting points: neither the true
	// answer nor the floor. They differ only because a rate and a concurrency
	// limit live on different scales; both are mid-range for their scenarios.
	warmStartIn  = 100
	warmStartOut = 50
)

// ---------------------------------------------------------------------------
// measurement plumbing, shared by both sweeps
// ---------------------------------------------------------------------------

type sweepStats struct {
	adm, good, refus atomic.Int64
	workNanos        atomic.Int64
	peak             atomic.Int64

	mu   sync.Mutex
	lats []time.Duration
}

func (s *sweepStats) store(d time.Duration, slo time.Duration) {
	s.adm.Add(1)
	s.workNanos.Add(int64(d))
	if d <= slo {
		s.good.Add(1)
	}
	s.mu.Lock()
	s.lats = append(s.lats, d)
	s.mu.Unlock()
}

func (s *sweepStats) peakStore(n int) {
	for {
		cur := s.peak.Load()
		if int64(n) <= cur || s.peak.CompareAndSwap(cur, int64(n)) {
			return
		}
	}
}

func (s *sweepStats) quantile(q float64) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.lats) == 0 {
		return 0
	}
	sort.Slice(s.lats, func(i, j int) bool { return s.lats[i] < s.lats[j] })
	return s.lats[int(float64(len(s.lats)-1)*q)]
}

func (s *sweepStats) mean() time.Duration {
	n := s.adm.Load()
	if n == 0 {
		return 0
	}
	return time.Duration(s.workNanos.Load() / n)
}

func mergeStats(list []*sweepStats) *sweepStats {
	out := &sweepStats{}
	for _, s := range list {
		out.adm.Add(s.adm.Load())
		out.good.Add(s.good.Load())
		out.refus.Add(s.refus.Load())
		out.workNanos.Add(s.workNanos.Load())
		if p := s.peak.Load(); p > out.peak.Load() {
			out.peak.Store(p)
		}
		s.mu.Lock()
		out.lats = append(out.lats, s.lats...)
		s.mu.Unlock()
	}
	return out
}

func emit(kind, scen, arm, phase string, secs float64, s *sweepStats, final string) {
	adm := float64(s.adm.Load())
	good := float64(s.good.Load())
	pct := 0.0
	if adm > 0 {
		pct = 100 * good / adm
	}
	fmt.Printf("CSV|%s|%s|%s|%s|%.1f|%.1f|%.1f|%d|%d|%d|%.1f|%s\n",
		kind, scen, arm, phase,
		adm/secs, good/secs, pct,
		s.mean()/time.Millisecond, s.quantile(0.99)/time.Millisecond,
		s.peak.Load(), float64(s.refus.Load())/secs, final)
}

// ---------------------------------------------------------------------------
// inbound: ConcurrencyLimiter vs uber ratelimit
// ---------------------------------------------------------------------------

type inboundLimiter interface {
	acquire() bool
	release()
	observe(time.Duration)
	final() int
	stop()
}

type noInbound struct{}

func (noInbound) acquire() bool         { return true }
func (noInbound) release()              {}
func (noInbound) observe(time.Duration) {}
func (noInbound) final() int            { return -1 }
func (noInbound) stop()                 {}

type uberInbound struct {
	l    uberratelimit.Limiter
	rate int
}

func (u *uberInbound) acquire() bool         { u.l.Take(); return true }
func (u *uberInbound) release()              {}
func (u *uberInbound) observe(time.Duration) {}
func (u *uberInbound) final() int            { return u.rate }
func (u *uberInbound) stop()                 {}

type agileInbound struct{ l *agilelimiter.ConcurrencyLimiter }

func (a *agileInbound) acquire() bool           { return a.l.TryTake() }
func (a *agileInbound) release()                { a.l.Release() }
func (a *agileInbound) observe(d time.Duration) { a.l.Observe(d) }
func (a *agileInbound) final() int              { return a.l.Limit() }
func (a *agileInbound) stop()                   { a.l.Stop() }

type inboundScenario struct {
	name    string
	base    time.Duration
	par     float64
	clients int
	slo     time.Duration

	// A temporary capacity loss: parallel *= drop for dropFor, starting at dropAt.
	drop    float64
	dropAt  time.Duration
	dropFor time.Duration
}

// ceiling is the service's throughput limit in req/s.
func (s inboundScenario) ceiling() float64 { return s.par / s.base.Seconds() }

func (s inboundScenario) parallelAt(elapsed time.Duration) float64 {
	if s.drop > 0 && elapsed >= s.dropAt && elapsed < s.dropAt+s.dropFor {
		return s.par * s.drop
	}
	return s.par
}

func (s inboundScenario) latency(inflight int, elapsed time.Duration) time.Duration {
	c := float64(inflight) / s.parallelAt(elapsed)
	if c < 1 {
		c = 1
	}
	return time.Duration(float64(s.base) * c)
}

func runInbound(l inboundLimiter, sc inboundScenario, phase time.Duration, phases int) []*sweepStats {
	stats := make([]*sweepStats, phases)
	for i := range stats {
		stats[i] = &sweepStats{}
	}
	total := phase * time.Duration(phases)
	start := time.Now()
	var inflight atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < sc.clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				elapsed := time.Since(start)
				if elapsed >= total {
					return
				}
				ph := int(elapsed / phase)
				if ph >= phases {
					ph = phases - 1
				}
				st := stats[ph]

				if !l.acquire() {
					st.refus.Add(1)
					time.Sleep(sweepRefuse)
					continue
				}
				n := inflight.Add(1)
				st.peakStore(int(n))
				begin := time.Now()
				time.Sleep(sc.latency(int(n), elapsed))
				d := time.Since(begin)
				inflight.Add(-1)

				l.release()
				l.observe(d)
				st.store(d, sc.slo)
			}
		}()
	}
	wg.Wait()
	return stats
}

func uberInArm(rate int) func() inboundLimiter {
	return func() inboundLimiter {
		return &uberInbound{l: uberratelimit.New(rate, uberratelimit.WithoutSlack), rate: rate}
	}
}

func agileInArm(sc inboundScenario) func() inboundLimiter {
	return func() inboundLimiter {
		l := agilelimiter.NewConcurrency(agilelimiter.ConcurrencyConfig{
			MinLimit:        2,
			MaxLimit:        500,
			InitialLimit:    warmStartIn,
			SLOTarget:       sc.slo,
			FastHalfLife:    750 * time.Millisecond,
			SlowHalfLife:    10 * time.Second,
			DownStep:        0.30,
			UpStep:          0.05,
			Deadband:        0.08,
			TargetInflation: 2.5,
			Tick:            250 * time.Millisecond,
		})
		l.Start()
		return &agileInbound{l: l}
	}
}

func TestSweepInbound(t *testing.T) {
	if testing.Short() {
		t.Skip("sweep is slow; skipped in -short")
	}
	const phase = 3 * time.Second
	const phases = 3 // p1 = settle, p2+p3 = measured

	fast := inboundScenario{name: "in-fast(base20,SLO100)", base: 20 * time.Millisecond,
		par: 150, clients: 1200, slo: 100 * time.Millisecond}
	slow := inboundScenario{name: "in-slow(base150,SLO300)", base: 150 * time.Millisecond,
		par: 150, clients: 1200, slo: 300 * time.Millisecond}
	step := slow
	step.name = "in-step(par150->60->150)"
	step.drop, step.dropAt, step.dropFor = 0.4, phase, phase

	for _, sc := range []inboundScenario{fast, slow, step} {
		c := sc.ceiling()
		arms := []struct {
			label string
			mk    func() inboundLimiter
		}{
			{"unlimited", func() inboundLimiter { return noInbound{} }},
			{fmt.Sprintf("uber(0.50C=%.0f/s)", 0.50*c), uberInArm(int(0.50 * c))},
			{fmt.Sprintf("uber(0.75C=%.0f/s)", 0.75*c), uberInArm(int(0.75 * c))},
			{fmt.Sprintf("uber(1.00C=%.0f/s)", 1.00*c), uberInArm(int(1.00 * c))},
			{fmt.Sprintf("uber(1.25C=%.0f/s)", 1.25*c), uberInArm(int(1.25 * c))},
			{"agile", agileInArm(sc)},
		}
		for _, a := range arms {
			lim := a.mk()
			stats := runInbound(lim, sc, phase, phases)
			emit("inbound", sc.name, a.label, "settle", phase.Seconds(), stats[0], "")
			measured := mergeStats(stats[1:])
			emit("inbound", sc.name, a.label, "measured", (phase * time.Duration(phases-1)).Seconds(), measured,
				fmt.Sprintf("%d", lim.final()))
			for i, st := range stats[1:] {
				emit("inbound", sc.name, a.label, fmt.Sprintf("p%d", i+2), phase.Seconds(), st, "")
			}
			lim.stop()
		}
	}
}

// ---------------------------------------------------------------------------
// outbound: Limiter (rate) vs uber ratelimit — both block, only the rate differs
// ---------------------------------------------------------------------------

type outboundLimiter interface {
	acquire()
	observe(time.Duration)
	target() float64 // the rate this limiter is currently aiming at, req/s
	final() int
	stop()
}

type uberOutbound struct {
	l    uberratelimit.Limiter
	rate int
}

func (u *uberOutbound) acquire()              { u.l.Take() }
func (u *uberOutbound) observe(time.Duration) {}
func (u *uberOutbound) target() float64       { return float64(u.rate) }
func (u *uberOutbound) final() int            { return u.rate }
func (u *uberOutbound) stop()                 {}

type agileOutbound struct{ l *agilelimiter.Limiter }

func (a *agileOutbound) acquire()                { a.l.Take() }
func (a *agileOutbound) observe(d time.Duration) { a.l.Observe(d) }
func (a *agileOutbound) target() float64         { return a.l.Rate() }
func (a *agileOutbound) final() int              { return int(math.Round(a.l.Rate())) }
func (a *agileOutbound) stop()                   { a.l.Stop() }

type outboundScenario struct {
	name     string
	base     time.Duration
	slo      time.Duration
	capacity func(elapsed time.Duration) float64
}

// rtt is the modelled downstream response time at the given utilisation.
// util is clamped so saturation stays finite: the worst case is 20x base.
func (s outboundScenario) rtt(util float64) time.Duration {
	if util < 0 {
		util = 0
	}
	if util > 0.95 {
		util = 0.95
	}
	return time.Duration(float64(s.base) / (1 - util))
}

// runOutbound runs one arm for the given phase durations. durations[0] is the
// settle phase and should be discarded; the rest are measured.
func runOutbound(l outboundLimiter, sc outboundScenario, durations []time.Duration, workers int) []*sweepStats {
	stats := make([]*sweepStats, len(durations))
	for i := range stats {
		stats[i] = &sweepStats{}
	}
	bounds := make([]time.Duration, len(durations))
	var total time.Duration
	for i, d := range durations {
		total += d
		bounds[i] = total
	}
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				elapsed := time.Since(start)
				if elapsed >= total {
					return
				}
				ph := 0
				for ph < len(bounds)-1 && elapsed >= bounds[ph] {
					ph++
				}
				st := stats[ph]

				// The RTT must be sampled at the moment the slot is granted, not
				// before acquire(): at a low rate acquire() blocks for a long
				// time, and a latency snapshot taken before the wait would be
				// stale by seconds — the controller would then be judging the
				// service by yesterday's queue and can never recover.
				l.acquire()
				rtt := sc.rtt(l.target() / sc.capacity(time.Since(start)))
				begin := time.Now()
				time.Sleep(rtt)
				d := time.Since(begin)
				l.observe(d)
				st.store(d, sc.slo)
			}
		}()
	}
	wg.Wait()
	return stats
}

func uberOutArm(rate int) func() outboundLimiter {
	return func() outboundLimiter {
		return &uberOutbound{l: uberratelimit.New(rate, uberratelimit.WithoutSlack), rate: rate}
	}
}

func agileOutArm(slo time.Duration) func() outboundLimiter {
	return func() outboundLimiter {
		l := agilelimiter.New(agilelimiter.Config{
			MinRate:          5,
			MaxRate:          2000,
			InitialRate:      warmStartOut,
			SLOTarget:        slo,
			FastHalfLife:     750 * time.Millisecond,
			SlowHalfLife:     10 * time.Second,
			SlowDownHalfLife: time.Second,
			TargetInflation:  2.5,
			UpStep:           0.05,
			DownStep:         0.30,
			Gain:             1.0,
			Deadband:         0.08,
			Tick:             250 * time.Millisecond,
		})
		l.Start()
		return &agileOutbound{l: l}
	}
}

type armSpec = struct {
	label string
	mk    func() outboundLimiter
}

func sweepOut(t *testing.T, sc outboundScenario, durations []time.Duration, workers int, arms []armSpec) {
	t.Helper()
	var measured time.Duration
	for _, d := range durations[1:] {
		measured += d
	}
	for _, a := range arms {
		lim := a.mk()
		stats := runOutbound(lim, sc, durations, workers)
		emit("outbound", sc.name, a.label, "settle", durations[0].Seconds(), stats[0], "")
		merged := mergeStats(stats[1:])
		emit("outbound", sc.name, a.label, "measured", measured.Seconds(), merged,
			fmt.Sprintf("%d", lim.final()))
		for i, st := range stats[1:] {
			emit("outbound", sc.name, a.label, fmt.Sprintf("p%d", i+2), durations[i+1].Seconds(), st, "")
		}
		lim.stop()
	}
}

func TestSweepOutboundStatic(t *testing.T) {
	if testing.Short() {
		t.Skip("sweep is slow; skipped in -short")
	}
	// 10s to converge (the controller ramps at 5%/tick by design), then 3x2s
	// measured. The settle phase is not scored.
	durations := []time.Duration{8 * time.Second, 2 * time.Second, 2 * time.Second, 2 * time.Second}
	const workers = 300

	// Fast downstream, loose SLO: util = 0.80 sits exactly on the SLO.
	fast := outboundScenario{name: "out-fast(base20,SLO100)", base: 20 * time.Millisecond,
		slo: 100 * time.Millisecond, capacity: func(time.Duration) float64 { return 150 }}
	var fastArms []armSpec
	for _, f := range []float64{0.65, 0.72, 0.78, 0.80, 0.85} {
		rate := int(math.Round(f * 150))
		fastArms = append(fastArms, armSpec{
			fmt.Sprintf("uber(%.2fC=%d/s)", f, rate), uberOutArm(rate)})
	}
	fastArms = append(fastArms, armSpec{"agile", agileOutArm(fast.slo)})
	sweepOut(t, fast, durations, workers, fastArms)

	// Slow downstream, tight SLO: the safe rate is only 0.50 x capacity.
	slow := outboundScenario{name: "out-slow(base150,SLO300)", base: 150 * time.Millisecond,
		slo: 300 * time.Millisecond, capacity: func(time.Duration) float64 { return 150 }}
	var slowArms []armSpec
	for _, f := range []float64{0.30, 0.40, 0.48, 0.50, 0.55} {
		rate := int(math.Round(f * 150))
		slowArms = append(slowArms, armSpec{
			fmt.Sprintf("uber(%.2fC=%d/s)", f, rate), uberOutArm(rate)})
	}
	slowArms = append(slowArms, armSpec{"agile", agileOutArm(slow.slo)})
	sweepOut(t, slow, durations, workers, slowArms)
}

func TestSweepOutboundDynamic(t *testing.T) {
	if testing.Short() {
		t.Skip("sweep is slow; skipped in -short")
	}
	const workers = 300
	// Phases must be much longer than the modelled RTT (up to 20x base = 3s
	// here), otherwise a request granted in one phase completes in the next and
	// the per-phase attribution lags by a full RTT.

	// Capacity drifts 150 -> 90 -> 150 over 30s: the "capacity moves but slowly"
	// case, which is what a real dependency does. No single fixed rate is good in
	// both the healthy part (safe rate 120/s) and the trough (safe rate 72/s).
	ramp := outboundScenario{name: "out-ramp(C150->90->150)", base: 150 * time.Millisecond,
		slo: 300 * time.Millisecond, capacity: func(el time.Duration) float64 {
			return 120 + 30*math.Cos(2*math.Pi*el.Seconds()/30.0)
		}}
	sweepOut(t, ramp, []time.Duration{10 * time.Second, 10 * time.Second, 10 * time.Second},
		workers, []armSpec{
			{"uber(0.80x150=120/s)", uberOutArm(120)},
			{"uber(0.80x90=72/s)", uberOutArm(72)},
			{"agile", agileOutArm(ramp.slo)},
		})

	// Capacity oscillates 40..160 req/s with a 6s period.
	diurnal := outboundScenario{name: "out-diurnal(C100+/-60)", base: 20 * time.Millisecond,
		slo: 100 * time.Millisecond, capacity: func(el time.Duration) float64 {
			return 100 + 60*math.Sin(2*math.Pi*el.Seconds()/6.0)
		}}
	sweepOut(t, diurnal, []time.Duration{6 * time.Second, 6 * time.Second, 6 * time.Second}, workers, []armSpec{
		{"uber(0.75x100=75/s)", uberOutArm(75)},
		{"uber(0.75x40=30/s)", uberOutArm(30)},
		{"agile", agileOutArm(diurnal.slo)},
	})
}
