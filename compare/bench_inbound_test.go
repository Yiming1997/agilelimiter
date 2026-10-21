// End-to-end (macro) comparison for the *inbound* case: protecting a service
// from its own clients.
//
// Both limiters gate the same simulated service, with the same client swarm, the
// same handler, the same SLO and the same fixed run duration. Only the admission
// strategy differs:
//
//	uber          fixed-rate leaky bucket. Take() BLOCKS. Excess demand is queued
//	              by the caller's own goroutine and never refused; no feedback.
//	agilelimiter  adaptive concurrency semaphore. TryTake() REFUSES. Excess
//	              demand is shed immediately and the admitted concurrency is
//	              steered by the handler's own latency.
//
// The service model:
//
//	handling time = base * max(1, inflight/parallel)
//
// Up to `parallel` concurrent requests it answers at `base`; beyond that,
// requests contend and each one gets slower in proportion. The service's
// throughput ceiling is therefore parallel/base req/s, and its latency knee sits
// exactly at parallel — which is what makes this a sharp test of whether the
// controller can find the knee from latency alone.
//
// Run with:
//
//	go test -run '^TestInboundMacro' -v -timeout 30m .
package main

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agilelimiter"
	uberratelimit "go.uber.org/ratelimit"
)

const (
	defaultSLO    = 300 * time.Millisecond
	benchPhase    = 4 * time.Second
	benchRefuseNs = 2 * time.Millisecond
	benchP99      = 0.99
)

// scenario is one service + workload under test. All rates are derived from
// these numbers, never hard-coded.
type scenario struct {
	name     string
	base     time.Duration
	parallel float64
	clients  int
	degraded bool
	slo      time.Duration

	// maxBaselineRise overrides Config.MaxBaselineRise. Zero keeps the library
	// default; a large value (1.0) disables the anti-drift cap, which is how the
	// two fixes are isolated from each other in the A/B runs.
	maxBaselineRise float64

	// targetInflation overrides Config.TargetInflation. Zero keeps the library
	// default (1.0, i.e. tolerate 2x baseline latency).
	targetInflation float64

	// limiterSLO overrides the limiter's own SLOTarget. Zero means "5x base",
	// which keeps the tail guard out of the way; set it to the business SLO to
	// exercise the window cap.
	limiterSLO time.Duration
}

func (s scenario) sloTarget() time.Duration {
	if s.slo > 0 {
		return s.slo
	}
	return defaultSLO
}

// ceiling is the service's own throughput limit in req/s.
func (s scenario) ceiling() float64 { return s.parallel / s.base.Seconds() }

// latencyAt models the service. Pure function of concurrency, identical for every
// arm, so no limiter gets a kinder service than another.
func (s scenario) latencyAt(inflight int) time.Duration {
	contention := 1.0
	if c := float64(inflight) / s.parallel; c > 1 {
		contention = c
	}
	return time.Duration(float64(s.base) * contention)
}

// parallelAt is the service's concurrency capacity at a moment in the run.
func (s scenario) parallelAt(elapsed time.Duration) float64 {
	if s.degraded && elapsed >= benchPhase && elapsed < 2*benchPhase {
		return s.parallel / 2.5
	}
	return s.parallel
}

func newInboundAgile(s scenario) (*agilelimiter.ConcurrencyLimiter, func()) {
	slo := s.limiterSLO
	if slo == 0 {
		slo = 5 * s.base
	}
	l := agilelimiter.NewConcurrency(agilelimiter.ConcurrencyConfig{
		MinLimit:        2,
		MaxLimit:        500,
		InitialLimit:    20,
		SLOTarget:       slo,
		FastHalfLife:    250 * time.Millisecond,
		SlowHalfLife:    5 * time.Second,
		TargetInflation: s.targetInflation,
		Tick:            100 * time.Millisecond,
		MaxBaselineRise: s.maxBaselineRise,
	})
	l.Start()
	return l, l.Stop
}

// arm is one limiter configuration under test.
//
// acquire returns (release, admitted); every admitted request MUST call release
// exactly once (TryTake pairs with Release). observe feeds the handler's duration
// back to the controller — uber ignores it by construction, and skipping it for
// agilelimiter would leave its controller blind.
type arm struct {
	label string
	make  func() (acquire func() (func(), bool), observe func(time.Duration), final func() string, done func())
}

func agileArm(s scenario) arm {
	return arm{
		label: "agile(adaptive)",
		make: func() (func() (func(), bool), func(time.Duration), func() string, func()) {
			l, stop := newInboundAgile(s)
			return func() (func(), bool) {
					if !l.TryTake() {
						return nil, false
					}
					return l.Release, true
				},
				l.Observe,
				func() string { return fmt.Sprintf("%d", l.Limit()) },
				stop
		},
	}
}

// uberArm takes a rate as a MULTIPLE of the service ceiling.
func uberArm(multiple float64, s scenario, noSlack bool) arm {
	rate := int(multiple * s.ceiling())
	if rate < 1 {
		rate = 1
	}
	label := fmt.Sprintf("uber(%.1fx)", multiple)
	opts := []uberratelimit.Option{}
	if noSlack {
		label = fmt.Sprintf("uber(%.1fx,noSlack)", multiple)
		opts = append(opts, uberratelimit.WithoutSlack)
	}
	return arm{
		label: label,
		make: func() (func() (func(), bool), func(time.Duration), func() string, func()) {
			l := uberratelimit.New(rate, opts...)
			return func() (func(), bool) { l.Take(); return func() {}, true },
				func(time.Duration) {},
				func() string { return fmt.Sprintf("%d", rate) },
				func() {}
		},
	}
}

func unlimitedArm() arm {
	return arm{
		label: "unlimited(none)",
		make: func() (func() (func(), bool), func(time.Duration), func() string, func()) {
			return func() (func(), bool) { return func() {}, true },
				func(time.Duration) {},
				func() string { return "-" },
				func() {}
		},
	}
}

type phaseStats struct {
	admitted  atomic.Int64
	refused   atomic.Int64
	good      atomic.Int64
	workNanos atomic.Int64
	peak      atomic.Int64

	mu   sync.Mutex
	lats []time.Duration
}

func (p *phaseStats) finish(d time.Duration, slo time.Duration) {
	p.lats = append(p.lats, d)
	p.workNanos.Add(int64(d))
	if d <= slo {
		p.good.Add(1)
	}
}

func (p *phaseStats) peakStore(n int) {
	for {
		cur := p.peak.Load()
		if int64(n) <= cur || p.peak.CompareAndSwap(cur, int64(n)) {
			return
		}
	}
}

func (p *phaseStats) quantile(q float64) time.Duration {
	if len(p.lats) == 0 {
		return 0
	}
	sort.Slice(p.lats, func(i, j int) bool { return p.lats[i] < p.lats[j] })
	return p.lats[int(float64(len(p.lats)-1)*q)]
}

func runArm(t *testing.T, a arm, s scenario) ([]*phaseStats, string) {
	t.Helper()

	const nPhases = 3
	stats := make([]*phaseStats, nPhases)
	for i := range stats {
		stats[i] = &phaseStats{}
	}

	acquire, observe, final, done := a.make()
	defer done()
	slo := s.sloTarget()

	var inflight atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < s.clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			for {
				elapsed := time.Since(start)
				if elapsed >= nPhases*benchPhase {
					return
				}
				ph := int(elapsed / benchPhase)
				if ph >= nPhases {
					ph = nPhases - 1
				}
				ps := stats[ph]

				release, ok := acquire()
				if !ok {
					ps.refused.Add(1)
					time.Sleep(benchRefuseNs)
					continue
				}

				n := inflight.Add(1)
				ps.peakStore(int(n))
				begin := time.Now()
				time.Sleep(s.latencyAt(int(n)))
				d := time.Since(begin)
				inflight.Add(-1)
				release()
				observe(d)

				ps.admitted.Add(1)
				ps.mu.Lock()
				ps.finish(d, slo)
				ps.mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return stats, final()
}

var benchPhases = []string{"phase1 healthy", "phase2 degraded", "phase3 recovered"}

func reportArm(t *testing.T, a arm, s scenario) {
	t.Helper()
	stats, final := runArm(t, a, s)
	for i, ps := range stats {
		secs := benchPhase.Seconds()
		adm := float64(ps.admitted.Load())
		good := float64(ps.good.Load())

		ps.mu.Lock()
		mean := time.Duration(0)
		if n := len(ps.lats); n > 0 {
			mean = time.Duration(ps.workNanos.Load() / int64(n))
		}
		p99 := ps.quantile(benchP99)
		ps.mu.Unlock()

		goodPct := 0.0
		if adm > 0 {
			goodPct = 100 * good / adm
		}
		svcWork := float64(ps.workNanos.Load()) / 1e9 / secs

		phase := benchPhases[i]
		if s.degraded && i == 1 {
			phase = fmt.Sprintf("phase2 degraded(%.0f)", s.parallel/2.5)
		} else if i != 1 {
			phase = fmt.Sprintf("%s(%.0f)", phase, s.parallel)
		}

		shown := ""
		if i == len(stats)-1 {
			shown = final
		}
		t.Logf("%-17s %-6s | %-22s %7.0f %7.0f %6.0f%% %9.0f %7s %7s %6d %7.0f",
			a.label, shown, phase,
			adm/secs, good/secs, goodPct, float64(ps.refused.Load())/secs,
			mean.Round(time.Millisecond), p99.Round(time.Millisecond),
			ps.peak.Load(), svcWork)
	}
	t.Logf("")
}

func reportScenario(t *testing.T, s scenario) {
	t.Helper()
	t.Logf("\n=== %s: base=%s, parallel=%.0f, ceiling=%.0f req/s, SLO=%s, %d clients (%s/phase)",
		s.name, s.base, s.parallel, s.ceiling(), s.sloTarget(), s.clients, benchPhase)
	t.Logf("%-17s %-6s %-24s %7s %7s %7s %9s %7s %7s %6s",
		"arm", "final", "phase", "adm/s", "good/s", "good%", "refuse/s", "mean", "p99", "peak")

	arms := []arm{
		unlimitedArm(),
		uberArm(0.5, s, true), uberArm(1.0, s, true), uberArm(2.5, s, true),
		agileArm(s),
	}
	for _, a := range arms {
		reportArm(t, a, s)
	}
}

func fastSteady() scenario {
	return scenario{name: "FAST-STEADY (web handler)", base: 20 * time.Millisecond,
		parallel: 150, clients: 2000, slo: 100 * time.Millisecond}
}

func slowSteady(extra ...func(*scenario)) scenario {
	s := scenario{name: "SLOW-STEADY (database-style handler)", base: 150 * time.Millisecond,
		parallel: 150, clients: 2000, slo: 300 * time.Millisecond}
	for _, f := range extra {
		f(&s)
	}
	return s
}

func TestInboundMacroFastSteady(t *testing.T) {
	if testing.Short() {
		t.Skip("macro comparison is slow; skipped in -short")
	}
	reportScenario(t, fastSteady())
}

func TestInboundMacroSlowSteady(t *testing.T) {
	if testing.Short() {
		t.Skip("macro comparison is slow; skipped in -short")
	}
	reportScenario(t, slowSteady())
}

// TestInboundMacroSlowNoDriftCap isolates the two fixes: the histogram
// resolution fix is always active now, but the baseline drift cap is disabled
// here (MaxBaselineRise=1.0), so the result shows what the histogram fix alone
// buys.
func TestInboundMacroSlowNoDriftCap(t *testing.T) {
	if testing.Short() {
		t.Skip("macro comparison is slow; skipped in -short")
	}
	reportScenario(t, slowSteady(func(s *scenario) {
		s.name = "SLOW-STEADY, drift cap DISABLED (histogram fix only)"
		s.maxBaselineRise = 1.0
	}))
}

// TestInboundMacroSlowTunedWindow checks the other half of the diagnosis: the
// target window is an ABSOLUTE latency multiple, and the library default of 1.0
// means "tolerate 2x baseline". Here SLO/base is exactly 2.0, so a 2x window
// parks the controller precisely on the SLO boundary and half the requests miss.
// Setting the window from the SLO budget instead (0.5 -> tolerate 1.5x baseline,
// i.e. 201ms, comfortably under the 300ms SLO) should close the remaining gap.
func TestInboundMacroSlowTunedWindow(t *testing.T) {
	if testing.Short() {
		t.Skip("macro comparison is slow; skipped in -short")
	}
	reportScenario(t, slowSteady(func(s *scenario) {
		s.name = "SLOW-STEADY, TargetInflation=0.5 (window fitted to the SLO)"
		s.targetInflation = 0.5
	}))
}

// TestInboundMacroSlowSLOWindow exercises the third fix: with SLOTarget set to
// the real business SLO and TargetInflation left at its default, the window cap
// alone should keep the controller under the SLO — no hand tuning of
// TargetInflation required.
func TestInboundMacroSlowSLOWindow(t *testing.T) {
	if testing.Short() {
		t.Skip("macro comparison is slow; skipped in -short")
	}
	reportScenario(t, slowSteady(func(s *scenario) {
		s.name = "SLOW-STEADY, SLOTarget=300ms, TargetInflation default"
		s.limiterSLO = 300 * time.Millisecond
	}))
}

func TestInboundMacroSlowStep(t *testing.T) {
	if testing.Short() {
		t.Skip("macro comparison is slow; skipped in -short")
	}
	reportScenario(t, scenario{
		name: "SLOW-STEP (capacity drops 2.5x)", base: 150 * time.Millisecond,
		parallel: 150, clients: 2000, degraded: true, slo: 300 * time.Millisecond,
	})
}
