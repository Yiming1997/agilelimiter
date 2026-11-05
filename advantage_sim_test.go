package agilelimiter

// Advantage map: where does the adaptive limiter beat a fixed-rate limiter
// (go.uber.org/ratelimit), and where does it lose?
//
// compare/ answers this on a real clock, which costs seconds per row and leaves
// room for a handful of points. This file answers it on a virtual clock: the
// real controller (ctrl.step) is driven against a fluid model of the protected
// service, so a whole grid of capacity schedules runs in seconds and every
// number is reproducible.
//
// The fixed-rate arm is modelled as exactly that — a pacer at one constant rate.
// Under saturated demand that is all uber's Take() does, and modelling it
// directly lets every table score the *best fixed rate in hindsight* (an oracle
// no operator has) next to the rate an operator would plausibly pick.
//
// Outbound downstream:
//
//	latency  = base / (1 - min(util, 0.95)) + backlog/capacity
//	backlog += (rate - capacity) * dt, floored at 0
//
// Inbound service (processor sharing over `par` workers, closed-loop clients):
//
//	latency  = base * max(1, inflight/par),  throughput = min(inflight, par)/base
//
// Every request's latency is the model value times a lognormal jitter with
// median 1 and sigma simSigma, and a request is good when it completes within
// the SLO. Goodput is scored with the expected good fraction, so the fixed arm
// needs no sampling; the adaptive arm is fed sampled latencies, because those
// are what its histogram sees. The backlog term is what makes an overload cost
// more than the tick it happened in: a probe that overshoots leaves a queue
// that the following ticks have to drain.
//
// Print the tables with:
//
//	go test -run '^TestAdvantage' -v .

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"
)

const (
	simSigma      = 0.2
	simDt         = time.Second // the default Tick
	simWarm       = 60 * time.Second
	simMaxSamples = 2000 // per tick; quantiles at 19% resolution do not need more
)

// tickRec is what one tick delivered: requests sent and requests inside the SLO.
type tickRec struct{ sent, good float64 }

type trace []tickRec

// score returns good/s, sent/s and good% over ticks [from, to).
func (tr trace) score(from, to time.Duration) (goodput, sent, okPct float64) {
	i, j := int(from/simDt), int(to/simDt)
	if j > len(tr) {
		j = len(tr)
	}
	var g, s float64
	for _, r := range tr[i:j] {
		g += r.good
		s += r.sent
	}
	secs := float64(j-i) * simDt.Seconds()
	if s > 0 {
		okPct = 100 * g / s
	}
	return g / secs, s / secs, okPct
}

func (tr trace) goodput(from, to time.Duration) float64 {
	g, _, _ := tr.score(from, to)
	return g
}

// goodFraction is P(lat * jitter <= slo) for lognormal jitter with median 1.
func goodFraction(lat float64, slo time.Duration) float64 {
	if lat <= 0 {
		return 1
	}
	return 0.5 * math.Erfc(-math.Log(slo.Seconds()/lat)/(simSigma*math.Sqrt2))
}

func feedJittered(observe func(time.Duration), rng *rand.Rand, lat float64, n int) {
	if n > simMaxSamples {
		n = simMaxSamples
	}
	for i := 0; i < n; i++ {
		observe(time.Duration(lat * math.Exp(simSigma*rng.NormFloat64()) * 1e9))
	}
}

// ---------------------------------------------------------------------------
// outbound: Limiter vs a fixed rate
// ---------------------------------------------------------------------------

type outModel struct {
	base     func(t time.Duration) time.Duration
	capacity func(t time.Duration) float64
	slo      time.Duration
	dur      time.Duration
}

func constBase(d time.Duration) func(time.Duration) time.Duration {
	return func(time.Duration) time.Duration { return d }
}

func constCap(c float64) func(time.Duration) float64 {
	return func(time.Duration) float64 { return c }
}

// outQueue carries the downstream's overload backlog across ticks.
type outQueue struct{ backlog float64 }

func (q *outQueue) latency(m outModel, rate float64, t time.Duration) float64 {
	c := m.capacity(t)
	q.backlog = math.Max(0, q.backlog+(rate-c)*simDt.Seconds())
	util := math.Min(rate/c, 0.95)
	return m.base(t).Seconds()/(1-util) + q.backlog/c
}

func runOutFixed(m outModel, rate float64) trace {
	var q outQueue
	tr := make(trace, 0, int(m.dur/simDt))
	for t := time.Duration(0); t < m.dur; t += simDt {
		lat := q.latency(m, rate, t)
		sent := rate * simDt.Seconds()
		tr = append(tr, tickRec{sent: sent, good: sent * goodFraction(lat, m.slo)})
	}
	return tr
}

func runOutAdaptive(m outModel, cfg Config) trace {
	l := New(cfg)
	rng := rand.New(rand.NewSource(1))
	var q outQueue
	var carry float64
	tr := make(trace, 0, int(m.dur/simDt))
	for t := time.Duration(0); t < m.dur; t += simDt {
		rate := l.Rate()
		lat := q.latency(m, rate, t)
		sent := rate * simDt.Seconds()
		carry += sent
		n := int(carry)
		carry -= float64(n)
		feedJittered(l.Observe, rng, lat, n)
		l.ctrl.step()
		tr = append(tr, tickRec{sent: sent, good: sent * goodFraction(lat, m.slo)})
	}
	return tr
}

// bestOutFixed scans fixed rates and returns the one with the most goodput over
// the scored window — the fixed rate an oracle would have picked.
func bestOutFixed(m outModel, from, to time.Duration) (rate, goodput float64) {
	return bestOutFixedOK(m, from, to, 0)
}

// bestOutFixedOK is bestOutFixed restricted to rates that keep at least minOK
// percent of requests inside the SLO. It is the fair yardstick for the adaptive
// arm: the default tail guard fires whenever a tick's p99 crosses the SLO, so
// the controller is holding "99% inside the SLO", not maximising goodput at any
// violation rate.
func bestOutFixedOK(m outModel, from, to time.Duration, minOK float64) (rate, goodput float64) {
	for r := 1.0; r <= 600; r++ {
		if g, _, ok := runOutFixed(m, r).score(from, to); g > goodput && ok >= minOK {
			rate, goodput = r, g
		}
	}
	return rate, goodput
}

// staticOutRate is the rate an operator who load-tested the downstream at a
// fixed capacity would pin: the best fixed rate for that capacity alone.
func staticOutRate(base, slo time.Duration, capacity float64) float64 {
	m := outModel{base: constBase(base), capacity: constCap(capacity), slo: slo, dur: simWarm + 60*time.Second}
	r, _ := bestOutFixed(m, simWarm, m.dur)
	return r
}

type outArm struct {
	name   string
	preset Preset
}

var outArms = []outArm{
	{"agile balanced(2x)", PresetBalanced},
	{"agile latency-first", PresetLatencyFirst},
	{"agile throughput-first", PresetThroughputFirst},
	{"agile default", PresetNone}, // with an SLO: the SLO anchor and a 5% tail tolerance
}

// simOutCfg is the configuration under test: library defaults (1s tick, 45s
// baseline half-life, +5%/-30% steps), a loose ceiling and floor, and the SLO
// the run is scored against. MaxRate is deliberately far above any capacity in
// these models, so the controller is never told the answer.
func simOutCfg(slo time.Duration, p Preset, maxRate int) Config {
	return Config{
		MaxRate:     maxRate,
		FloorRate:   5,
		InitialRate: 50,
		SLOTarget:   slo,
		Preset:      p,
	}
}

// table renders rows with every column as wide as its widest cell. The first
// column is left-aligned, the rest right-aligned.
type table struct{ rows [][]string }

func newTable(header ...string) *table { return &table{rows: [][]string{header}} }

func (t *table) row(cells ...string) { t.rows = append(t.rows, cells) }

func (t *table) String() string {
	var widths []int
	for _, r := range t.rows {
		for i, c := range r {
			n := len([]rune(c))
			if i >= len(widths) {
				widths = append(widths, n)
			} else if n > widths[i] {
				widths[i] = n
			}
		}
	}
	var b strings.Builder
	for _, r := range t.rows {
		b.WriteString("\n")
		for i, c := range r {
			pad := strings.Repeat(" ", widths[i]-len([]rune(c)))
			if i == 0 {
				b.WriteString(c + pad)
			} else {
				b.WriteString("  " + pad + c)
			}
		}
	}
	return b.String()
}

func f0(v float64) string  { return fmt.Sprintf("%.0f", v) }
func f1(v float64) string  { return fmt.Sprintf("%.1f", v) }
func pct(v float64) string { return fmt.Sprintf("%.0f%%", v) }

// Static capacity, mis-estimated. The operator pins uber at the true best rate
// times (1+e); the adaptive limiter is told nothing but a loose ceiling. A
// second adaptive arm is given the operator's guess as its MaxRate, which is how
// the library is meant to be deployed: the same number, promised as a ceiling
// instead of pinned as a rate.
func TestAdvantageEstimateError(t *testing.T) {
	for _, ds := range []struct {
		name      string
		base, slo time.Duration
	}{
		{"fast downstream: base 20ms, SLO 100ms, capacity 150/s", 20 * time.Millisecond, 100 * time.Millisecond},
		{"slow downstream: base 150ms, SLO 300ms, capacity 150/s", 150 * time.Millisecond, 300 * time.Millisecond},
	} {
		m := outModel{base: constBase(ds.base), capacity: constCap(150), slo: ds.slo, dur: simWarm + 600*time.Second}
		best, bestGood := bestOutFixed(m, simWarm, m.dur)
		safe, safeGood := bestOutFixedOK(m, simWarm, m.dur, 99)

		loose := runOutAdaptive(m, simOutCfg(ds.slo, PresetBalanced, 600))
		looseGood, _, looseOK := loose.score(simWarm, m.dur)
		// The default aims at 0.8 x SLOTarget and tolerates 5% of the traffic
		// above it: more goodput, a few points less inside the SLO.
		dflt, _, dfltOK := runOutAdaptive(m, simOutCfg(ds.slo, PresetNone, 600)).score(simWarm, m.dur)

		tb := newTable("estimate error", "uber rate", "uber good/s", "uber ok",
			"balanced(Max=guess)", "balanced(Max=600)", "default(Max=600)")
		for _, e := range []float64{-0.5, -0.3, -0.2, -0.1, -0.05, 0, 0.05, 0.1, 0.2, 0.5} {
			guess := best * (1 + e)
			ug, _, uok := runOutFixed(m, guess).score(simWarm, m.dur)
			capped := runOutAdaptive(m, simOutCfg(ds.slo, PresetBalanced, int(math.Round(guess))))
			cg, _, cok := capped.score(simWarm, m.dur)
			tb.row(fmt.Sprintf("%+.0f%%", e*100), f0(guess), f1(ug), pct(uok),
				fmt.Sprintf("%s (%s)", f1(cg), pct(cok)), fmt.Sprintf("%s (%s)", f1(looseGood), pct(looseOK)),
				fmt.Sprintf("%s (%s)", f1(dflt), pct(dfltOK)))

			// Whatever the guess, a ceiling must not cost the SLO.
			if cok < 97 {
				t.Errorf("%s, e=%+.0f%%: agile capped at the guess kept only %.0f%% inside the SLO", ds.name, e*100, cok)
			}
		}
		t.Logf("%s\noracle fixed rate (max goodput)     = %.0f/s -> %.1f good/s\n"+
			"oracle fixed rate (>=99%% in SLO)    = %.0f/s -> %.1f good/s\n"+
			"agile, untold: %.1f good/s = %.0f%% / %.0f%% of the two oracles; uber error columns are relative to the first%s",
			ds.name, best, bestGood, safe, safeGood, looseGood, 100*looseGood/bestGood, 100*looseGood/safeGood, tb)

		if looseGood < 0.85*safeGood {
			t.Errorf("%s: agile reached %.1f good/s, under 85%% of the SLO-constrained oracle %.1f", ds.name, looseGood, safeGood)
		}
		if ug := runOutFixed(m, best*1.5).goodput(simWarm, m.dur); looseGood <= ug {
			t.Errorf("%s: a fixed rate 50%% too high still made %.1f good/s against agile's %.1f", ds.name, ug, looseGood)
		}
	}
}

// One step change in capacity. Two operators: one tuned for the capacity before
// the change, one for the worse of the two phases. Scored over the 120s before
// the change and the 300s after it.
func TestAdvantageStep(t *testing.T) {
	const base, slo = 20 * time.Millisecond, 100 * time.Millisecond
	change := simWarm + 120*time.Second
	dur := change + 300*time.Second

	tb := newTable("capacity", "uber(before)", "uber(worst)", "oracle fixed", "agile balanced", "agile slo", "agile tput", "agile default", "agile recover")
	for _, step := range []struct{ from, to float64 }{
		{150, 135}, {150, 105}, {150, 75}, {150, 45}, {150, 15},
		{45, 150}, {75, 150},
	} {
		m := outModel{
			base: constBase(base),
			capacity: func(t time.Duration) float64 {
				if t < change {
					return step.from
				}
				return step.to
			},
			slo: slo, dur: dur,
		}
		before := staticOutRate(base, slo, step.from)
		worst := staticOutRate(base, slo, math.Min(step.from, step.to))
		_, oracle := bestOutFixed(m, simWarm, dur)

		cells := []string{fmt.Sprintf("%.0f->%.0f", step.from, step.to)}
		for _, r := range []float64{before, worst} {
			g, _, ok := runOutFixed(m, r).score(simWarm, dur)
			cells = append(cells, fmt.Sprintf("%s (%s)", f1(g), pct(ok)))
		}
		cells = append(cells, f1(oracle))
		var defaultTrace trace
		for _, a := range outArms {
			tr := runOutAdaptive(m, simOutCfg(slo, a.preset, 600))
			if a.preset == PresetBalanced {
				defaultTrace = tr
			}
			g, _, ok := tr.score(simWarm, dur)
			cells = append(cells, fmt.Sprintf("%s (%s)", f1(g), pct(ok)))
		}
		settled := runOutAdaptive(outModel{base: constBase(base), capacity: constCap(step.to), slo: slo, dur: simWarm + 300*time.Second},
			simOutCfg(slo, PresetBalanced, 600)).goodput(simWarm, simWarm+300*time.Second)
		cells = append(cells, recoverTime(defaultTrace, change, settled))
		tb.row(cells...)

		if step.to < 0.6*step.from {
			uber, _, _ := runOutFixed(m, before).score(simWarm, dur)
			agile := defaultTrace.goodput(simWarm, dur)
			if agile <= uber {
				t.Errorf("capacity %v->%v: agile %.1f good/s did not beat uber tuned for the healthy phase (%.1f)",
					step.from, step.to, agile, uber)
			}
		}
	}
	t.Logf("base 20ms, SLO 100ms; capacity changes at t=120s, scored 120s before + 300s after\n"+
		"cells are good/s (share of sent inside the SLO); agile recover = seconds after the change until the\n"+
		"default arm's 30s rolling goodput reaches 90%% of what it settles at on the new capacity alone%s", tb)
}

// recoverTime is how long after `at` the trace's 30-tick rolling goodput first
// reaches 90% of target (good/s). A rolling window, because the controller
// saw-tooths around its operating point and single ticks undershoot by design.
func recoverTime(tr trace, at time.Duration, target float64) string {
	if s := recoverSecs(tr, at, target); s >= 0 {
		return fmt.Sprintf("%ds", s)
	}
	return "never"
}

// recoverSecs is recoverTime as a number, -1 for never.
func recoverSecs(tr trace, at time.Duration, target float64) int {
	const window = 30
	start := int(at / simDt)
	for end := start + window; end <= len(tr); end++ {
		if g := tr.goodput(time.Duration(end-window)*simDt, time.Duration(end)*simDt); g >= 0.9*target {
			return end - window - start
		}
	}
	return -1
}

// Periodic capacity drift: capacity = 150 * (1 + a*sin(2*pi*t/P)). This is the
// map of the advantage: the amplitude says how much a single fixed rate must
// give up, the period says whether the controller can keep up.
func TestAdvantageDrift(t *testing.T) {
	const base, slo = 20 * time.Millisecond, 100 * time.Millisecond
	mean := staticOutRate(base, slo, 150)

	tb := newTable("amp", "period", "uber(mean)", "uber(trough)", "oracle fixed", "agile balanced", "agile slo", "agile tput", "agile default", "best agile/oracle")
	for _, a := range []float64{0.1, 0.3, 0.5} {
		trough := staticOutRate(base, slo, 150*(1-a))
		for _, p := range []time.Duration{10 * time.Second, 30 * time.Second, 60 * time.Second, 120 * time.Second, 300 * time.Second, 900 * time.Second} {
			span := 3 * p
			if span < 600*time.Second {
				span = 600 * time.Second
			}
			m := outModel{
				base: constBase(base),
				capacity: func(t time.Duration) float64 {
					return 150 * (1 + a*math.Sin(2*math.Pi*t.Seconds()/p.Seconds()))
				},
				slo: slo, dur: simWarm + span,
			}
			_, oracle := bestOutFixed(m, simWarm, m.dur)
			cells := []string{fmt.Sprintf("±%.0f%%", a*100), p.String()}
			for _, r := range []float64{mean, trough} {
				g, _, ok := runOutFixed(m, r).score(simWarm, m.dur)
				cells = append(cells, fmt.Sprintf("%s (%s)", f0(g), pct(ok)))
			}
			cells = append(cells, f0(oracle))
			bestAgile := 0.0
			for _, arm := range outArms {
				g, _, ok := runOutAdaptive(m, simOutCfg(slo, arm.preset, 600)).score(simWarm, m.dur)
				bestAgile = math.Max(bestAgile, g)
				cells = append(cells, fmt.Sprintf("%s (%s)", f0(g), pct(ok)))
				if arm.preset == PresetBalanced && a >= 0.3 && p >= time.Minute && g < oracle {
					t.Errorf("drift ±%.0f%% over %v: agile %.1f good/s did not beat the best fixed rate in hindsight (%.1f)",
						a*100, p, g, oracle)
				}
			}
			cells = append(cells, fmt.Sprintf("%.2f", bestAgile/oracle))
			tb.row(cells...)
		}
	}
	t.Logf("base 20ms, SLO 100ms, capacity 150*(1+a*sin(2πt/P)); uber(mean) is pinned at the best rate for 150/s,\n"+
		"uber(trough) at the best rate for the minimum; cells are good/s (share inside the SLO)%s", tb)
}

// The idle latency rises with capacity unchanged: 20ms -> 60ms against a 200ms
// SLO. Under the queueing model this also lowers the safe rate (the SLO allows
// less queueing on top of a slower base), so a fixed rate tuned for the fast
// phase is now too high — and the baseline anchor's known weakness, reading a
// slower-but-healthy downstream as congestion, is visible next to it.
func TestAdvantageBenignRise(t *testing.T) {
	const slo = 200 * time.Millisecond
	change := simWarm + 120*time.Second
	dur := change + 300*time.Second

	tb := newTable("base", "uber(before)", "uber(after)", "oracle fixed", "agile balanced", "agile slo", "agile tput", "agile default")
	for _, rise := range []struct{ from, to time.Duration }{
		{20 * time.Millisecond, 40 * time.Millisecond},
		{20 * time.Millisecond, 60 * time.Millisecond},
		{20 * time.Millisecond, 100 * time.Millisecond},
	} {
		m := outModel{
			base: func(t time.Duration) time.Duration {
				if t < change {
					return rise.from
				}
				return rise.to
			},
			capacity: constCap(150), slo: slo, dur: dur,
		}
		cells := []string{fmt.Sprintf("%v->%v", rise.from, rise.to)}
		for _, r := range []float64{staticOutRate(rise.from, slo, 150), staticOutRate(rise.to, slo, 150)} {
			g, _, ok := runOutFixed(m, r).score(change, dur)
			cells = append(cells, fmt.Sprintf("%s (%s)", f1(g), pct(ok)))
		}
		_, oracle := bestOutFixed(m, change, dur)
		cells = append(cells, f1(oracle))
		for _, arm := range outArms {
			g, _, ok := runOutAdaptive(m, simOutCfg(slo, arm.preset, 600)).score(change, dur)
			cells = append(cells, fmt.Sprintf("%s (%s)", f1(g), pct(ok)))
		}
		tb.row(cells...)
	}
	t.Logf("capacity 150/s, SLO 200ms, idle latency rises at t=120s; scored over the 300s after the rise\n"+
		"uber(before)/uber(after) are the best fixed rates for the old/new idle latency%s", tb)
}

// ---------------------------------------------------------------------------
// inbound: ConcurrencyLimiter vs a fixed admission rate
// ---------------------------------------------------------------------------

type inModel struct {
	base    time.Duration
	par     func(t time.Duration) float64
	slo     time.Duration
	clients float64
	dur     time.Duration
}

func (m inModel) capacity(t time.Duration) float64 { return m.par(t) / m.base.Seconds() }

// runInFixed admits a fixed rate, as uber's blocking Take() does in front of a
// handler. Admitted work beyond capacity piles up inside the service until
// every client is stuck in it.
func runInFixed(m inModel, rate float64) trace {
	b := m.base.Seconds()
	dt := simDt.Seconds()
	var inflight float64
	tr := make(trace, 0, int(m.dur/simDt))
	for t := time.Duration(0); t < m.dur; t += simDt {
		par, c := m.par(t), m.capacity(t)
		if steady := rate * b; rate <= c {
			inflight = math.Max(steady, inflight-(c-rate)*dt)
		} else {
			inflight = math.Min(m.clients, math.Max(inflight, par)+(rate-c)*dt)
		}
		lat := b * math.Max(1, inflight/par)
		done := math.Min(inflight, par) / b * dt
		tr = append(tr, tickRec{sent: done, good: done * goodFraction(lat, m.slo)})
	}
	return tr
}

func runInAdaptive(m inModel, cfg ConcurrencyConfig) trace {
	l := NewConcurrency(cfg)
	rng := rand.New(rand.NewSource(1))
	b := m.base.Seconds()
	tr := make(trace, 0, int(m.dur/simDt))
	for t := time.Duration(0); t < m.dur; t += simDt {
		par := m.par(t)
		inflight := math.Min(float64(l.Limit()), m.clients)
		lat := b * math.Max(1, inflight/par)
		done := inflight / lat * simDt.Seconds()
		feedJittered(l.Observe, rng, lat, int(done))
		l.ctrl.step()
		tr = append(tr, tickRec{sent: done, good: done * goodFraction(lat, m.slo)})
	}
	return tr
}

func bestInFixed(m inModel, from, to time.Duration) (rate, goodput float64) {
	for r := 10.0; r <= 3000; r += 10 {
		if g := runInFixed(m, r).goodput(from, to); g > goodput {
			rate, goodput = r, g
		}
	}
	return rate, goodput
}

func simInCfg(slo time.Duration, p Preset) ConcurrencyConfig {
	return ConcurrencyConfig{
		MinLimit:     2,
		MaxLimit:     500,
		InitialLimit: 100,
		SLOTarget:    slo,
		Preset:       p,
	}
}

func TestAdvantageInbound(t *testing.T) {
	const base, slo = 150 * time.Millisecond, 300 * time.Millisecond
	const par = 150.0 // capacity 1000/s
	change := simWarm + 120*time.Second
	dur := simWarm + 600*time.Second // whole periods for both drift rows

	steadyModel := inModel{base: base, par: func(time.Duration) float64 { return par }, slo: slo, clients: 1200, dur: simWarm + 60*time.Second}
	healthy, _ := bestInFixed(steadyModel, simWarm, steadyModel.dur)

	tb := newTable("scenario", "uber(0.9x)", "uber(1.0x)", "uber(1.1x)", "oracle fixed", "agile balanced", "agile slo", "agile tput", "agile default")
	for _, sc := range []struct {
		name string
		par  func(time.Duration) float64
	}{
		{"steady", func(time.Duration) float64 { return par }},
		{"par 150->105", func(t time.Duration) float64 {
			if t < change {
				return par
			}
			return 105
		}},
		{"par 150->60", func(t time.Duration) float64 {
			if t < change {
				return par
			}
			return 60
		}},
		{"par 60->150", func(t time.Duration) float64 {
			if t < change {
				return 60
			}
			return par
		}},
		{"drift ±30%, 60s", func(t time.Duration) float64 {
			return par * (1 + 0.3*math.Sin(2*math.Pi*t.Seconds()/60))
		}},
		{"drift ±30%, 600s", func(t time.Duration) float64 {
			return par * (1 + 0.3*math.Sin(2*math.Pi*t.Seconds()/600))
		}},
	} {
		m := inModel{base: base, par: sc.par, slo: slo, clients: 1200, dur: dur}
		cells := []string{sc.name}
		for _, f := range []float64{0.9, 1.0, 1.1} {
			g, _, ok := runInFixed(m, healthy*f).score(simWarm, dur)
			cells = append(cells, fmt.Sprintf("%s (%s)", f0(g), pct(ok)))
		}
		_, oracle := bestInFixed(m, simWarm, dur)
		cells = append(cells, f0(oracle))
		var agile float64
		for _, arm := range outArms {
			g, _, ok := runInAdaptive(m, simInCfg(slo, arm.preset)).score(simWarm, dur)
			if arm.preset == PresetBalanced {
				agile = g
			}
			cells = append(cells, fmt.Sprintf("%s (%s)", f0(g), pct(ok)))
		}
		tb.row(cells...)

		if agile < 0.95*oracle {
			t.Errorf("inbound %s: agile %.0f good/s is under 95%% of the best fixed admission rate %.0f", sc.name, agile, oracle)
		}
	}
	t.Logf("inbound: base 150ms, SLO 300ms, par 150 (capacity 1000/s), 1200 closed-loop clients; scored 600s,\n"+
		"capacity changes at t=120s; uber(k x) admits k x the best fixed rate for the steady service (%.0f/s);\n"+
		"cells are good/s (share inside the SLO). The fluid model has no arrival noise, which flatters uber(1.0x)%s",
		healthy, tb)
}
