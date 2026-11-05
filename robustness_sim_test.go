package agilelimiter

// Robustness: what corrupted input does to the controller.
//
// The controller has one input, the stream of observed latencies, and cannot
// tell a true sample from a false one. Every experiment below runs the outbound
// model from advantage_sim_test.go, lets the *requests* see the true latency (so
// goodput is scored honestly) and rewrites what the *limiter* observes. Two ways
// to break it are measured:
//
//   - break-through: false samples make it send more than the downstream can
//     take (overload ticks, requests outside the SLO);
//   - knock-down: false samples make it throttle a healthy downstream (lost
//     goodput, time to recover once the interference stops).
//
// Print the tables with:
//
//	go test -run '^TestRobust' -v .

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
)

// tamper rewrites the latencies one tick produced before the limiter sees them.
// It may modify real in place and return it, drop entries, or append more.
type tamper func(t time.Duration, rng *rand.Rand, real []time.Duration) []time.Duration

type robustRun struct {
	tr    trace
	rates []float64
	// over counts scored ticks whose rate exceeded the downstream's capacity:
	// the backlog grew on each of them.
	over int
}

func (r robustRun) meanRate(from, to time.Duration) float64 {
	var s float64
	i, j := int(from/simDt), int(to/simDt)
	for _, v := range r.rates[i:j] {
		s += v
	}
	return s / float64(j-i)
}

func runOutTampered(m outModel, cfg Config, tp tamper) robustRun {
	l := New(cfg)
	rng := rand.New(rand.NewSource(1))
	trng := rand.New(rand.NewSource(2))
	var q outQueue
	var carry float64
	var rr robustRun
	buf := make([]time.Duration, 0, simMaxSamples)
	for t := time.Duration(0); t < m.dur; t += simDt {
		rate := l.Rate()
		lat := q.latency(m, rate, t)
		sent := rate * simDt.Seconds()
		carry += sent
		n := int(carry)
		carry -= float64(n)
		if n > simMaxSamples {
			n = simMaxSamples
		}
		buf = buf[:0]
		for i := 0; i < n; i++ {
			buf = append(buf, time.Duration(lat*math.Exp(simSigma*rng.NormFloat64())*1e9))
		}
		obs := buf
		if tp != nil {
			obs = tp(t, trng, buf)
		}
		for _, d := range obs {
			l.Observe(d)
		}
		l.ctrl.step()
		rr.tr = append(rr.tr, tickRec{sent: sent, good: sent * goodFraction(lat, m.slo)})
		rr.rates = append(rr.rates, rate)
		if t >= simWarm && rate > m.capacity(t) {
			rr.over++
		}
	}
	return rr
}

// replaceFrac replaces each sample with probability f while t is in [from, to).
func replaceFrac(f float64, from, to time.Duration, v func(real time.Duration) time.Duration) tamper {
	return func(t time.Duration, rng *rand.Rand, real []time.Duration) []time.Duration {
		if t < from || t >= to {
			return real
		}
		for i := range real {
			if rng.Float64() < f {
				real[i] = v(real[i])
			}
		}
		return real
	}
}

func fixedSample(d time.Duration) func(time.Duration) time.Duration {
	return func(time.Duration) time.Duration { return d }
}

type robustArm struct {
	name string
	cfg  func(slo time.Duration) Config
}

func (a robustArm) is(b robustArm) bool { return a.name == b.name }

func armBalanced(slo time.Duration) Config { return simOutCfg(slo, PresetBalanced, 600) }

var (
	rBalanced = robustArm{"balanced", armBalanced}
	rDefault  = robustArm{"default", func(slo time.Duration) Config { return simOutCfg(slo, PresetNone, 600) }}
	rTailTol  = robustArm{"balanced+Tol 5%", func(slo time.Duration) Config { c := armBalanced(slo); c.TailTolerance = 0.05; return c }}
	rSLOAnch  = robustArm{"latency-first", func(slo time.Duration) Config { return simOutCfg(slo, PresetLatencyFirst, 600) }}
	rNoSLO    = robustArm{"no SLOTarget", func(time.Duration) Config { return armBalanced(0) }}
	rFloor80  = robustArm{"FloorRate 80", func(slo time.Duration) Config { c := armBalanced(slo); c.FloorRate = 80; return c }}
	rBackoff2 = robustArm{"BackoffTicks 2", func(slo time.Duration) Config { c := armBalanced(slo); c.BackoffTicks = 2; return c }}
)

const (
	rBase = 20 * time.Millisecond
	rSLO  = 100 * time.Millisecond
	rCap  = 150.0
)

func steadyModel(dur time.Duration) outModel {
	return outModel{base: constBase(rBase), capacity: constCap(rCap), slo: rSLO, dur: dur}
}

func cell(rr robustRun, from, to time.Duration) string {
	g, _, ok := rr.tr.score(from, to)
	return fmt.Sprintf("%s (%s) r=%s", f0(g), pct(ok), f0(rr.meanRate(from, to)))
}

// A share of the observed stream answers in ~0: refused connections nobody
// classified, cache hits, a fallback path. The downstream itself is real and
// will melt above ~107 req/s. Break-through means: sending more than that.
func TestRobustFakeFast(t *testing.T) {
	m := steadyModel(simWarm + 600*time.Second)
	arms := []robustArm{rDefault, rBalanced, rTailTol, rSLOAnch, rNoSLO}
	header := []string{"fake-fast share"}
	for _, a := range arms {
		header = append(header, a.name)
	}
	tb := newTable(header...)
	for _, f := range []float64{0, 0.2, 0.4, 0.5, 0.6, 0.8, 0.9, 0.95, 0.99} {
		cells := []string{pct(f * 100)}
		for _, a := range arms {
			rr := runOutTampered(m, a.cfg(rSLO), replaceFrac(f, 0, m.dur, fixedSample(time.Millisecond)))
			cells = append(cells, cell(rr, simWarm, m.dur)+fmt.Sprintf(" over=%d%%", 100*rr.over/int((m.dur-simWarm)/simDt)))

			// The p99 guard sees the real slow requests through any amount of fake
			// fast ones, as long as more than 1% of the stream is real.
			_, _, ok := rr.tr.score(simWarm, m.dur)
			guarded := (a.is(rBalanced) && f >= 0.6 && f <= 0.95) || (a.is(rSLOAnch) && f <= 0.95)
			if guarded && (rr.over > 0 || ok < 85) {
				t.Errorf("%s, %.0f%% fake-fast: broke through (%d ticks above capacity, %.0f%% inside the SLO)",
					a.name, f*100, rr.over, ok)
			}
			// The default's ratio guard is diluted instead: the real requests'
			// share of the stream is what it sees, so it holds the capacity
			// while that share is well above its 5% tolerance, and 95% fake
			// answers leave nothing to see. Fast answers are the verdict's job.
			if a.is(rDefault) && f <= 0.9 && rr.over > 0 {
				t.Errorf("default, %.0f%% fake-fast: %d ticks above capacity", f*100, rr.over)
			}
		}
		tb.row(cells...)
	}
	t.Logf("capacity 150/s, base 20ms, SLO 100ms; a share of observations replaced by 1ms\n"+
		"cells: good/s (share of real requests inside the SLO) r=mean rate, over=ticks above capacity%s", tb)
}

// A share of the observed stream is absurdly slow: GC pauses on the caller, a
// stuck connection, a 10s outlier. The downstream is healthy. Knock-down means:
// throttling it anyway.
func TestRobustFakeSlow(t *testing.T) {
	m := steadyModel(simWarm + 600*time.Second)
	arms := []robustArm{rDefault, rBalanced, rTailTol, rSLOAnch, rFloor80}
	cleanTol := runOutTampered(m, rTailTol.cfg(rSLO), nil).meanRate(simWarm, m.dur)
	header := []string{"outlier share"}
	for _, a := range arms {
		header = append(header, a.name)
	}
	tb := newTable(header...)
	for _, f := range []float64{0, 0.002, 0.005, 0.01, 0.02, 0.04, 0.08, 0.2, 0.5} {
		cells := []string{fmt.Sprintf("%.1f%%", f*100)}
		for _, a := range arms {
			rr := runOutTampered(m, a.cfg(rSLO), replaceFrac(f, 0, m.dur, fixedSample(10*time.Second)))
			cells = append(cells, cell(rr, simWarm, m.dur))

			r := rr.meanRate(simWarm, m.dur)
			if a.is(rFloor80) && r < 80 {
				t.Errorf("FloorRate 80, %.1f%% outliers: mean rate %.0f went below the floor", f*100, r)
			}
			if a.is(rTailTol) && f <= 0.04 && r < 0.9*cleanTol {
				t.Errorf("TailTolerance 5%%, %.1f%% outliers: mean rate %.0f, clean %.0f", f*100, r, cleanTol)
			}
		}
		tb.row(cells...)
	}
	t.Logf("healthy downstream (capacity 150/s, base 20ms, SLO 100ms); a share of observations replaced by 10s\n"+
		"cells: good/s (share inside the SLO) r=mean rate%s", tb)
}

// A burst of fake-fast samples, then clean data. The baseline follows
// improvements down with a 5s half-life and climbs back at most 0.2% per tick,
// so a short burst can leave it far below the real latency.
func TestRobustBaselineDragDown(t *testing.T) {
	start := simWarm + 120*time.Second
	after := 3600 * time.Second
	arms := []robustArm{rDefault, rBalanced, rSLOAnch, rFloor80}
	header := []string{"burst"}
	for _, a := range arms {
		header = append(header, a.name, "over", "recover")
	}
	tb := newTable(header...)

	clean := runOutTampered(steadyModel(simWarm+300*time.Second), armBalanced(rSLO), nil)
	settled := clean.tr.goodput(simWarm, simWarm+300*time.Second)

	for _, burst := range []struct {
		share float64
		span  time.Duration
	}{
		{0.4, 300 * time.Second}, {0.6, 10 * time.Second}, {0.6, 60 * time.Second}, {1, 10 * time.Second}, {1, 60 * time.Second}, {1, 300 * time.Second},
	} {
		end := start + burst.span
		m := steadyModel(end + after)
		cells := []string{fmt.Sprintf("%.0f%% for %v", burst.share*100, burst.span)}
		for _, a := range arms {
			rr := runOutTampered(m, a.cfg(rSLO), replaceFrac(burst.share, start, end, fixedSample(time.Millisecond)))
			cells = append(cells, cell(rr, end, end+600*time.Second),
				fmt.Sprintf("%ds", overIn(rr, m, start, end)), recoverTime(rr.tr, end, settled))

			// A partial burst leaves real samples in every tick; only the
			// baseline-anchored window can be dragged down by it.
			if !a.is(rBalanced) && burst.share < 1 {
				if s := recoverSecs(rr.tr, end, settled); s < 0 || s > 5 {
					t.Errorf("%s, %.0f%% fake-fast for %v: recovery took %ds", a.name, burst.share*100, burst.span, s)
				}
			}
			if s := recoverSecs(rr.tr, end, settled); s < 0 {
				t.Errorf("%s, %.0f%% fake-fast for %v: never recovered", a.name, burst.share*100, burst.span)
			}
		}
		tb.row(cells...)
	}
	t.Logf("healthy downstream; fake 1ms samples for a while starting at t=120s, then clean data\n"+
		"cells: the 600s after the burst, good/s (share inside the SLO) r=mean rate; over = seconds above capacity\n"+
		"during the burst; clean settles at %.0f good/s; recover = seconds after the burst until the 30s rolling\n"+
		"goodput is back to 90%% of that%s", settled, tb)
}

// overIn counts the ticks in [from, to) whose rate exceeded capacity.
func overIn(rr robustRun, m outModel, from, to time.Duration) int {
	n := 0
	for t := from; t < to; t += simDt {
		if rr.rates[int(t/simDt)] > m.capacity(t) {
			n++
		}
	}
	return n
}

// Ten seconds of nonsense values, then clean data: how long does each kind of
// garbage keep hurting? Negative durations come from subtracting timestamps
// taken on two hosts; zero from a timer that was never started; MaxInt64 from
// an overflowed subtraction.
func TestRobustGarbageRecovery(t *testing.T) {
	start := simWarm + 120*time.Second
	end := start + 10*time.Second
	arms := []robustArm{rDefault, rBalanced, rSLOAnch, rFloor80}
	header := []string{"10s of"}
	for _, a := range arms {
		header = append(header, a.name, "recover")
	}
	tb := newTable(header...)

	clean := runOutTampered(steadyModel(simWarm+300*time.Second), armBalanced(rSLO), nil)
	settled := clean.tr.goodput(simWarm, simWarm+300*time.Second)

	for _, g := range []struct {
		name string
		d    time.Duration
	}{
		{"-1ms", -time.Millisecond},
		{"0", 0},
		{"1ns", 1},
		{"1h", time.Hour},
		{"MaxInt64", math.MaxInt64},
	} {
		m := steadyModel(end + 3600*time.Second)
		cells := []string{g.name}
		for _, a := range arms {
			rr := runOutTampered(m, a.cfg(rSLO), replaceFrac(1, start, end, fixedSample(g.d)))
			cells = append(cells, cell(rr, end, end+600*time.Second), recoverTime(rr.tr, end, settled))
			if s := recoverSecs(rr.tr, end, settled); s < 0 || s > 1800 {
				t.Errorf("%s after 10s of %s: recovery took %ds", a.name, g.name, s)
			}
		}
		tb.row(cells...)
	}
	t.Logf("healthy downstream; every observation replaced by one value for 10s, then clean data\n"+
		"cells: the 600s after, good/s (share inside the SLO) r=mean rate; recover as in DragDown (clean = %.0f good/s)%s",
		settled, tb)
}

// Whatever the input, the clamps hold, nothing panics, and Stats stays finite —
// for both limiters, with Observe, Unavailable and ObserveOutcome interleaved.
func TestRobustGarbageNeverEscapes(t *testing.T) {
	const probe, floor, max = 5, 20, 200
	garbage := []time.Duration{math.MinInt64, -time.Hour, -1, 0, 1, math.MaxInt64, math.MaxInt64 / 2, 24 * time.Hour}
	for _, tol := range []float64{0, 0.05} {
		l := New(Config{ProbeRate: probe, FloorRate: floor, MaxRate: max, InitialRate: 100,
			SLOTarget: 100 * time.Millisecond, TailTolerance: tol})
		c := NewConcurrency(ConcurrencyConfig{ProbeLimit: probe, FloorLimit: floor, MaxLimit: max, InitialLimit: 100,
			SLOTarget: 100 * time.Millisecond, TailTolerance: tol})
		rng := rand.New(rand.NewSource(3))
		errDown := fmt.Errorf("down")
		for i := 0; i < 20000; i++ {
			for j, n := 0, rng.Intn(30); j < n; j++ {
				var d time.Duration
				switch rng.Intn(3) {
				case 0:
					d = garbage[rng.Intn(len(garbage))]
				case 1:
					d = time.Duration(rng.Uint64())
				default:
					d = time.Duration(rng.Intn(500)) * time.Millisecond
				}
				switch rng.Intn(10) {
				case 0:
					l.Unavailable()
					c.Unavailable()
				case 1:
					l.ObserveOutcome(d, errDown)
					c.ObserveOutcome(d, errDown)
				default:
					l.Observe(d)
					c.Observe(d)
				}
			}
			l.ctrl.step()
			c.ctrl.step()
			for _, s := range []Stats{l.Stats(), c.Stats()} {
				if s.Rate < probe || s.Rate > max || math.IsNaN(s.Rate) {
					t.Fatalf("tol=%v tick %d: rate %v escaped [%d, %d]", tol, i, s.Rate, probe, max)
				}
				if s.Baseline < 0 || s.Fast < 0 {
					t.Fatalf("tol=%v tick %d: negative signal: baseline %v fast %v", tol, i, s.Baseline, s.Fast)
				}
				for _, v := range []float64{s.TailRatio, s.TailThreshold} {
					if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
						t.Fatalf("tol=%v tick %d: non-finite tail stat %v", tol, i, v)
					}
				}
			}
			if got := c.Limit(); got < probe || got > max {
				t.Fatalf("tol=%v tick %d: concurrency limit %d escaped [%d, %d]", tol, i, got, probe, max)
			}
		}
	}
}

// Caller-side overhead that grows slowly — a leak, lock contention, a GC that
// gets worse — is added to every sample. The baseline rises with it, and the
// tolerated window rises twice as fast, so the *real* latency the controller
// accepts rises too.
func TestRobustCreepingOverhead(t *testing.T) {
	arms := []robustArm{rDefault, rBalanced, rSLOAnch, rNoSLO}
	header := []string{"overhead"}
	for _, a := range arms {
		header = append(header, a.name)
	}
	tb := newTable(header...)
	ramp := 1200 * time.Second
	for _, top := range []time.Duration{10 * time.Millisecond, 30 * time.Millisecond, 60 * time.Millisecond, 120 * time.Millisecond} {
		m := steadyModel(simWarm + ramp + 600*time.Second)
		tp := func(t time.Duration, _ *rand.Rand, real []time.Duration) []time.Duration {
			if t < simWarm {
				return real
			}
			frac := math.Min(1, float64(t-simWarm)/float64(ramp))
			add := time.Duration(frac * float64(top))
			for i := range real {
				real[i] += add
			}
			return real
		}
		cells := []string{fmt.Sprintf("0->%v over %v", top, ramp)}
		for _, a := range arms {
			rr := runOutTampered(m, a.cfg(rSLO), tp)
			cells = append(cells, cell(rr, simWarm+ramp, m.dur)+fmt.Sprintf(" over=%d", rr.over))

			// With an SLOTarget the window is capped on the *observed* latency,
			// so overhead can only make the limiter more conservative. The
			// default tolerates 5% of the traffic above the SLO by design.
			want := 99.0
			if a.is(rDefault) {
				want = 95
			}
			if _, _, ok := rr.tr.score(simWarm, m.dur); !a.is(rNoSLO) && ok < want {
				t.Errorf("%s, overhead up to %v: only %.0f%% of real requests inside the SLO", a.name, top, ok)
			}
		}
		tb.row(cells...)
	}
	t.Logf("capacity 150/s, base 20ms, SLO 100ms; caller overhead ramps up over 20min, scored the 600s after;\n"+
		"cells: good/s (share of REAL requests inside the SLO) r=mean rate over=ticks above capacity%s", tb)
}

// runInTampered is runInAdaptive with the observed stream rewritten.
func runInTampered(m inModel, cfg ConcurrencyConfig, tp tamper) trace {
	l := NewConcurrency(cfg)
	rng := rand.New(rand.NewSource(1))
	trng := rand.New(rand.NewSource(2))
	b := m.base.Seconds()
	buf := make([]time.Duration, 0, simMaxSamples)
	tr := make(trace, 0, int(m.dur/simDt))
	for t := time.Duration(0); t < m.dur; t += simDt {
		par := m.par(t)
		inflight := math.Min(float64(l.Limit()), m.clients)
		lat := b * math.Max(1, inflight/par)
		done := inflight / lat * simDt.Seconds()
		buf = buf[:0]
		for i := 0; i < int(done) && i < simMaxSamples; i++ {
			buf = append(buf, time.Duration(lat*math.Exp(simSigma*rng.NormFloat64())*1e9))
		}
		obs := buf
		if tp != nil {
			obs = tp(t, trng, buf)
		}
		for _, d := range obs {
			l.Observe(d)
		}
		l.ctrl.step()
		tr = append(tr, tickRec{sent: done, good: done * goodFraction(lat, m.slo)})
	}
	return tr
}

// The inbound side runs the same controller, so the same interference applies —
// and on this side the fake-fast samples are cheaper to produce: a 404, a 400
// or a 401 answers in microseconds, DefaultVerdict classifies it Reached, and
// anyone on the internet can send them.
func TestRobustInbound(t *testing.T) {
	const base, slo = 150 * time.Millisecond, 300 * time.Millisecond
	model := func(dur time.Duration) inModel {
		return inModel{base: base, par: func(time.Duration) float64 { return 150 }, slo: slo, clients: 1200, dur: dur}
	}
	type inArm struct {
		name string
		cfg  ConcurrencyConfig
	}
	floor := simInCfg(slo, PresetBalanced)
	floor.FloorLimit = 100
	arms := []inArm{
		{"default", simInCfg(slo, PresetNone)},
		{"balanced", simInCfg(slo, PresetBalanced)},
		{"latency-first", simInCfg(slo, PresetLatencyFirst)},
		{"FloorLimit 100", floor},
	}
	header := []string{"interference"}
	for _, a := range arms {
		header = append(header, a.name, "recover")
	}
	tb := newTable(header...)

	clean := model(simWarm + 300*time.Second)
	settled := runInTampered(clean, arms[0].cfg, nil).goodput(simWarm, clean.dur)

	start := simWarm + 120*time.Second
	for _, sc := range []struct {
		name string
		span time.Duration
		tp   func(end time.Duration) tamper
	}{
		{"40% fake 0.1ms, steady", 0, func(end time.Duration) tamper {
			return replaceFrac(0.4, 0, end, fixedSample(100*time.Microsecond))
		}},
		{"1% outliers 10s, steady", 0, func(end time.Duration) tamper {
			return replaceFrac(0.01, 0, end, fixedSample(10*time.Second))
		}},
		{"60% fake 0.1ms for 10s", 10 * time.Second, func(end time.Duration) tamper {
			return replaceFrac(0.6, start, end, fixedSample(100*time.Microsecond))
		}},
		{"60% fake 0.1ms for 60s", 60 * time.Second, func(end time.Duration) tamper {
			return replaceFrac(0.6, start, end, fixedSample(100*time.Microsecond))
		}},
		{"10s of 0", 10 * time.Second, func(end time.Duration) tamper {
			return replaceFrac(1, start, end, fixedSample(0))
		}},
	} {
		// Steady rows interfere for the whole run and score it; burst rows score
		// the 600s after the burst.
		from, end := simWarm, simWarm+600*time.Second
		m := model(end)
		if sc.span > 0 {
			end = start + sc.span
			from = end
			m = model(end + 3600*time.Second)
		}
		cells := []string{sc.name}
		for _, a := range arms {
			tr := runInTampered(m, a.cfg, sc.tp(end))
			g, _, ok := tr.score(from, from+600*time.Second)
			rec := "-"
			if sc.span > 0 {
				rec = recoverTime(tr, end, settled)
			}
			cells = append(cells, fmt.Sprintf("%s (%s)", f0(g), pct(ok)), rec)

			if a.name == "FloorLimit 100" && g < 0.6*settled {
				t.Errorf("inbound %s, %s: %.0f good/s; the floor should bound the damage (clean %.0f)", a.name, sc.name, g, settled)
			}
		}
		tb.row(cells...)
	}
	t.Logf("inbound: base 150ms, SLO 300ms, par 150 (capacity 1000/s); clean settles at %.0f good/s\n"+
		"cells: good/s over 600s (steady rows: whole run; burst rows: after the burst), recover as in DragDown%s", settled, tb)
}

// A fast population that is simply part of the traffic — cache hits nobody
// marked, a cheap endpoint sharing the limiter with an expensive one, 304s,
// validation 4xx. The median law is fine when that share is clearly below or
// clearly above one half; near one half, tick-to-tick sampling noise puts the
// median on the fast side now and then, the baseline follows it down at once and
// climbs back at 0.2% per tick. How wide that band is depends on the sample
// count, and a share that drifts through the day crosses it twice.
func TestRobustFastShareBand(t *testing.T) {
	m := steadyModel(simWarm + 600*time.Second)
	cleanOut := runOutTampered(m, armBalanced(rSLO), nil).meanRate(simWarm, m.dur)

	inM := inModel{base: 150 * time.Millisecond, par: func(time.Duration) float64 { return 150 },
		slo: 300 * time.Millisecond, clients: 1200, dur: simWarm + 600*time.Second}
	cleanIn := runInTampered(inM, simInCfg(inM.slo, PresetBalanced), nil).goodput(simWarm, inM.dur)

	tb := newTable("fast share", "outbound (~90 samples/tick)", "inbound (~1000 samples/tick)")
	for _, f := range []float64{0.2, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.7} {
		out := runOutTampered(m, armBalanced(rSLO), replaceFrac(f, 0, m.dur, fixedSample(time.Millisecond)))
		in := runInTampered(inM, simInCfg(inM.slo, PresetBalanced), replaceFrac(f, 0, inM.dur, fixedSample(100*time.Microsecond)))
		tb.row(pct(f*100),
			fmt.Sprintf("%.0f%% of clean rate", 100*out.meanRate(simWarm, m.dur)/cleanOut),
			fmt.Sprintf("%.0f%% of clean goodput", 100*in.goodput(simWarm, inM.dur)/cleanIn))
	}
	t.Logf("balanced (the relative law); a steady share of observations is a fast population (outbound 1ms, inbound 0.1ms)%s", tb)

	// The share drifts 30% -> 70% -> 30% over an hour, as a cache hit ratio does.
	const period = time.Hour
	drift := func(fast time.Duration) tamper {
		return func(t time.Duration, rng *rand.Rand, real []time.Duration) []time.Duration {
			f := 0.5 - 0.2*math.Cos(2*math.Pi*t.Seconds()/period.Seconds())
			for i := range real {
				if rng.Float64() < f {
					real[i] = fast
				}
			}
			return real
		}
	}
	dm := steadyModel(simWarm + 2*period)
	dIn := inM
	dIn.dur = simWarm + 2*period
	tb2 := newTable("config", "outbound rate", "inbound goodput")
	for _, a := range []struct {
		name string
		out  Config
		in   ConcurrencyConfig
	}{
		{"default", simOutCfg(rSLO, PresetNone, 600), simInCfg(inM.slo, PresetNone)},
		{"balanced", armBalanced(rSLO), simInCfg(inM.slo, PresetBalanced)},
		{"latency-first", simOutCfg(rSLO, PresetLatencyFirst, 600), simInCfg(inM.slo, PresetLatencyFirst)},
	} {
		out := runOutTampered(dm, a.out, drift(time.Millisecond)).meanRate(simWarm, dm.dur)
		in := runInTampered(dIn, a.in, drift(100*time.Microsecond)).goodput(simWarm, dIn.dur)
		tb2.row(a.name, fmt.Sprintf("%.0f%% of clean", 100*out/cleanOut), fmt.Sprintf("%.0f%% of clean", 100*in/cleanIn))
	}
	t.Logf("fast share drifting 30%% -> 70%% -> 30%% with a 1h period, two periods scored%s", tb2)
}

// Only a few samples per tick reach the controller: low traffic, or sampling
// one request in a hundred. The downstream is healthy but jittery.
func TestRobustSparseSamples(t *testing.T) {
	m := steadyModel(simWarm + 600*time.Second)
	arms := []robustArm{rDefault, rBalanced, rBackoff2, rSLOAnch}
	header := []string{"samples/tick"}
	for _, a := range arms {
		header = append(header, a.name)
	}
	tb := newTable(header...)
	for _, k := range []int{1, 2, 3, 5, 10, 1000} {
		keep := k
		tp := func(_ time.Duration, _ *rand.Rand, real []time.Duration) []time.Duration {
			if len(real) > keep {
				return real[:keep]
			}
			return real
		}
		cells := []string{fmt.Sprint(k)}
		for _, a := range arms {
			rr := runOutTampered(m, a.cfg(rSLO), tp)
			cells = append(cells, cell(rr, simWarm, m.dur))
			// At a few samples a tick the ratio guard's 3σ margin is wide, so
			// the default runs a little closer to the SLO.
			want := 93.0
			if a.is(rDefault) {
				want = 90
			}
			if g, _, ok := rr.tr.score(simWarm, m.dur); g < 70 || ok < want {
				t.Errorf("%s with %d samples/tick: %.0f good/s, %.0f%% inside the SLO", a.name, k, g, ok)
			}
		}
		tb.row(cells...)
	}
	t.Logf("capacity 150/s, base 20ms, SLO 100ms, jitter sigma 0.2; only the first k samples of each tick are observed%s", tb)
}
