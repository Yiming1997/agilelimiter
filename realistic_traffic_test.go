package agilelimiter_test

// Realistic traffic: endpoint mixes with heavy-tailed latency, every kind of
// fast answer and error a production service sees, and a 150-minute timeline
// of the incidents a limiter exists for.
//
// Every call goes through the real verdicts and the adapters' routing (see
// filtered_traffic_test.go). The arms differ only in what decides the rate:
// nothing, a fixed rate tuned for the normal peak (go.uber.org/ratelimit), or
// agilelimiter in three configurations.
//
// Latency of one request: endpoint base x load slowdown x lognormal(sigma 0.35)
// x, for one request in a hundred, a Pareto(alpha 1.5) pause from 3x to 30x — a
// GC, a slow query, a cold cache line. The slowdown stays near 1 with headroom
// and climbs steeply near saturation, and overload leaves a backlog that later
// ticks inherit.
//
//	go test -run '^TestRealistic' -v .

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"syscall"
	"testing"
	"time"

	"agilelimiter"
)

const (
	rtDt      = time.Second
	rtDur     = 150 * time.Minute
	rtSigma   = 0.35
	rtTailP   = 0.01
	rtCancel  = 0.01 // calls the caller abandons before the answer
	rtNoLimit = math.MaxFloat64
)

type phase struct {
	name     string
	from, to time.Duration
}

func (p phase) label() string {
	return fmt.Sprintf("%s %.0f-%.0fm", p.name, p.from.Minutes(), p.to.Minutes())
}

func phaseAt(ps []phase, t time.Duration) int {
	for i, p := range ps {
		if t < p.to {
			return i
		}
	}
	return len(ps) - 1
}

func minutes(m float64) time.Duration { return time.Duration(m * float64(time.Minute)) }

// rtTail is one request's latency multiplier: a lognormal body and a Pareto tail.
func rtTail(rng *rand.Rand) float64 {
	m := math.Exp(rtSigma * rng.NormFloat64())
	if rng.Float64() < rtTailP {
		m *= math.Min(30, 3/math.Pow(1-rng.Float64(), 1/1.5))
	}
	return m
}

// slowdown is the load curve: flat with headroom, steep near saturation —
// gentler than M/M/1, which already doubles latency at half load.
func slowdown(util float64) float64 {
	u := math.Min(math.Max(util, 0), 0.98)
	return 1 + math.Pow(u, 4)/(1-u)
}

func clamp(v, lo, hi float64) float64 { return math.Min(math.Max(v, lo), hi) }

func dur(sec float64) time.Duration { return time.Duration(sec * 1e9) }

// rtScore is what one arm delivered in one phase. Calls the caller abandoned,
// scanners and health checks are not scored: nobody was waiting for them. A
// failure the client retries is not counted until its last attempt.
type rtScore struct {
	good     float64 // useful answers inside the SLO
	bad      float64 // errors, timeouts, answers outside the SLO
	held     float64 // calls our own limiter refused or held back
	overload int     // ticks in which the protected resource was past saturation
	secs     float64

	// Why the limiter stepped down, tick by tick: the median law, the tail
	// guard, or an outage tick.
	median, tail, outage int
	// end is the limiter's state at the end of the phase.
	end agilelimiter.Stats
}

// rtRetry is the share of failed attempts a client retries: with up to two
// retries per call, about two in three failures get another attempt.
const rtRetry = 0.67

// backoffCause names what made one tick step the rate down, from what the tick
// fed the limiter and what Stats reports after it.
func backoffCause(sc *rtScore, unreachable, reached int, s agilelimiter.Stats, tailTol float64, slo time.Duration) {
	switch {
	case unreachable > 0 && unreachable >= 2*reached:
		sc.outage++
	case tailTol > 0 && s.TailRatio > s.TailThreshold, tailTol == 0 && s.P99 > slo:
		sc.tail++
	default:
		sc.median++
	}
}

func (s rtScore) cell() string {
	total := s.good + s.bad + s.held
	fail := 0.0
	if total > 0 {
		fail = 100 * (s.bad + s.held) / total
	}
	return fmt.Sprintf("%.0f (%.1f%%)", s.good/s.secs, fail)
}

type rtArm struct {
	name  string
	fixed float64 // the fixed rate, or rtNoLimit; ignored when an agile config is set
	out   *agilelimiter.Config
	in    *agilelimiter.ConcurrencyConfig
}

// rtReport logs the tables and returns each arm's whole-run score.
func rtReport(t *testing.T, title string, phases []phase, arms []rtArm, scores [][]rtScore) []rtScore {
	t.Helper()
	tb := &ftTable{}
	head := []string{"phase"}
	for _, a := range arms {
		head = append(head, a.name)
	}
	tb.row(head...)
	ov := &ftTable{}
	ov.row(head...)
	total := make([]rtScore, len(arms))
	for p := range phases {
		row := []string{phases[p].label()}
		orow := []string{phases[p].label()}
		for a := range arms {
			s := scores[a][p]
			row = append(row, s.cell())
			orow = append(orow, fmt.Sprintf("%ds", s.overload))
			total[a].good += s.good
			total[a].bad += s.bad
			total[a].held += s.held
			total[a].overload += s.overload
			total[a].secs += s.secs
		}
		tb.row(row...)
		ov.row(orow...)
	}
	row := []string{"whole run"}
	orow := []string{"whole run"}
	for a := range arms {
		row = append(row, total[a].cell())
		orow = append(orow, fmt.Sprintf("%ds", total[a].overload))
	}
	tb.row(row...)
	ov.row(orow...)

	why := &ftTable{}
	whyHead := []string{"phase"}
	var agile []int
	for a, arm := range arms {
		if arm.out != nil || arm.in != nil {
			agile = append(agile, a)
			whyHead = append(whyHead, arm.name)
		}
	}
	why.row(whyHead...)
	for p := range phases {
		row := []string{phases[p].label()}
		for _, a := range agile {
			s := scores[a][p]
			row = append(row, fmt.Sprintf("held %.0f/s  m%d t%d o%d | end: %.0f base %v p50 %v",
				s.held/s.secs, s.median, s.tail, s.outage,
				s.end.Rate, s.end.Baseline.Round(time.Millisecond), s.end.P50.Round(time.Millisecond)))
		}
		why.row(row...)
	}
	t.Logf("%s\ngood answers/s (share of wanted calls that failed, were too slow, or were held back by the limiter)%s\n\n"+
		"seconds the protected resource spent past saturation%s\n\n"+
		"agile: calls held back per second, and backoff ticks by cause (m median law, t tail guard, o outage)%s",
		title, tb, ov, why)
	return total
}

// rtCheck pins the two properties the realistic runs support: every agile arm
// keeps the protected resource out of saturation far better than a fixed rate
// tuned for the normal peak, and the configuration built for heterogeneous,
// heavy-tailed traffic does so without giving up goodput against it.
func rtCheck(t *testing.T, arms []rtArm, total []rtScore, fixed, tuned int) {
	t.Helper()
	for i, a := range arms {
		if a.out == nil && a.in == nil {
			continue
		}
		if total[i].overload > total[fixed].overload*3/10 {
			t.Errorf("%s: %ds past saturation, %s %ds", a.name, total[i].overload, arms[fixed].name, total[fixed].overload)
		}
	}
	if g, f := total[tuned].good, total[fixed].good; g < 0.95*f {
		t.Errorf("%s: %.0f good answers, %s %.0f", arms[tuned].name, g, arms[fixed].name, f)
	}
}

// ---------------------------------------------------------------------------
// outbound: callers -> Limiter -> httpclient.Wrap -> CDN -> origin
// ---------------------------------------------------------------------------

type outWorld struct {
	demand   float64 // calls/s the callers want to make
	hit      float64 // CDN hit ratio
	capacity float64 // origin capacity, cost units/s
	baseMul  float64 // origin idle latency multiplier
	refused  float64 // share of origin calls refused at connect
	err503   float64 // share of origin calls answered 503 by a broken replica
}

var outPhases = []phase{
	{"normal", 0, minutes(30)},
	{"rolling deploy", minutes(30), minutes(36)},
	{"normal", minutes(36), minutes(50)},
	{"CDN purge", minutes(50), minutes(60)},
	{"normal", minutes(60), minutes(75)},
	{"slow DB", minutes(75), minutes(85)},
	{"normal", minutes(85), minutes(95)},
	{"1 of 4 replicas 503", minutes(95), minutes(100)},
	{"origin down", minutes(100), minutes(103)},
	{"recovery", minutes(103), minutes(120)},
	{"demand x2", minutes(120), minutes(135)},
	{"normal", minutes(135), minutes(150)},
}

func outTimeline() []outWorld {
	rng := rand.New(rand.NewSource(3))
	ws := make([]outWorld, int(rtDur/rtDt))
	for i := range ws {
		m := (time.Duration(i) * rtDt).Minutes()
		w := outWorld{
			demand:   (300 + 60*math.Sin(2*math.Pi*m/60)) * math.Exp(0.15*rng.NormFloat64()),
			hit:      0.5 + 0.1*math.Sin(2*math.Pi*m/45),
			capacity: 200,
			baseMul:  1,
		}
		switch {
		case m >= 30 && m < 36: // one of four replicas restarting at a time
			w.capacity, w.refused = 150, 0.05
		case m >= 50 && m < 60: // purged, refilling over ten minutes
			w.hit *= (m - 50) / 10
		case m >= 75 && m < 85:
			w.baseMul, w.capacity = 2.5, 100
		case m >= 95 && m < 100:
			w.err503, w.capacity = 0.25, 150
		case m >= 100 && m < 103:
			w.refused = 1
		case m >= 120 && m < 135:
			w.demand *= 2
		}
		ws[i] = w
	}
	return ws
}

type endpointSpec struct {
	share float64
	base  time.Duration
	cost  float64 // origin capacity units per call
}

// A cheap read, a search, and a report that is 5% of calls and 24% of the cost.
var outEndpoints = [...]endpointSpec{
	{0.60, 8 * time.Millisecond, 0.5},
	{0.35, 30 * time.Millisecond, 1},
	{0.05, 150 * time.Millisecond, 4},
}

const (
	outAvgCost = 0.85
	outSLO     = 500 * time.Millisecond
	outTimeout = 2 * time.Second
)

var (
	rt304     = &clientCall{resp: response(304), good: true}
	rt500     = &clientCall{resp: response(500)}
	rtReset   = &clientCall{err: &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}}
	rtTimeout = &clientCall{err: context.DeadlineExceeded}
)

// drawOutCall draws one call and what it measured. scored is false for a call
// nobody waits for.
func drawOutCall(rng *rand.Rand, w outWorld, slow, queue, shed float64) (c *clientCall, d time.Duration, good, scored bool) {
	if rng.Float64() < rtCancel {
		return canceledC, dur(0.005 + 0.05*rng.Float64()), false, false
	}
	if rng.Float64() < w.hit {
		return hitAge, dur(0.002 * rtTail(rng)), true, true
	}
	if rng.Float64() < w.refused {
		return refused, 50 * time.Microsecond, false, true
	}
	if rng.Float64() < shed+w.err503 {
		return upstream, dur(0.002 * rtTail(rng)), false, true
	}
	r := rng.Float64()
	ep := 0
	for x := rng.Float64(); ep < len(outEndpoints)-1 && x > outEndpoints[ep].share; ep++ {
		x -= outEndpoints[ep].share
	}
	lat := outEndpoints[ep].base.Seconds()*w.baseMul*slow*rtTail(rng) + queue
	switch {
	case r < 0.002: // a bug on the origin, answered at once
		return rt500, dur(0.005 * rtTail(rng)), false, true
	case r < 0.003: // the connection dropped mid-response
		return rtReset, dur(lat * rng.Float64()), false, true
	case lat > outTimeout.Seconds():
		return rtTimeout, outTimeout, false, true
	case ep == 0 && r < 0.033:
		return notFound, dur(0.003 * rtTail(rng)), true, true
	case ep == 0 && r < 0.083: // conditional GET, not modified
		return rt304, dur(0.004 * rtTail(rng)), true, true
	}
	return originMiss, dur(lat), lat <= outSLO.Seconds(), true
}

func runRealOutbound(arm rtArm, world []outWorld) []rtScore {
	var lim *agilelimiter.Limiter
	if arm.out != nil {
		lim = agilelimiter.New(*arm.out)
	}
	rng := rand.New(rand.NewSource(7))
	scores := make([]rtScore, len(outPhases))
	var backlog, carry, retries float64
	for i, w := range world {
		sc := &scores[phaseAt(outPhases, time.Duration(i)*rtDt)]
		sc.secs++
		rate := arm.fixed
		if lim != nil {
			rate = lim.Rate()
		}
		w.demand += retries
		retries = 0
		sent := math.Min(w.demand, rate)
		sc.held += (w.demand - sent) * (1 - rtCancel)

		origin := sent * (1 - rtCancel) * (1 - w.hit) * (1 - w.refused)
		util := origin * outAvgCost / w.capacity
		shed := clamp((util-0.95)/0.25, 0, 0.9) // an overloaded origin answers 503 at once
		served := origin * outAvgCost * math.Max(0, 1-shed-w.err503)
		backlog = math.Max(0, backlog+(served-w.capacity)*rtDt.Seconds())
		if util > 1 || backlog > 0 {
			sc.overload++
		}
		slow := slowdown(served / w.capacity)
		queue := backlog / w.capacity

		x := sent + carry
		n := int(x)
		carry = x - float64(n)
		var unreachable, reached int
		for j := 0; j < n; j++ {
			c, d, good, scored := drawOutCall(rng, w, slow, queue, shed)
			if lim != nil {
				switch routeClient(lim, c, d) {
				case agilelimiter.Unreachable:
					unreachable++
				case agilelimiter.Reached:
					reached++
				}
			}
			switch {
			case !scored:
			case good:
				sc.good++
			case c == refused || c == upstream || c == rt500 || c == rtReset || c == rtTimeout:
				sc.bad += 1 - rtRetry
				retries += rtRetry
			default:
				sc.bad++
			}
		}
		if lim != nil {
			agilelimiter.Step(lim)
			sc.end = lim.Stats()
			if sc.end.Rate < 0.99*rate {
				backoffCause(sc, unreachable, reached, sc.end, arm.out.TailTolerance, outSLO)
			}
		}
	}
	return scores
}

func TestRealisticOutbound(t *testing.T) {
	if testing.Short() {
		t.Skip("simulates 150 minutes of traffic; skipped in -short")
	}
	cfg := func(p agilelimiter.Preset, tol float64) *agilelimiter.Config {
		return &agilelimiter.Config{
			MaxRate: 1000, FloorRate: 100, ProbeRate: 10, InitialRate: 300,
			SLOTarget: outSLO, Preset: p, TailTolerance: tol,
		}
	}
	arms := []rtArm{
		{name: "no limiter", fixed: rtNoLimit},
		// The normal peak is 360 calls/s; 380 keeps the origin near 80% busy at
		// the usual hit ratio — what a load test would have picked.
		{name: "uber(380)", fixed: 380},
		{name: "agile default", out: cfg(agilelimiter.PresetNone, 0)},
		{name: "agile latency-first", out: cfg(agilelimiter.PresetLatencyFirst, 0)},
		{name: "agile TailTolerance 5%", out: cfg(agilelimiter.PresetNone, 0.05)},
		{name: "agile latency-first+Tol 5%", out: cfg(agilelimiter.PresetLatencyFirst, 0.05)},
	}
	world := outTimeline()
	scores := make([][]rtScore, len(arms))
	for i, a := range arms {
		scores[i] = runRealOutbound(a, world)
	}
	total := rtReport(t, "outbound: demand 240-360 calls/s with bursts, CDN hits 40-60% (Age), origin 200 units/s, SLO 500ms, timeout 2s;\n"+
		"origin traffic: 404 3%, 304 5% of reads, 500 0.2%, resets 0.1%, 1% canceled, overload answered with 503;\n"+
		"two in three failed attempts are retried",
		outPhases, arms, scores)
	rtCheck(t, arms, total, 1, len(arms)-1)
}

// ---------------------------------------------------------------------------
// inbound: clients -> httpserver.Wrap(ConcurrencyLimiter) -> handlers -> DB
// ---------------------------------------------------------------------------

type inWorld struct {
	demand  float64 // user requests/s
	hit     float64 // share answered from the handler's own cache (X-Cache: HIT)
	par     float64 // workers / DB connections
	baseMul float64 // DB latency multiplier
	fail    float64 // share of real requests answered 503 at once (a dependency is down)
	scan    float64 // scanner requests/s, all 404
}

var inPhases = []phase{
	{"normal", 0, minutes(30)},
	{"rolling deploy", minutes(30), minutes(36)},
	{"normal", minutes(36), minutes(50)},
	{"cache flush", minutes(50), minutes(60)},
	{"normal", minutes(60), minutes(75)},
	{"slow DB", minutes(75), minutes(85)},
	{"normal", minutes(85), minutes(95)},
	{"scanner flood", minutes(95), minutes(100)},
	{"normal", minutes(100), minutes(110)},
	{"flash crowd x2.5", minutes(110), minutes(120)},
	{"normal", minutes(120), minutes(130)},
	{"auth service down", minutes(130), minutes(133)},
	{"recovery", minutes(133), minutes(150)},
}

func inTimeline() []inWorld {
	rng := rand.New(rand.NewSource(5))
	ws := make([]inWorld, int(rtDur/rtDt))
	for i := range ws {
		m := (time.Duration(i) * rtDt).Minutes()
		w := inWorld{
			demand:  (900 + 200*math.Sin(2*math.Pi*m/60)) * math.Exp(0.15*rng.NormFloat64()),
			hit:     0.4,
			par:     40,
			baseMul: 1,
			scan:    15,
		}
		switch {
		case m >= 30 && m < 36:
			w.par = 30
		case m >= 50 && m < 60:
			w.hit *= (m - 50) / 10
		case m >= 75 && m < 85:
			w.baseMul = 3
		case m >= 95 && m < 100:
			w.scan = 600
		case m >= 110 && m < 120:
			w.demand *= 2.5
		case m >= 130 && m < 133:
			w.fail = 1
		}
		ws[i] = w
	}
	return ws
}

var inEndpoints = [...]endpointSpec{
	{0.60, 20 * time.Millisecond, 0},
	{0.35, 60 * time.Millisecond, 0},
	{0.05, 400 * time.Millisecond, 0},
}

const (
	inAvgBase  = 0.6*0.020 + 0.35*0.060 + 0.05*0.400 // worker-seconds per real request
	inSLO      = time.Second
	inTimeout  = 5 * time.Second // the client gives up and disconnects
	inAuthFail = 0.01
	inGone     = 0.005
	inHealthz  = 20.0
)

func runRealInbound(arm rtArm, world []inWorld) []rtScore {
	var lim *agilelimiter.ConcurrencyLimiter
	if arm.in != nil {
		lim = agilelimiter.NewConcurrency(*arm.in)
	}
	rng := rand.New(rand.NewSource(9))
	scores := make([]rtScore, len(inPhases))
	var inflight, poolFail, retries float64
	carry := [4]float64{}
	take := func(k int, x float64) int {
		x += carry[k]
		n := int(x)
		carry[k] = x - float64(n)
		return n
	}
	for i, w := range world {
		sc := &scores[phaseAt(inPhases, time.Duration(i)*rtDt)]
		sc.secs++
		w.demand += retries
		retries = 0
		var unreachable, reached int
		count := func(o agilelimiter.Outcome) {
			switch o {
			case agilelimiter.Unreachable:
				unreachable++
			case agilelimiter.Reached:
				reached++
			}
		}
		// fail scores one failed user request; the client retries most of them.
		fail1 := func() {
			sc.bad += 1 - rtRetry
			retries += rtRetry
		}
		wpr := inAvgBase * w.baseMul
		capacity := w.par / wpr
		fail := math.Max(w.fail, poolFail)

		// Real work offered this tick: user requests that miss the cache, are
		// not rejected by auth, and are not failed at once.
		realShare := (1 - w.hit) * (1 - inAuthFail - inGone)
		offered := w.demand * realShare * (1 - fail)
		admitted := offered
		if arm.fixed != rtNoLimit && arm.in == nil {
			admitted *= math.Min(1, arm.fixed/(w.demand+w.scan))
		}
		if admitted <= capacity {
			inflight = math.Max(admitted*wpr, inflight-(capacity-admitted)*rtDt.Seconds())
		} else {
			inflight = math.Max(inflight, w.par) + (admitted-capacity)*rtDt.Seconds()
		}
		inflight = math.Min(inflight, admitted*inTimeout.Seconds()+w.par)
		if lim != nil {
			if l := float64(lim.Limit()); inflight > l {
				inflight = l
				admitted = math.Min(admitted, math.Min(l, w.par)/wpr)
			}
		}
		// Too many handlers waiting on too few DB connections: the pool times
		// out and the handler answers 503 at once.
		poolFail = clamp((inflight/w.par-1.5)/1.5, 0, 0.8)
		if inflight > 1.2*w.par {
			sc.overload++
		}
		done := math.Min(inflight, w.par) / wpr
		slow := math.Max(slowdown(math.Min(inflight, w.par)/w.par), inflight/w.par)

		// The share of every kind of arrival that got a slot.
		admitShare := 1.0
		if offered > 0 {
			admitShare = math.Min(1, admitted/offered)
		}
		if arm.fixed != rtNoLimit && arm.in == nil {
			admitShare = math.Min(1, arm.fixed/(w.demand+w.scan))
		}
		// A 429 is retried like any other failure, after Retry-After.
		users := w.demand * (1 - inAuthFail - inGone)
		sc.held += users * (1 - admitShare) * (1 - rtRetry)
		retries += users * (1 - admitShare) * rtRetry

		for j, n := 0, take(0, done); j < n; j++ {
			ep := 0
			for x := rng.Float64(); ep < len(inEndpoints)-1 && x > inEndpoints[ep].share; ep++ {
				x -= inEndpoints[ep].share
			}
			lat := inEndpoints[ep].base.Seconds() * w.baseMul * slow * rtTail(rng)
			c, good := realWork, lat <= inSLO.Seconds()
			switch r := rng.Float64(); {
			case lat > inTimeout.Seconds():
				c, lat, good = clientGone, inTimeout.Seconds(), false
			case r < 0.001:
				c, lat, good = bug500, 0.002*rtTail(rng), false
			}
			if lim != nil {
				count(routeServer(lim, c, dur(lat)))
			}
			switch {
			case good:
				sc.good++
			case c == realWork: // answered, just too slowly
				sc.bad++
			default:
				fail1()
			}
		}
		// Real requests failed at once by a missing dependency or an exhausted pool.
		for j, n := 0, take(1, w.demand*(1-w.hit)*(1-inAuthFail-inGone)*fail*admitShare); j < n; j++ {
			if lim != nil {
				count(routeServer(lim, dep503, dur(0.0005*rtTail(rng))))
			}
			fail1()
		}
		// Cache hits are answered too, and scored: a user was waiting for them.
		for j, n := 0, take(2, w.demand*w.hit*(1-inAuthFail-inGone)*admitShare); j < n; j++ {
			if lim != nil {
				count(routeServer(lim, srvHit, dur(0.0003*rtTail(rng))))
			}
			sc.good++
		}
		// Unscored traffic the limiter still sees: auth failures, disconnects,
		// scanners and health checks.
		if lim != nil {
			other := w.demand*(inAuthFail+inGone)*admitShare + w.scan*admitShare
			for j, n := 0, take(3, other+inHealthz); j < n; j++ {
				switch x := rng.Float64() * (other + inHealthz); {
				case x < w.demand*inAuthFail*admitShare:
					count(routeServer(lim, authFail, dur(0.0003*rtTail(rng))))
				case x < w.demand*(inAuthFail+inGone)*admitShare:
					count(routeServer(lim, clientGone, dur(0.01*rtTail(rng))))
				case x < other:
					count(routeServer(lim, scanner404, dur(0.0001*rtTail(rng))))
				default:
					count(routeServer(lim, healthz, dur(0.00005*rtTail(rng))))
				}
			}
			before := lim.Stats().Rate
			agilelimiter.StepConcurrency(lim)
			sc.end = lim.Stats()
			if sc.end.Rate < 0.99*before {
				backoffCause(sc, unreachable, reached, sc.end, arm.in.TailTolerance, inSLO)
			}
		}
	}
	return scores
}

func TestRealisticInbound(t *testing.T) {
	if testing.Short() {
		t.Skip("simulates 150 minutes of traffic; skipped in -short")
	}
	cfg := func(p agilelimiter.Preset, tol float64) *agilelimiter.ConcurrencyConfig {
		return &agilelimiter.ConcurrencyConfig{
			MaxLimit: 400, FloorLimit: 20, ProbeLimit: 4, InitialLimit: 40,
			SLOTarget: inSLO, Preset: p, TailTolerance: tol,
		}
	}
	arms := []rtArm{
		{name: "no limiter", fixed: rtNoLimit},
		// The normal peak is 1100 users/s plus scanners; 1200 admits all of it.
		{name: "uber(1200)", fixed: 1200},
		{name: "agile default", in: cfg(agilelimiter.PresetNone, 0)},
		{name: "agile latency-first", in: cfg(agilelimiter.PresetLatencyFirst, 0)},
		{name: "agile TailTolerance 5%", in: cfg(agilelimiter.PresetNone, 0.05)},
		{name: "agile latency-first+Tol 5%", in: cfg(agilelimiter.PresetLatencyFirst, 0.05)},
	}
	world := inTimeline()
	scores := make([][]rtScore, len(arms))
	for i, a := range arms {
		scores[i] = runRealInbound(a, world)
	}
	total := rtReport(t, "inbound: 700-1100 users/s with bursts, 40% cache hits (X-Cache), 40 DB connections, SLO 1s, client timeout 5s;\n"+
		"endpoints 20/60/400ms; 401 1%, disconnects 0.5%, 500 0.1%, scanners 15/s, /healthz 20/s; pool exhaustion answers 503;\n"+
		"two in three failed requests and 429s are retried",
		inPhases, arms, scores)
	rtCheck(t, arms, total, 1, len(arms)-1)
}
