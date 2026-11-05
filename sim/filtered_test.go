package sim

// Robustness with the traffic filtered the way the README says to: the HTTP
// adapters' DefaultVerdict drops tagged cache hits, refused connections and
// 429/5xx become outage samples, and the inbound verdict excludes health checks
// and metrics by path. The question is what still gets through, and whether it
// is enough to knock the limiter down or push it through.
//
// Every simulated call is a real *http.Request / *http.Response with real status
// codes and headers, classified by the real DefaultVerdict and routed exactly as
// httpclient.Wrap and httpserver.Wrap route it: a canceled call is dropped first,
// then Reached -> Observe, Unreachable -> Unavailable, Aborted -> nothing. Only
// time is simulated — the adapters time a call with time.Now, so the duration a
// call would have measured is generated instead of slept.
//
//	go test -run '^TestFiltered' -v ./sim

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"agilelimiter"
	"agilelimiter/httpclient"
	"agilelimiter/httpserver"
	"agilelimiter/internal/simhook"
)

const (
	ftDt         = time.Second // the default Tick
	ftWarm       = 60 * time.Second
	ftSigma      = 0.2
	ftMaxPerKind = 2000
	ftShort      = ftWarm + 10*time.Minute // steady shares
	ftLong       = ftWarm + 2*time.Hour    // two periods of an hourly swing
)

func ftGoodFraction(lat float64, slo time.Duration) float64 {
	return 0.5 * math.Erfc(-math.Log(slo.Seconds()/lat)/(ftSigma*math.Sqrt2))
}

func ftJitter(rng *rand.Rand, lat float64) time.Duration {
	return time.Duration(lat * math.Exp(ftSigma*rng.NormFloat64()) * 1e9)
}

// share is the fraction of calls of one kind at time t.
type share func(t time.Duration) float64

func steady(f float64) share { return func(time.Duration) float64 { return f } }

// hourly swings between lo and hi with a one-hour period, the way a cache hit
// ratio or a scanner's traffic does over a day, compressed.
func hourly(lo, hi float64) share {
	return func(t time.Duration) float64 {
		return lo + (hi-lo)*(0.5-0.5*math.Cos(2*math.Pi*t.Seconds()/3600))
	}
}

// swings marks the scenarios that need two whole periods of an hourly share.
type swings bool

func (s swings) dur() time.Duration {
	if s {
		return ftLong
	}
	return ftShort
}

// ---------------------------------------------------------------------------
// outbound: httpclient.Wrap in front of an origin
// ---------------------------------------------------------------------------

type clientCall struct {
	resp    *http.Response
	err     error
	latency time.Duration // what the call takes; it does not touch the origin
	good    bool          // an answer the caller can use
}

func response(status int, kv ...string) *http.Response {
	h := http.Header{}
	for i := 0; i+1 < len(kv); i += 2 {
		h.Set(kv[i], kv[i+1])
	}
	return &http.Response{StatusCode: status, Header: h}
}

var (
	originMiss  = &clientCall{resp: response(200), good: true}
	hitXCache   = &clientCall{resp: response(200, "X-Cache", "HIT"), latency: time.Millisecond, good: true}
	hitAge      = &clientCall{resp: response(200, "Age", "12"), latency: time.Millisecond, good: true}
	hitUntagged = &clientCall{resp: response(200), latency: time.Millisecond, good: true}
	// nginx proxy_cache: $upstream_cache_status is STALE / UPDATING when it serves
	// an expired entry, and nginx sends no Age header.
	hitStale  = &clientCall{resp: response(200, "X-Cache-Status", "STALE"), latency: time.Millisecond, good: true}
	notFound  = &clientCall{resp: response(404), latency: 2 * time.Millisecond, good: true}
	refused   = &clientCall{err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, latency: 50 * time.Microsecond}
	upstream  = &clientCall{resp: response(503), latency: 2 * time.Millisecond}
	canceledC = &clientCall{err: context.Canceled, latency: 5 * time.Millisecond}
)

// routeClient is httpclient.Wrap's routing, after the round trip returned. It
// reports what the limiter was told; a canceled call reads as Aborted, whether
// or not ObserveTruncated kept it.
func routeClient(lim *agilelimiter.Limiter, c *clientCall, d time.Duration) agilelimiter.Outcome {
	if errors.Is(c.err, context.Canceled) {
		lim.ObserveTruncated(d)
		return agilelimiter.Aborted
	}
	out := httpclient.DefaultVerdict(c.resp, c.err, d)
	switch out {
	case agilelimiter.Unreachable:
		lim.Unavailable()
	case agilelimiter.Aborted:
	default:
		lim.Observe(d)
	}
	return out
}

type clientMix struct {
	call  *clientCall
	share share
}

type ftResult struct {
	good, sent float64 // origin calls inside the SLO, origin calls
	load       float64 // sum of origin load / capacity over scored ticks
	ticks      int
}

func (r ftResult) String() string {
	ok := 0.0
	if r.sent > 0 {
		ok = 100 * r.good / r.sent
	}
	secs := float64(r.ticks) * ftDt.Seconds()
	return fmt.Sprintf("%.0f (%.0f%%) load=%.2f", r.good/secs, ok, r.load/float64(r.ticks))
}

func (r ftResult) goodput() float64 { return r.good / (float64(r.ticks) * ftDt.Seconds()) }

type ftEvent struct {
	client *clientCall
	server *serverCall
	d      time.Duration
}

// runClient drives one Limiter whose every call goes through httpclient's
// verdict. Every call takes a slot (a hit cannot be known in advance), and
// only the misses load the origin: base 20ms, latency base/(1-util) plus the
// backlog of any overload, capacity 150/s, SLO 100ms.
func runClient(cfg agilelimiter.Config, mixes []clientMix, dur time.Duration) ftResult {
	const base, capacity = 0.020, 150.0
	slo := cfg.SLOTarget
	lim := agilelimiter.New(cfg)
	rng := rand.New(rand.NewSource(1))
	carry := make([]float64, len(mixes)+1)
	var backlog float64
	var res ftResult
	var ev []ftEvent
	for t := time.Duration(0); t < dur; t += ftDt {
		calls := lim.Rate() * ftDt.Seconds()
		originCalls := calls
		ev = ev[:0]
		for i, m := range mixes {
			x := calls*m.share(t) + carry[i]
			k := int(x)
			carry[i] = x - float64(k)
			originCalls -= calls * m.share(t)
			for j := 0; j < k && j < ftMaxPerKind; j++ {
				ev = append(ev, ftEvent{client: m.call, d: ftJitter(rng, m.call.latency.Seconds())})
			}
		}
		load := originCalls / ftDt.Seconds()
		backlog = math.Max(0, backlog+(load-capacity)*ftDt.Seconds())
		lat := base/(1-math.Min(load/capacity, 0.95)) + backlog/capacity
		x := originCalls + carry[len(mixes)]
		n := int(x)
		carry[len(mixes)] = x - float64(n)
		for j := 0; j < n && j < ftMaxPerKind; j++ {
			ev = append(ev, ftEvent{client: originMiss, d: ftJitter(rng, lat)})
		}
		rng.Shuffle(len(ev), func(i, j int) { ev[i], ev[j] = ev[j], ev[i] })
		for _, e := range ev {
			routeClient(lim, e.client, e.d)
		}
		simhook.Use(lim, calls)
		simhook.Step(lim)

		if t >= ftWarm {
			res.good += originCalls * ftGoodFraction(lat, slo)
			res.sent += originCalls
			res.load += load / capacity
			res.ticks++
		}
	}
	return res
}

func clientCfg(p agilelimiter.Preset) agilelimiter.Config {
	return agilelimiter.Config{
		MaxRate:     1000,
		FloorRate:   5,
		InitialRate: 50,
		SLOTarget:   100 * time.Millisecond,
		Preset:      p,
	}
}

type ftTable struct{ rows [][]string }

func (t *ftTable) row(c ...string) { t.rows = append(t.rows, c) }

func (t *ftTable) String() string {
	w := map[int]int{}
	for _, r := range t.rows {
		for i, c := range r {
			if n := len([]rune(c)); n > w[i] {
				w[i] = n
			}
		}
	}
	var b strings.Builder
	for _, r := range t.rows {
		b.WriteString("\n")
		for i, c := range r {
			pad := strings.Repeat(" ", w[i]-len([]rune(c)))
			if i == 0 {
				b.WriteString(c + pad)
			} else {
				b.WriteString("  " + pad + c)
			}
		}
	}
	return b.String()
}

func TestFilteredOutbound(t *testing.T) {
	if testing.Short() {
		t.Skip("simulates hours of traffic; skipped in -short")
	}
	presets := []agilelimiter.Preset{agilelimiter.PresetNone, agilelimiter.PresetLatencyFirst}
	ref := map[swings][]ftResult{}
	for _, long := range []swings{false, true} {
		for _, p := range presets {
			ref[long] = append(ref[long], runClient(clientCfg(p), nil, long.dur()))
		}
	}

	tb := &ftTable{}
	tb.row("traffic besides origin misses", "verdict", "default", "latency-first")
	tb.row("(none)", "-", ref[false][0].String(), ref[false][1].String())
	for _, sc := range []struct {
		name, verdict string
		long          swings
		filtered      bool // classified so that it must not move the limiter
		mix           []clientMix
	}{
		{"hits 30%->70%, X-Cache: HIT", "Aborted", true, true, []clientMix{{hitXCache, hourly(0.3, 0.7)}}},
		{"hits 30%->70%, Age: 12", "Aborted", true, true, []clientMix{{hitAge, hourly(0.3, 0.7)}}},
		{"5% canceled by the caller", "dropped", false, true, []clientMix{{canceledC, steady(0.05)}}},
		{"0.5% refused", "Unreachable", false, true, []clientMix{{refused, steady(0.005)}}},
		{"1% refused", "Unreachable", false, true, []clientMix{{refused, steady(0.01)}}},
		{"1% 503", "Unreachable", false, true, []clientMix{{upstream, steady(0.01)}}},
		{"5% 503", "Unreachable", false, true, []clientMix{{upstream, steady(0.05)}}},
		{"hits 30%->70%, untagged", "Reached", true, false, []clientMix{{hitUntagged, hourly(0.3, 0.7)}}},
		{"20% nginx STALE (no Age)", "Reached", false, false, []clientMix{{hitStale, steady(0.2)}}},
		{"10% 404", "Reached", false, false, []clientMix{{notFound, steady(0.1)}}},
		{"30% 404", "Reached", false, false, []clientMix{{notFound, steady(0.3)}}},
		{"404 0%->60%", "Reached", true, false, []clientMix{{notFound, hourly(0, 0.6)}}},
		{"tagged hits 30->70% + 10% 404", "mixed", true, false, []clientMix{{hitXCache, hourly(0.3, 0.7)}, {notFound, steady(0.1)}}},
	} {
		cells := []string{sc.name, sc.verdict}
		for i, p := range presets {
			got := runClient(clientCfg(p), sc.mix, sc.long.dur())
			cells = append(cells, got.String())
			if want := ref[sc.long][i]; sc.filtered && got.goodput() < 0.95*want.goodput() {
				t.Errorf("preset %v, %s: %.0f origin good/s, %.0f without it; the verdict should make it invisible",
					p, sc.name, got.goodput(), want.goodput())
			}
		}
		tb.row(cells...)
	}
	t.Logf("outbound through httpclient.DefaultVerdict: origin capacity 150/s, base 20ms, SLO 100ms;\n"+
		"steady rows score 10min, hourly swings 2h; cells: origin good/s (origin calls inside the SLO)\n"+
		"load=mean origin load/capacity%s", tb)
}

// ---------------------------------------------------------------------------
// inbound: httpserver.Wrap in front of a service
// ---------------------------------------------------------------------------

type serverCall struct {
	req     *http.Request
	status  int
	header  http.Header
	latency time.Duration
}

func request(path string) *http.Request { return httptest.NewRequest(http.MethodGet, path, nil) }

func header(kv ...string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(kv); i += 2 {
		h.Set(kv[i], kv[i+1])
	}
	return h
}

var canceledCtx = func() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}()

var (
	realWork    = &serverCall{req: request("/api/items"), status: 200, header: header()}
	srvHit      = &serverCall{req: request("/api/items"), status: 200, header: header("X-Cache", "HIT"), latency: 200 * time.Microsecond}
	srvHitPlain = &serverCall{req: request("/api/items"), status: 200, header: header(), latency: 200 * time.Microsecond}
	healthz     = &serverCall{req: request("/healthz"), status: 200, header: header(), latency: 50 * time.Microsecond}
	scanner404  = &serverCall{req: request("/wp-login.php"), status: 404, header: header(), latency: 100 * time.Microsecond}
	authFail    = &serverCall{req: request("/api/items"), status: 401, header: header(), latency: 300 * time.Microsecond}
	bug500      = &serverCall{req: request("/api/items"), status: 500, header: header(), latency: 500 * time.Microsecond}
	dep503      = &serverCall{req: request("/api/items"), status: 503, header: header(), latency: 500 * time.Microsecond}
	clientGone  = &serverCall{req: request("/api/items").WithContext(canceledCtx), status: 200, header: header(), latency: 10 * time.Millisecond}
)

// serverVerdict is the README's verdict: health checks and metrics by path,
// everything else DefaultVerdict.
func serverVerdict(r *http.Request, status int, h http.Header, d time.Duration) agilelimiter.Outcome {
	if strings.HasPrefix(r.URL.Path, "/health") || r.URL.Path == "/metrics" {
		return agilelimiter.Aborted
	}
	return httpserver.DefaultVerdict(r, status, h, d)
}

// routeServer is httpserver.Wrap's routing, after the handler returned. It
// reports what the limiter was told; a canceled request reads as Aborted,
// whether or not ObserveTruncated kept it.
func routeServer(lim *agilelimiter.ConcurrencyLimiter, c *serverCall, d time.Duration) agilelimiter.Outcome {
	if c.req.Context().Err() != nil {
		lim.ObserveTruncated(d)
		return agilelimiter.Aborted
	}
	out := serverVerdict(c.req, c.status, c.header, d)
	switch out {
	case agilelimiter.Unreachable:
		lim.Unavailable()
	case agilelimiter.Aborted:
	default:
		lim.Observe(d)
	}
	return out
}

type serverMix struct {
	call  *serverCall
	share share   // fraction of admitted requests
	rate  float64 // or a fixed rate per second (health checks)
}

// runServer drives one ConcurrencyLimiter whose every admitted request goes
// through serverVerdict. The service: base 150ms, 150 workers (capacity
// 1000/s), latency base*max(1, inflight/150), SLO 300ms, 1200 closed-loop
// clients. Fast requests hold a slot for microseconds, so the slots are held by
// real work, and they are admitted in proportion to their share of arrivals.
func runServer(cfg agilelimiter.ConcurrencyConfig, mixes []serverMix, dur time.Duration) ftResult {
	const base, par, clients = 0.150, 150.0, 1200.0
	lim := agilelimiter.NewConcurrency(cfg)
	rng := rand.New(rand.NewSource(1))
	carry := make([]float64, len(mixes)+1)
	var res ftResult
	var ev []ftEvent
	for t := time.Duration(0); t < dur; t += ftDt {
		inflight := math.Min(float64(lim.Limit()), clients)
		lat := base * math.Max(1, inflight/par)
		work := inflight / lat * ftDt.Seconds()

		fast := 0.0
		for _, m := range mixes {
			if m.share != nil {
				fast += m.share(t)
			}
		}
		admitted := work / (1 - fast)
		ev = ev[:0]
		for i, m := range mixes {
			x := carry[i]
			if m.share != nil {
				x += admitted * m.share(t)
			} else {
				x += m.rate * ftDt.Seconds()
			}
			k := int(x)
			carry[i] = x - float64(k)
			for j := 0; j < k && j < ftMaxPerKind; j++ {
				ev = append(ev, ftEvent{server: m.call, d: ftJitter(rng, m.call.latency.Seconds())})
			}
		}
		x := work + carry[len(mixes)]
		n := int(x)
		carry[len(mixes)] = x - float64(n)
		for j := 0; j < n && j < ftMaxPerKind; j++ {
			ev = append(ev, ftEvent{server: realWork, d: ftJitter(rng, lat)})
		}
		rng.Shuffle(len(ev), func(i, j int) { ev[i], ev[j] = ev[j], ev[i] })
		for _, e := range ev {
			routeServer(lim, e.server, e.d)
		}
		simhook.Use(lim, inflight)
		simhook.Step(lim)

		if t >= ftWarm {
			res.good += work * ftGoodFraction(lat, cfg.SLOTarget)
			res.sent += work
			res.load += math.Min(inflight, par) / par
			res.ticks++
		}
	}
	return res
}

func serverCfg(p agilelimiter.Preset) agilelimiter.ConcurrencyConfig {
	return agilelimiter.ConcurrencyConfig{
		MinLimit:     2,
		MaxLimit:     500,
		InitialLimit: 100,
		SLOTarget:    300 * time.Millisecond,
		Preset:       p,
	}
}

func TestFilteredInbound(t *testing.T) {
	if testing.Short() {
		t.Skip("simulates hours of traffic; skipped in -short")
	}
	presets := []agilelimiter.Preset{agilelimiter.PresetNone, agilelimiter.PresetLatencyFirst}
	ref := map[swings][]ftResult{}
	for _, long := range []swings{false, true} {
		for _, p := range presets {
			ref[long] = append(ref[long], runServer(serverCfg(p), nil, long.dur()))
		}
	}

	tb := &ftTable{}
	tb.row("traffic besides real work", "verdict", "default", "latency-first")
	tb.row("(none)", "-", ref[false][0].String(), ref[false][1].String())
	for _, sc := range []struct {
		name, verdict string
		long          swings
		filtered      bool
		mix           []serverMix
	}{
		{"hits 30%->70%, X-Cache: HIT", "Aborted", true, true, []serverMix{{call: srvHit, share: hourly(0.3, 0.7)}}},
		{"/healthz 50/s", "Aborted (path)", false, true, []serverMix{{call: healthz, rate: 50}}},
		{"5% client went away", "dropped", false, true, []serverMix{{call: clientGone, share: steady(0.05)}}},
		{"1% 503", "Unreachable", false, true, []serverMix{{call: dep503, share: steady(0.01)}}},
		{"hits 30%->70%, untagged", "Reached", true, false, []serverMix{{call: srvHitPlain, share: hourly(0.3, 0.7)}}},
		{"5% fast 500", "Reached", false, false, []serverMix{{call: bug500, share: steady(0.05)}}},
		{"20% 401", "Aborted", false, true, []serverMix{{call: authFail, share: steady(0.2)}}},
		{"10% scanner 404", "Aborted", false, true, []serverMix{{call: scanner404, share: steady(0.1)}}},
		{"30% scanner 404", "Aborted", false, true, []serverMix{{call: scanner404, share: steady(0.3)}}},
		{"scanner 404 0%->60%", "Aborted", true, true, []serverMix{{call: scanner404, share: hourly(0, 0.6)}}},
		{"README mix: tagged hits, /healthz, 5% gone", "filtered", true, true, []serverMix{
			{call: srvHit, share: hourly(0.3, 0.7)}, {call: healthz, rate: 50}, {call: clientGone, share: steady(0.05)}}},
	} {
		cells := []string{sc.name, sc.verdict}
		for i, p := range presets {
			got := runServer(serverCfg(p), sc.mix, sc.long.dur())
			cells = append(cells, got.String())
			if want := ref[sc.long][i]; sc.filtered && got.goodput() < 0.95*want.goodput() {
				t.Errorf("preset %v, %s: %.0f good/s, %.0f without it; the verdict should make it invisible",
					p, sc.name, got.goodput(), want.goodput())
			}
		}
		tb.row(cells...)
	}
	t.Logf("inbound through httpserver.DefaultVerdict + path exclusion: capacity 1000/s, base 150ms, SLO 300ms;\n"+
		"steady rows score 10min, hourly swings 2h; cells: real-work good/s (share inside the SLO)\n"+
		"load=mean worker utilization%s", tb)
}
