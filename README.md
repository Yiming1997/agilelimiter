# agileLimiter

An adaptive, latency-driven rate limiter that protects a downstream dependency.

Standalone Go module (`module agilelimiter`), zero external dependencies. It is
deliberately small: a **simple leaky bucket** (one mutex) is the *actuator*, and
a **feedback controller** decides the rate. A static ceiling is enforced in two
independent places, so the controller can never exceed a number you are willing
to promise.

```
downstream RTT ──> histogram (p50/p99) ──> fast/slow EWMA ──> rate factor ──> clamp[min,max] ──> bucket.SetRate
   (Observe)                                     (baseline)      gradient/AIMD
```

## Files

| File | Role |
|---|---|
| `bucket.go` | Simplest leaky bucket; `setRate` hot-updates the rate. |
| `histogram.go` | Lock-free log-bucketed histogram (p50/p99), reset each tick. |
| `controller.go` | Fast/slow EWMA baseline + gradient decrease / additive increase + p99 guard. |
| `limiter.go` | `Config`, `New`, `Do` (and the generic `Do[T]`), the lower-level `Take`/`Observe`, `Rate`, `Stats`, `Start`, `Stop`. |
| `semaphore.go` | Concurrency actuator for inbound limiting; non-blocking `tryAcquire`. |
| `concurrency.go` | `ConcurrencyConfig`, `NewConcurrency`, `TryTake`, `Release`, `Do`, `Limit`. |
| `httpserver/httpserver.go` | `net/http` middleware: shed with 429, feed handler latency back. |
| `httpclient/httpclient.go` | `http.RoundTripper`: pace outbound HTTP, keep cache hits out of the signal. |
| `limiter_test.go` | Deterministic tests (virtual clock). |
| `concurrency_test.go` | Deterministic tests for the concurrency limiter. |
| `httpserver/httpserver_test.go` | `httptest`-based tests for the middleware. |
| `example/main.go` | Runnable demo: a downstream whose capacity changes over time. |
| `example/concurrency/main.go` | Runnable demo: inbound concurrency limiting. |
| `example/httpserver/main.go` | Runnable demo: an HTTP service shedding load under a swarm. |
| `docs/design.md` | Full design notes. |

## Usage

Two things are yours to decide: how much you are willing to promise, and how slow
is still acceptable. Everything else is policy, and policy comes from a preset.

```go
cfg := agilelimiter.Config{
	MaxRate:  500,                     // the ceiling you promise the downstream
	SLOTarget: 200 * time.Millisecond, // the red line
	Preset:   agilelimiter.PresetLatencyFirst,
}
if err := cfg.Validate(); err != nil { // optional; do it in tests
	log.Fatal(err)
}

lim := agilelimiter.New(cfg)
lim.Start()
defer lim.Stop()

log.Printf("limiter: %+v", lim.Config()) // what it actually resolved to

// Hot path: wrap the downstream call in Do. It takes a slot, times the call, and
// feeds the measured round-trip time back to the controller — you never touch
// Take/Observe, so you cannot feed it the wrong latency.
err := lim.Do(func() error { return doRequest(ctx) })
```

| Preset | Threshold | A step needs | Use it when |
|---|---|---|---|
| `PresetNone` (zero) / `PresetBalanced` | 2x baseline | 1 tick | default; protects the knee |
| `PresetLatencyFirst` | `0.8 x SLOTarget` | 2 ticks | the SLO is a contract |
| `PresetThroughputFirst` | 3.5x baseline | 2 ticks | downstream degrades gracefully; set `MaxRate` to the knee |

A preset only fills the fields you left at zero, so an explicit setting always
wins. No preset touches `Tick` or the half-lives: those are the *scale* of the
control law, and moving them silently re-tunes every gain — they stay available
for anyone who has read `docs/design.md`.

`Config()` returns a `Resolved` snapshot (defaults applied, presets expanded, an
SLO anchor that could not be honoured reported rather than hidden). `Validate()`
reports the configs `New` would otherwise repair in silence — `MinRate` above
`MaxRate`, a `Deadband` of 1 that makes every latency "healthy", `AnchorSLO`
without a `SLOTarget`.

When the call returns a value, use the generic form:

```go
resp, err := agilelimiter.Do(lim, func() (*http.Response, error) {
	return client.Do(req)
})
```

`Do` observes the round trip even when `fn` returns an error: a timeout is a real
signal that the downstream is struggling, so it still moves the controller.

## The one rule that matters: measure the right latency

The signal the controller wants is the *downstream* round-trip time — measured
from the moment the request was **actually sent**, not from the moment it was
planned. `Do` gets this right for you: it starts its clock the instant the slot
is granted, so what it feeds the controller is exactly that round trip.

If you instead feed end-to-end latency that includes this limiter's own throttle
wait, you create a self-reinforcing loop: backing off raises the measured
latency, which triggers more backing off, which collapses the rate to `MinRate`.
`Do` is the default precisely because it removes that footgun. Reach for the
manual `Take`/`Observe` pair only when `Do` cannot measure the round trip — for
an async send, or a call that retries internally, where the value must be one
attempt's real round trip rather than the wrapper's. When you do, feed `Observe`
the downstream RTT and keep `Take`'s planned-send timestamp for a
coordinated-omission-safe *reporting* metric only:

```go
t0 := lim.Take()               // planned send time (blocks until granted)
start := time.Now()            // actually sent
err := doRequest(ctx)
lim.Observe(time.Since(start)) // downstream RTT -> drives the controller
report(time.Since(t0))         // CO-safe end-to-end metric -> reporting only
```

## When the downstream is down: classify failures by "did it reach?"

A failure is **not** automatically a congestion signal, and getting this wrong
makes the limiter actively harmful. A refused connection returns in microseconds;
the controller reads that as "the downstream is fast and healthy" and raises the
rate into a dependency that is already down. Measured: starting at 50 req/s
against a port that refuses every call, the rate climbed to **335 req/s in ten
seconds** — and kept going.

The question that matters is not "did it fail?" but "did it reach the
downstream?". Say so once at the boundary — `Do`/`Do[T]` take `Config.Classify`;
the HTTP adapters take a `Verdict` (`httpclient`/`httpserver` below), and each
call path has exactly one of the two:

```go
lim := agilelimiter.New(agilelimiter.Config{
	MinRate:   20,
	MaxRate:   500,
	SLOTarget: 200 * time.Millisecond,
	Classify: func(err error) agilelimiter.Outcome {
		switch {
		case errors.Is(err, syscall.ECONNREFUSED), errors.Is(err, errBreakerOpen):
			return agilelimiter.Unreachable // never arrived
		case errors.Is(err, context.Canceled):
			return agilelimiter.Aborted // not the downstream's opinion
		default:
			return agilelimiter.Reached // timeout, 5xx: it answered
		}
	},
})
```

| `Outcome` | When | What the controller is told |
|---|---|---|
| `Reached` | it reached the downstream and came back — 2xx, 5xx, timeout | the measured round trip, as usual |
| `Unreachable` | it never arrived — refused, DNS, TLS, open breaker | a synthetic "congested" sample, so it backs off toward `MinRate` |
| `Aborted` | the caller gave up, or the call never started | nothing |

Details that matter:

- **The default is `Reached` for every error** (i.e. today's behaviour), so this
  is opt-in. It is right for anything the downstream answered; it is wrong only
  for calls that never arrived, and only you can tell the difference.
- The synthetic sample is placed above the tolerated window and above
  `SLOTarget`, so the proportional backoff *and* the p99 tail guard both fire.
  It is a no-op before there is anything to anchor against (no `SLOTarget`, no
  sample yet) — inventing a baseline value would be worse than staying silent.
- **`MinRate` is the outage probe rate.** Backing off to 20 req/s is how the
  limiter keeps testing whether the downstream came back. Make sure it is a rate
  the downstream can absorb (× your instance count).
- On the manual `Take`/`Observe` path, call `lim.Unavailable()` instead of
  `Observe` for that attempt. Never both — that records two samples for one call.
- `ConcurrencyLimiter` has the same options, for handlers that answer 503 without
  doing any work: `Config.Classify` and `lim.Unavailable()`.

Pair this with a circuit breaker. The breaker decides "stop calling"; the limiter
decides "how much of the remaining capacity to use". Without a breaker you still
pay a full network timeout on every call while the limiter sits at `MinRate`.

## Downstream caches: don't let hits drive the controller

A cache in front of a downstream splits its traffic into two populations sharing
one pipe: a hit answers in ~1ms and consumes none of the origin's capacity, a
miss costs whatever the origin costs. Feed both into one p50 and the controller
tracks the cache's hit ratio instead of the origin's load. Measured
(`compare/cache_test.go`, 95% hits, origin saturated):

| | overall SLO | requests that reached the origin | origin p99 |
|---|---:|---:|---:|
| observe everything | **98.9%** | **79%** | **369ms** |
| observe misses only | 100% | 100% | 278ms |

The aggregate was green while the origin was timing out. So route hits past the
controller:

```go
client := &http.Client{
	Transport: httpclient.Wrap(http.DefaultTransport, lim),
}
```

`Wrap` takes a slot, measures the round trip, and hands the result to a
`Verdict`: one function, called once per call, returning one of three answers.

```go
type Verdict func(resp *http.Response, err error, d time.Duration) agilelimiter.Outcome

client := &http.Client{
	Transport: httpclient.Wrap(http.DefaultTransport, lim,
		httpclient.WithVerdict(myVerdict)),
}
```

That is the whole policy surface. There is no option per case, because the cases
are not enumerable: a new proxy header, a new async endpoint, a circuit-breaker
fallback — each is a line in your function, not a release of this library.

`DefaultVerdict` is what you get without one. It is small enough to read:

| result | answer | why |
|---|---|---|
| `context.Canceled` | `Aborted` | the caller gave up; the duration is truncated, not fast |
| a `*net.OpError` with `Op == "dial"` | `Unreachable` | refused, DNS, no route, connect timeout: nothing was sent |
| any other error | `Reached` | a timeout is a real signal |
| 429, any 5xx | `Unreachable` | "do not send more" — from outside you cannot tell a bug from overload, and sending less is kind either way |
| cache hit | `Aborted` | `Age > 0`, or a `HIT` in `X-Cache` / `X-Proxy-Cache` / `CF-Cache-Status` |
| anything else | `Reached` | |

Copy it and add the cases you know about. Two it deliberately leaves to you:

```go
case errors.Is(err, ErrBreakerOpen):
	return agilelimiter.Unreachable   // your own error, not a *net.OpError
case resp.StatusCode == http.StatusAccepted:
	return agilelimiter.Aborted       // the work has not happened yet
```

A custom verdict replaces `DefaultVerdict` entirely — `Wrap` does not consult
`Config.Classify` (that one is for `Do`/`Do[T]`, so each call path has exactly one
policy entry point).

One thing a verdict **cannot** change: a canceled call is dropped by `Wrap` before
the verdict runs. It is a truncated measurement, not a fast response, so a verdict
that returns `Reached` for everything still cannot turn it into evidence.

Two things this does **not** do:

- **It does not raise the ceiling for hits.** A hit still takes a slot, because
  the caller cannot know a request will hit before sending it. To give hits a
  higher ceiling, split them onto a second limiter where a local cache already
  tells you the answer.
- **It does not stop a cold cache from stampeding the origin.** That is a step
  change, and an adaptive limiter is a lagging controller. Coalesce misses
  (single-flight so one fill serves many), size `MaxRate` for a **cold** origin,
  and keep a small cache-bypassing probe feeding the controller so the origin's
  real latency stays observable.

If the cache is yours (in-process, or Redis you query yourself), do not classify
anything: put the limiter around the call that actually costs capacity — the
cache hit never reaches it, so it never produces a sample.

## How the controller decides

Each tick:

1. Take p50/p99 from the per-tick histogram.
2. Update `fast` (short half-life) and `slow` (long half-life) EWMAs of p50.
3. Compare `fast` against a window around the tolerated latency
   `high = slow * (1 + TargetInflation)`, `low = high * (1 - Deadband)` — or
   against `0.8 * SLOTarget` outright under `AnchorSLO` (see below):
   - `fast <= low` → healthy, probe up: `+UpStep`, or `×SlowStartGain` while the
     limiter has never seen congestion (cold start — see below).
   - `fast >= high` → congested, proportional decrease `1 - Gain*(excess/high)`,
     capped at `-DownStep`.
   - in between → hold (deadband).
4. If p99 exceeds `SLOTarget x TailFactor`, force a full `-DownStep`.
5. Clamp to `[MinRate, MaxRate]` and push to the bucket.

**Asymmetry is intentional**: back off fast (30%/tick), ramp up slowly (5%/tick).
It is friendlier to the downstream and resists oscillation around the knee. The
baseline EWMA itself is also asymmetric — it falls quickly and rises slowly (see
Pitfalls).

**Multiplicative probing happens in two cases.** Before the first backoff the
controller does not know where the knee is, so `+5%/tick` is guessing very slowly:
10→500 costs 80 ticks. It doubles instead (`SlowStartGain`, default 2.0), which
costs 6. The asymmetry above is exactly what makes this safe — overshooting costs
two ticks to correct, undershooting costs eighty.

The cold-start flag clears on the first backoff and never re-arms, which would
leave a limiter at `MinRate` climbing at `UpStep`: a dip that took 8 ticks to
fall into would take 80 to climb out of. `Config.RecoverRamp` is the opt-in
answer — set it to 0.5 and the multiplicative probe returns whenever the measured
latency sits at or below *half* the tolerated window. It is **off by default**,
and the matrix below shows why: it turns 25 req/s into 52 when capacity comes
back, and 59 into 46 when capacity drops, because doubling is also how you
overshoot a cliff.

## Choosing what "congested" is measured against

`Config.Anchor` decides the threshold, and it changes what the controller
optimizes:

| | `AnchorBaseline` (default) | `AnchorSLO` |
|---|---|---|
| Threshold | `(1+TargetInflation) x baseline`, capped at `0.8 x SLOTarget` | `0.8 x SLOTarget`, flat |
| Optimizing for | the latency knee | goodput subject to the SLO |
| A benign rise | backs off: 20ms -> 50ms is 2.5x the baseline | nothing, while it stays under the threshold |
| `SLOTarget` | optional | required — falls back to `AnchorBaseline` without one |

The default protects the knee, and that is the right default for a downstream
that has one. But the SLO cap is one-way: `min()` can only *lower* the window,
never raise it, so with a small baseline the SLO contributes nothing and the
effective target is "2x the fastest response ever observed", whatever the SLO
allows:

```
20ms service, 200ms SLO  ->  window = min(2 x 20ms, 160ms) = 40ms
```

A healthy-but-slower 50ms period is 2.5x that baseline, so the rate walks down
to `MinRate` and stays: the baseline may only climb `MaxBaselineRise` per tick
(~12%/min at the defaults) and the ramp back is `UpStep` per tick. On a virtual
clock that is 500 -> 10 req/s in 26 ticks, and 88 ticks to return
(`TestBaselineAnchorCollapsesOnBenignRise`). `AnchorSLO` reads 50ms against a
160ms threshold and does not move (`TestSLOAnchorIgnoresBenignRise`).

What it costs: the absolute controller keeps probing while the SLO has headroom,
so it will not find a knee that sits below the SLO. If your downstream degrades
sharply before its own latency target, keep the relative anchor — or keep it and
set `MaxRate` to the knee instead.

Head-to-head against `go.uber.org/ratelimit` (`cd compare && go test -run
TestSLOAnchorVsUber -v`), 20s runs, a downstream that answers 20ms and then
20ms -> 70ms at t=5s with capacity held at 150 req/s and a 100ms SLO:

| | achieved/s | goodput/s | %<=SLO | final rate |
|---|---:|---:|---:|---:|
| `uber(100)` fixed rate | 102 | 102 | 100% | 100 |
| `AnchorBaseline`, 2x (default) | 48 | 48 | 100% | **5** |
| `AnchorBaseline`, 3.5x | 99 | 99 | 100% | 112 |
| `AnchorSLO` | 99 | 99 | 100% | 109 |

The default law spends the rest of the run at `MinRate` on a latency that never
breaks the SLO. When capacity really drops (150 -> 40 req/s) the picture
reverses and the SLO anchor is the one that behaves: it settles at ~39 req/s
with 83% of requests inside the SLO, while the fixed rate keeps sending 100/s
into 40/s (619ms mean, 40% inside).

`TestSLOAnchorMatrix` runs the same comparison over seven scenarios — steady,
benign bump, an SLO that really breaks, capacity dropping, capacity recovering,
flapping latency and a diurnal curve — two rounds each, and prints a summary of
which arm held up where (`cd compare && go test -run TestSLOAnchorMatrix -v
-timeout 40m`). Goodput/s, best in bold:

| scenario | uber(60) | uber(100) | uber(140) | 2x | SLO |
|---|---:|---:|---:|---:|---:|
| steady, capacity unknown (150/s) | 76 | 116 | **156** | 115 | 115 |
| benign bump 20 -> 60ms | 76 | **116** | 58 | 108 | 109 |
| SLO breaks 20 -> 150ms | 25 | 42 | 58 | 38 | 38 |
| capacity drops 150 -> 40/s | 25 | 42 | 58 | **59** | 58 |
| capacity recovers 40 -> 150/s | 52 | 75 | **98** | 25 | 25 |
| flapping latency 20 <-> 70ms | 76 | **116** | 85 | 100 | 113 |
| diurnal 100 +/- 60/s | **58** | 56 | 33 | 58 | 57 |

Read `%<=SLO` alongside goodput, not goodput alone. What it says:

- **No arm holds everywhere.** A fixed rate is excellent while capacity is what
  it was and poor the moment it moves: `uber(100)` is at 52% inside the SLO on
  the diurnal curve and 38% when capacity drops, and `uber(140)` drops to 38% on
  a benign 20 -> 60ms bump.
- **The adaptive arms stay inside the SLO across the board** (78-95%) without
  being told the capacity. That is what the trade buys.
- **Their weak spot is recovery, not backoff.** 40 -> 150 req/s costs them the
  scenario outright (25 vs 98), because `coldStart` does not re-arm and the
  climb is `UpStep` per tick. `Config.RecoverRamp` is the opt-in answer: at 0.5
  that scenario goes 25 -> 52, and the capacity-drop one goes 59 -> 46, because
  doubling is also how you overshoot a cliff. It ships off for that reason.
- `AnchorSLO` is the steadier of the two modes (flapping latency: 112-113 req/s
  in both rounds, against 95-105), and it is the one that survives a benign bump
  once the SLO has real headroom.

One caveat on the model: it prices load from the limiter's *target* rate, so a
fixed-rate arm that actually completes 1.1-1.5x its nominal value is priced as
if it sent exactly that. That flatters the uber rows slightly.

### Time hysteresis: `BackoffTicks` / `RecoverTicks`

Both anchors move the rate on a single tick. `BackoffTicks` and `RecoverTicks`
require that many *consecutive* ticks to agree before a step is spent — CoDel's
interval, reduced to a counter. Defaults are 1/1 under `AnchorBaseline` (the
historical behaviour) and 2/2 under `AnchorSLO`, where the threshold is fixed and
jitter across it is the main false-positive risk.

This is not just smoothing. The steps are asymmetric (-30% down, +5% up), so a
noisy signal does not average out — one bad tick and one good tick *ratchet* the
rate down. Requiring agreement is what stops that. A tick inside the deadband
resets both counters, so the agreement has to be in a row, not merely cumulative.

## Inbound: protecting your own service (concurrency limiting)

The limiter above protects a *downstream*. To protect a *service you run* — the
inbound direction — use `ConcurrencyLimiter` instead. It shares the same brain
(the fast/slow EWMA baseline, target window and p99 guard) but swaps the actuator
from a leaky bucket to a concurrency semaphore, because the inbound problem is
different:

- **Rate is not your knob.** Clients decide how fast requests arrive; what you
  control is how many you serve *at once*. By Little's Law `in-flight = rate ×
  latency`, so a fixed rate cap does not bound concurrency — the same rate at 10×
  the latency is 10× the in-flight requests.
- **Blocking would amplify an overload.** A middleware that blocks on admission
  builds a queue holding the very goroutines, sockets and connections it is meant
  to protect. So admission here is *non-blocking*: at the limit it refuses at
  once, and you shed load with a fast 429 / `ResourceExhausted`.
- **The signal is clean by construction.** You feed the handler's own duration,
  and since admission never waits there is no throttle time to contaminate it.

```go
lim := agilelimiter.NewConcurrency(agilelimiter.ConcurrencyConfig{
	MinLimit:     10,   // never admit fewer than this many concurrent
	MaxLimit:     500,  // hard ceiling you promise the service
	InitialLimit: 50,   // optional; defaults to MaxLimit/8
	SLOTarget:    200 * time.Millisecond, // optional: p99 tail guard
})
lim.Start()
defer lim.Stop()

// Hot path: Do admits the request if there is room, times the handler, and feeds
// the handling time back to the controller. At the limit it returns
// ErrOverloaded at once — refusing, not queuing.
err := lim.Do(func() error { return handle(w, r) })
if errors.Is(err, agilelimiter.ErrOverloaded) {
	http.Error(w, "overloaded", http.StatusTooManyRequests)
}
```

When the slot must span more than one call, or you want to release it yourself,
drive the primitives directly. Every successful `TryTake` must be paired with
exactly one `Release`:

```go
if !lim.TryTake() {
	http.Error(w, "overloaded", http.StatusTooManyRequests)
	return
}
defer lim.Release()            // released even if the handler panics
start := time.Now()
handle(w, r)
lim.Observe(time.Since(start)) // this request's handling time -> drives the controller
```

`Limit()` returns the current concurrency limit and `InFlight()` the number of
requests being served — export both. It takes the same tuning knobs as the
outbound limiter; `MinLimit`/`MaxLimit`/`InitialLimit` take the place of
`MinRate`/`MaxRate`/`InitialRate`. (`MaxLimit` is required.) For the four
HTTP/gRPC integration seams, see `docs/integrations.md`.

### Drop-in HTTP middleware

`agilelimiter/httpserver` wraps a `net/http` handler (stdlib only, so the module
stays zero-dependency). Apply it **once, around the whole protected subtree** —
every route beneath shares one concurrency budget, because they compete for the
same process resources (do not build a limiter per endpoint):

```go
lim := agilelimiter.NewConcurrency(agilelimiter.ConcurrencyConfig{
	MaxLimit:     500,
	InitialLimit: 50,
	SLOTarget:    200 * time.Millisecond,
})
lim.Start()
defer lim.Stop()

mux := http.NewServeMux()
mux.HandleFunc("/api/things", getThings)
http.ListenAndServe(":8080", httpserver.Wrap(mux, lim,
	httpserver.WithRetryAfter(time.Second))) // sends Retry-After on a 429
```

`Wrap` runs the handler only for admitted requests and observes only those, so
the signal stays clean; a shed request never reaches the handler and is never
measured. `Wrap` releases the slot even if the handler panics, and
`httpserver.StatsHandler(lim)` exposes the current limit and signals as JSON —
mount it *outside* the wrapper so it stays readable while the service is
saturated.

#### What the middleware records, and what you tell it

A handler that fails instantly reports a ~0 handling time, which would otherwise
read as "this service is fast and healthy" and raise the limit. So the response is
inspected before it is measured.

Same one-hook design as the outbound side:

```go
type Verdict func(r *http.Request, status int, h http.Header, d time.Duration) agilelimiter.Outcome
```

`DefaultVerdict` gives you:

| Response | Treated as | Why |
|---|---|---|
| request context canceled | not observed | the client went away; the handler returned early because nobody is listening |
| 429, 502, 503, 504 | outage → back off | "cannot serve right now" |
| 500, 501, other 5xx | a normal round trip | "your code has a bug" — backing off fixes nothing |
| a cache hit (`Age > 0`, `X-Cache: HIT`, …) | not observed at all | a cheap path says nothing about remaining capacity |

Everything else is your call, in one function:

```go
httpserver.Wrap(mux, lim, httpserver.WithVerdict(
	func(r *http.Request, status int, h http.Header, d time.Duration) agilelimiter.Outcome {
		switch {
		case strings.HasPrefix(r.URL.Path, "/health"), r.URL.Path == "/metrics":
			return agilelimiter.Aborted // cheap by design, and noisy on a quiet service
		case r.Method == http.MethodOptions:
			return agilelimiter.Aborted // CORS preflight
		case httpserver.IsCacheHitHeader(h):
			return agilelimiter.Aborted
		default:
			return httpserver.DefaultVerdict(r, status, h, d)
		}
	}))
```

`IsCacheHitHeader` reads only headers your handler already wrote, so marking a cache
hit is one line: `w.Header().Set("X-Cache", "HIT")` before the body. The same
reasoning as the outbound side applies — with 95% of requests served from memory
in 0.2ms, p50 tracks the cache, not the 5% that reach the database.

Two cases the default does not handle, because they need knowledge it does not
have: **health/metrics/preflight** (exclude by path, as above) and **long-lived
streams** (SSE, websocket, long poll — give them their own budget, or the median
of every other request pays for them).

## Why the filtering works this way

Everything above is one rule: **a duration is evidence only while it measures the
resource you are protecting.** A cache hit, an instant 503, a `/healthz` probe and
a circuit-breaker fallback all return in about a millisecond, and none of them say
anything about remaining capacity. Fed into one p50 they make a saturated
dependency look fast — and the controller's answer to "fast" is to raise the rate.

It cannot be fixed inside the controller: deciding what a response *means* needs
the response. It cannot be a fixed set of built-in cases either, for two reasons:

- **The list does not converge.** Every year adds a proxy header, an async
  endpoint, a framework convention. One option per case is a treadmill.
- **Every built-in case is wrong for somebody.** A 202 is free if your queue is
  cheap and expensive if enqueueing *is* the bottleneck; a 4xx flood is noise from
  a client bug and signal from a broken auth service. The library does not know
  which company it is in.

So it is split into three layers:

| Layer | Who owns it | Can it be overridden? |
|---|---|---|
| **Truncated measurement** — the call was canceled, or the client disconnected | the library | **no.** A truncated sample is not a fast response, it is not a measurement at all |
| **Default policy** — 429/5xx, dial failures, cache hits | `DefaultVerdict`, about six lines | yes — replace it wholesale |
| **Everything else** — health paths, 202, 4xx floods, your own error types | you, one line each | — |

The middle layer ships as a *function* rather than as options because a function
is readable, testable as a pure function, and composable: your verdict can call
`DefaultVerdict` for everything it does not care about.

### What is left to you

| Case | Why the default cannot decide | Answer |
|---|---|---|
| `/healthz`, `/metrics`, `/favicon.ico` | depends on your routes; on a quiet service they can be most of the samples | `Aborted` by path |
| CORS preflight, `HEAD` | depends on your routes | `Aborted` |
| 202 Accepted / fire-and-forget | depends on whether enqueueing is your bottleneck | `Aborted` or `Reached` |
| circuit-breaker fallback | your own error, not a `*net.OpError` | `Unreachable` |
| 4xx flood | your business judgement | `Aborted` |
| SSE / long poll / websocket | not cheap — it drags the median up for everyone else | its own limiter |

### What no amount of filtering fixes

These are properties of the control law and of the deployment, not of the signal:

- **Capacity that moves faster than the controller.** Steady-state ramping is
  `UpStep` per tick (5% by default), so a swing with a period of a few seconds
  outruns it.
- **Cold start and recovery.** A *fresh* limiter doubles while it has never seen
  congestion (`SlowStartGain`, default 2.0), so it finds the knee in
  `log2(MaxRate/InitialRate)` ticks — 6 instead of 80 for 10→500. Recovery from a
  deep dip is the same shape (`RecoverRamp`): once the observed latency is at or
  below half the tolerated window the multiplicative probe returns, because
  nothing about that reading is ambiguous. What is left to you is a limiter that
  *restarts* cold and has to re-learn a knee it already knew — persist the last
  good rate across restarts for that.
- **A cache going cold.** That is a step change; the backstop is single-flight and
  sizing `MaxRate` for a *cold* origin, not a better classifier.
- **Many instances.** Each process converges on its own; there is no shared view
  of a shared dependency.

## Signal hygiene: what must not enter the sample

Everything above is about which *policy* a duration is subject to. This section is
about whether the duration measures the downstream at all. The controller has one
input and no way to tell a meaningful sample from a misleading one, so
`compare/pollution_test.go` measures the cost of getting it wrong. Every arm runs
against a **flat** downstream — latency independent of rate — which means no rate
change in it can be congestion by construction. The control arm is the same
limiter with the pollution removed:

| sample stream | control | polluted | what happened |
|---|---:|---:|---|
| 80% of requests answer in 1ms (cache hits) | 135/s | 136/s | no damage: the miss path could not saturate |
| 5% of requests always cost 300ms | 136/s | **54/s** | the tail guard read a fixed population as congestion |
| an internal retry doubles the sample | 135/s | **24/s, 0% in SLO** | the absolute check compared twice the real latency |
| 20ms of caller work, sample stays under `SLOTarget` | 136/s | 136/s | no damage: window and signal shift together |
| 50ms of caller work, sample crosses `SLOTarget` | 135/s | **24/s, 0% in SLO** | same pollution, now it trips the guard |

**The relative window is self-correcting; the guard is not.** In every row that
collapsed, the median law said *healthy*: a constant offset moves `fast` and
`slow` together, and a ratio does not care about it. The absolute comparison
does, and it turns a polluted sample into a 30%-per-tick backoff straight to
`MinRate`.

**The guard compares one tick's p99 against `SLOTarget`.** A SLO is a fraction
over a long window ("99% of requests under 200ms"); a per-tick p99 crossing that
number is *normal* for a service that meets it — roughly half the ticks. And half
the ticks is enough to walk the rate to the floor, because `0.70 x 1.05 < 1`.
Measured on a downstream with no queueing at all: 5% of requests that always cost
300ms took 300 req/s to 21.

`TailFactor` (default 1.0 — the guard fires at the SLO itself) moves that
threshold and nothing else. Set it to 1.5–2 when your tail is made of different
request types rather than of queueing: `SLOTarget` stays the number in the
contract, the window cap and the `AnchorSLO` target stay where they are, and only
the guard gets room.

`TailTolerance` (default 0 — off) changes the guard's *statistic* instead: it fires
when the smoothed **fraction of the tick above the target** exceeds the tolerance
by more than three standard errors. The caller supplies the policy (how much slow
traffic is acceptable) and the controller supplies the evidence bar, derived from
the sample count it actually has — so a busy service gets a tight margin and a
quiet one gets a wide one, because a quiet service measures less. That is the
difference between "5% of my traffic is a report endpoint" and "everything got
slower":

| 5% of requests always cost 300ms | achieved/s | %<=SLO | rate |
|---|---:|---:|---:|
| `TailTolerance` 0 (p99 test) | 54 | 94% | **21** |
| `TailTolerance` 0.05 | 136 | 95% | **300** |
| `TailTolerance` 0.10 | 135 | 95% | 300 |

A tolerance set *at* the cohort's share holds, because the margin absorbs the
sampling noise — there is no headroom factor to tune. And when *every* request
costs 300ms all three settings still take the rate to `MinRate` (25 req/s, 0%
inside the SLO), so the protection is intact.

`Stats().TailThreshold` is the derived bar (`tol + 3σ`), wider exactly where the
sample count is small. The sweep in `compare` (`TestTailMargin`) measures 0.149 at
~8 samples per tick, 0.105 at ~24 and 0.098 at ~33, with no false positive at any
of them. The same sweep shows where the evidence runs out: a cohort three points
above the tolerance is not resolvable at ~24 samples per tick, and the guard stays
quiet rather than guessing.

The shape of the fix is always the same — decide what the number means *before*
it reaches `Observe`:

| if a sample can be… | then |
|---|---|
| a cache hit, a health check, a CORS preflight | classify it `Aborted`; it says nothing about remaining capacity |
| a different endpoint with its own latency profile | give it its own limiter; do not average two populations into one p50 |
| a retried attempt | use `Take`/`Observe` per attempt — `Do` measures exactly one attempt by definition |
| caller-side work (marshal, lock waits, GC) | measure only the call, and remember that the overhead eats your SLO headroom |
| an attempt truncated by *your* deadline | make sure the deadline is longer than `SLOTarget`: a shorter one caps every sample, and the limiter goes blind above it |

That last row is the one that fails quietly. `DeadlineExceeded` is deliberately
recorded as a real round trip (a downstream that times out *is* struggling), but
if the deadline is shorter than the downstream's healthy latency, every sample
becomes the deadline: a flat line at 30ms that looks perfectly healthy while every
request fails. The limiter will happily raise the rate into it.

## Pitfalls (read before tuning)

- **Baseline pollution.** If the slow EWMA is allowed to rise freely, a long
  overload drags the baseline up to the congested latency, the tolerated window
  rises with it, and the controller freezes in the overloaded state. This implementation avoids
  it two ways: the baseline is updated *asymmetrically* (falls quickly, rises
  slowly) and its upward drift is hard-capped per tick by `MaxBaselineRise`, so a
  sustained overload cannot move the target at all.
- **Fast failures look like health.** A connection that is refused, a DNS
  failure, an open breaker: all return in microseconds, which the controller
  reads as a fast response and answers by raising the rate. Classify them as
  `Unreachable` (see above). This is the one mistake that makes the limiter
  actively harmful rather than merely suboptimal.
- **Coordinated omission.** In the measurement path, never stop sampling when the
  downstream slows. If your workers block and stop issuing calls, you go blind
  exactly when it matters most — a stalled caller produces no sample at all.
- **Oscillation.** If the rate rings, lengthen the half-lives, increase
  `Deadband`, shrink `UpStep`, or lower `Gain`.
- **Observability.** Export `Rate()`, `Stats()` (baseline, fast, the last tick's
  p50/p99) — and `Stats().Samples` above all: with no samples a step changes
  nothing, so a *frozen* rate and a *steady* one look identical. Alert on
  `Samples == 0` for more than a few ticks; it is what tells you the controller
  went blind (low traffic, or a pipeline that stopped observing because every
  response was a cache hit).

## Testing

Tests use a virtual clock and drive the control step directly, so they are fully
deterministic:

```
go test ./...
go vet ./...
gofmt -l .
```

A runnable end-to-end demo lives in `example/`:

```
go run ./example            # outbound: pacing requests to a downstream
go run ./example/concurrency # inbound: capping concurrency on a service
go run ./example/httpserver  # inbound: a service shedding load under a swarm
```
