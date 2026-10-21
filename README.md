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

```go
lim := agilelimiter.New(agilelimiter.Config{
	MinRate:     20,         // never go below this many req/s
	MaxRate:     500,        // hard ceiling you promise the downstream
	InitialRate: 50,         // cold-start low, let it ramp up
	SLOTarget:   200 * time.Millisecond, // optional: p99 tail guard
})
lim.Start()
defer lim.Stop()

// Hot path: wrap the downstream call in Do. It takes a slot, times the call, and
// feeds the measured round-trip time back to the controller — you never touch
// Take/Observe, so you cannot feed it the wrong latency.
err := lim.Do(func() error { return doRequest(ctx) })
```

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
downstream?". Say so once at the boundary:

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

`Wrap` takes a slot, measures the round trip, and skips the observation for a
response that `IsCacheHit` recognises — `Age > 0` (RFC 7234), or a `HIT` in
`X-Cache` / `X-Proxy-Cache` / `CF-Cache-Status`. Errors still go through
`Config.Classify`. Use `httpclient.WithCacheHit(fn)` for another signal.

A **429 or any 5xx** is handled the same way as a refused connection: it is not a
Go error, so by default it would be recorded as a fast healthy round trip, and
the controller would accelerate into the service that just said "no". `Wrap`
reports those as an outage and backs off. `httpclient.WithErrorStatus(codes...)`
replaces that set; pass an empty list to observe every status.

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
   `high = slow * (1 + TargetInflation)`, `low = high * (1 - Deadband)`:
   - `fast <= low` → healthy, additive-increase probe (`+UpStep`).
   - `fast >= high` → congested, proportional decrease `1 - Gain*(excess/high)`,
     capped at `-DownStep`.
   - in between → hold (deadband).
4. If p99 exceeds `SLOTarget`, force a full `-DownStep`.
5. Clamp to `[MinRate, MaxRate]` and push to the bucket.

**Asymmetry is intentional**: back off fast, ramp up slowly. It is friendlier to
the downstream and resists oscillation around the knee. The baseline EWMA itself
is also asymmetric — it falls quickly and rises slowly (see Pitfalls).

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
	InitialLimit: 50,   // cold-start low, let it ramp up
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

#### What the middleware does *not* observe

A handler that fails instantly reports a ~0 handling time, which would otherwise
read as "this service is fast and healthy" and raise the limit. So the response
is inspected before it is measured:

| Response | Treated as | Why |
|---|---|---|
| 429, 502, 503, 504 | outage → back off | "cannot serve right now" |
| 500, 501, other 5xx | a normal round trip | "your code has a bug" — backing off fixes nothing and would turn a failing endpoint into a smaller failing endpoint |
| a cache hit (`Age > 0`, `X-Cache: HIT`, …) | not observed at all | a cheap path says nothing about how much of the real capacity is left |

```go
httpserver.Wrap(mux, lim,
	httpserver.WithErrorStatus(429, 503),                 // narrow the set
	httpserver.WithCacheHit(func(h http.Header) bool {    // your own signal
		return h.Get("X-Served-From") == "cache"
	}))
```

`IsCacheHit` reads only headers your handler already wrote, so marking a cache
hit is one line: `w.Header().Set("X-Cache", "HIT")` before the body. The same
reasoning as the outbound side applies — with 95% of requests served from memory
in 0.2ms, p50 tracks the cache, not the 5% that reach the database.

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
