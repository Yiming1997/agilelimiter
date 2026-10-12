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

## Pitfalls (read before tuning)

- **Baseline pollution.** If the slow EWMA is allowed to rise freely, a long
  overload drags the baseline up to the congested latency, `ratio` returns to 1,
  and the controller freezes in the overloaded state. This implementation avoids
  it by updating the baseline *asymmetrically* — it falls quickly, rises slowly.
- **Coordinated omission.** In the measurement path, never stop sampling when the
  downstream slows. If your workers block and stop issuing calls, you go blind
  exactly when it matters most — a stalled caller produces no sample at all.
- **Oscillation.** If the rate rings, lengthen the half-lives, increase
  `Deadband`, shrink `UpStep`, or lower `Gain`.
- **Observability.** Export `Rate()`, the baseline, and the current p50/p99.
  An adaptive limiter you cannot see into is one you cannot debug.

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
