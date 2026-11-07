<p align="center">
  <img src="docs/logo.svg" alt="agileLimiter logo" width="144" height="144">
</p>

<h1 align="center">agileLimiter</h1>

<p align="center">
  Adaptive rate limiting for outbound dependencies, and adaptive concurrency
  limiting for inbound services.
</p>

`agileLimiter` watches latency and adjusts how much traffic to allow. It is a
small, zero-dependency Go module. You give it a ceiling and a safe floor; it tries
to use the headroom above the floor without overwhelming the other side.

- **Outbound:** pace calls to a dependency with `Limiter`.
- **Inbound:** cap concurrent work in your own service with `ConcurrencyLimiter`.
- **Safe by default:** normal traffic never goes below `FloorRate`; an outage can
  fall to the lower `ProbeRate`.

## Install

```bash
git clone https://github.com/Yiming1997/agilelimiter.git
cd agilelimiter
go test ./...
```

Requires Go 1.20 or newer. No external dependencies.

The module path is currently `agilelimiter`. To use it from another local module,
add a `replace` directive for now:

```text
replace agilelimiter => ../agilelimiter
```

## Outbound: call a dependency

```go
import (
    "time"

    "agilelimiter"
)

lim := agilelimiter.New(agilelimiter.Config{
    MaxRate:   500,                     // hard ceiling
    FloorRate: 250,                     // normal floor: a rate you trust
    ProbeRate: 10,                      // outage floor: keep probing, don't hammer
    SLOTarget: 200 * time.Millisecond,  // optional; your latency red line
})
lim.Start()
defer lim.Stop()

// Do takes a slot, times the call, and feeds the result back to the controller.
err := lim.Do(func() error {
    return callDownstream(ctx)
})
```

`MaxRate` is the only required field. Start with `MaxRate`; add `FloorRate` when
you know the static rate you trust, and `ProbeRate` when you want outages to back
off further.

### HTTP client

```go
import "agilelimiter/httpclient"

client := &http.Client{
    Transport: httpclient.Wrap(http.DefaultTransport, lim),
}
```

`httpclient.Wrap` paces each request, measures the round trip, and reports it to
the limiter. Use `httpclient.WithVerdict` when some responses should not count as
capacity signals (cache hits, redirects, cheap errors, and so on).

### Manual Take / Observe

Use this when the call is not a plain synchronous function:

```go
lim.Take()
start := time.Now()
err := callDownstream(ctx)
lim.ObserveOutcome(time.Since(start), err)
```

If the call never reached the downstream, return `agilelimiter.Unreachable` from
`Config.Classify` (or call `lim.Unavailable()` on the manual path). Do not count a
refused connection as a fast success.

## Inbound: protect your own service

For inbound traffic, the useful knob is concurrency, not rate: clients decide how
fast requests arrive, while you decide how many run at once.

```go
lim := agilelimiter.NewConcurrency(agilelimiter.ConcurrencyConfig{
    MaxLimit:   500, // hard ceiling
    FloorLimit: 100, // normal floor
    ProbeLimit: 10,  // outage floor
    SLOTarget:  200 * time.Millisecond,
})
lim.Start()
defer lim.Stop()

err := lim.Do(func() error {
    return handleRequest(w, r)
})
if errors.Is(err, agilelimiter.ErrOverloaded) {
    http.Error(w, "overloaded", http.StatusTooManyRequests)
}
```

Admission is non-blocking: at the limit, `Do` returns `ErrOverloaded` immediately
instead of building a queue. Use `TryTake`/`Release` if you need to hold a slot
across multiple calls.

### HTTP server middleware

```go
import "agilelimiter/httpserver"

root := http.NewServeMux()
root.Handle("/debug/limiter", httpserver.StatsHandler(lim))
root.Handle("/", httpserver.Wrap(mux, lim,
    httpserver.WithRetryAfter(time.Second)))

http.ListenAndServe(":8080", root)
```

`httpserver.Wrap` sheds with `429` when the limit is reached and records only
admitted requests. Keep `StatsHandler` outside the wrapped subtree so it remains
readable while the service is saturated.

## Configuration

Most services only need these fields:

| Field | Meaning |
|---|---|
| `MaxRate` / `MaxLimit` | Hard ceiling. Required. |
| `FloorRate` / `FloorLimit` | Normal operating floor. Keep it near the static rate you trust. |
| `ProbeRate` / `ProbeLimit` | Floor while the downstream is unreachable. Keep it low. |
| `SLOTarget` | Optional latency target. Enables the tail guard and the SLO anchor. |
| `Classify` | Which errors mean "never reached the downstream". |

`MinRate` / `MinLimit` still work as a compatibility fallback: if `FloorRate` or
`ProbeRate` is zero, it inherits the legacy value.

Presets are optional shortcuts:

```go
Preset: agilelimiter.PresetLatencyFirst
```

Start with the zero preset and explicit `MaxRate`/`FloorRate`; add a preset only
when its policy matches what you want.

## How it behaves

On every tick, the controller looks at the median and tail latency and adjusts the
allowed rate or concurrency:

- latency is healthy: try a little more
- latency is above the target window: back off
- latency is in between: hold steady
- tail latency is above the SLO: back off

It keeps a slow baseline of normal latency, so a temporary spike does not
immediately rewrite the target. When the downstream is unreachable, the rate can
fall to `ProbeRate`; a normal response brings it back to the regular floor.

There are no formulas to tune here. If you need the control law and the measured
trade-offs, read [docs/design.md](docs/design.md).

## Observability

```go
log.Printf("rate=%.0f stats=%+v", lim.Rate(), lim.Stats())
log.Printf("resolved config=%+v", lim.Config())
```

Export at least:

- `Rate()` / `Limit()`
- `Stats().Samples` — zero means the controller is blind
- `Stats().P50` and `Stats().P99`
- `Config()` — the defaults and presets actually in effect

## Things to know

- This is **not a circuit breaker**. Pair it with one if “stop calling entirely”
  is part of your failure policy.
- It is **per process**. Multiple instances each learn and limit independently.
- It does not queue. Inbound overload is refused, not buffered.
- It does not distribute quotas across machines.
- `FloorRate` is a capacity bet. If real capacity drops below it, the floor is no
  longer safe; use `ProbeRate` for actual outages and alert on sustained floor
  operation.
- Measure the downstream round trip, not the time spent waiting inside the
  limiter. `Do` does this for you.

## Examples and tests

```bash
go run ./example                 # outbound
go run ./example/concurrency     # inbound
go run ./example/httpserver      # HTTP middleware

go test ./...
go test -race ./...
```

More detail:

- [docs/design.md](docs/design.md) — controller design and trade-offs
- [docs/integrations.md](docs/integrations.md) — HTTP/gRPC integration notes
- [httpclient](httpclient/) and [httpserver](httpserver/) — drop-in adapters
- [sim/](sim/) — virtual-clock simulations and comparisons

## License

MIT. See [LICENSE](LICENSE).
