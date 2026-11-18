<p align="center">
  <img src="docs/logo.svg" alt="agileLimiter logo" width="144" height="144">
</p>

<h1 align="center">agileLimiter</h1>

[中文文档](README.zh.md)

<p align="center">
  Adaptive rate limiting for outbound dependencies, and adaptive concurrency
  limiting for inbound services.
</p>

[![CI](https://github.com/Yiming1997/agilelimiter/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Yiming1997/agilelimiter/actions/workflows/ci.yml)
[![Go >= 1.24](https://img.shields.io/badge/go-%3E%3D1.24-00ADD8)](go.mod)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Tag](https://img.shields.io/github/v/tag/Yiming1997/agilelimiter?label=tag)](https://github.com/Yiming1997/agilelimiter/tags)
[![Go Reference](https://pkg.go.dev/badge/github.com/Yiming1997/agilelimiter.svg)](https://pkg.go.dev/github.com/Yiming1997/agilelimiter)

`agileLimiter` watches latency and adjusts how much traffic to allow. It is a
small, zero-dependency Go module. You give it a ceiling and a safe floor; it tries
to use the headroom above the floor without overwhelming the other side.

- **Outbound:** pace calls to a dependency with `Limiter`.
- **Inbound:** cap concurrent work in your own service with `ConcurrencyLimiter`.
- **Safe by default:** normal traffic never goes below `FloorRate`; an outage can
  fall to the lower `ProbeRate`.

## Install

```bash
go get github.com/Yiming1997/agilelimiter
```

Requires Go 1.24 or newer. No external dependencies.

## Outbound: call a dependency

```go
import (
    "time"

    "github.com/Yiming1997/agilelimiter"
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
import "github.com/Yiming1997/agilelimiter/httpclient"

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
    MinLimit:     2,                      // set it; do not let it default to 1
    FloorLimit:   10,                     // concurrency to keep while a dependency wobbles
    ProbeLimit:   5,                      // the floor during an outage; not 1
    MaxLimit:     50,                     // ≈ real concurrency capacity (pool / worker count)
    InitialLimit: 10,                     // set it; do not let MaxLimit/8 decide
    SLOTarget:    200 * time.Millisecond, // required; without it this degrades to throughput-only
    Classify:     myClassify,             // "failed without doing any work" -> Unreachable
})
lim.Start()
defer lim.Stop()
```

Every field is there for a reason (all measured):

| Field | Why |
|---|---|
| `SLOTarget` | **Required.** Without it there is no latency anchor: the controller watches the median alone, and the baseline follows the load upward — the limiter degrades into protecting throughput only. Concurrency climbs to several times your real capacity, latency climbs with it, and throughput does not improve at all. |
| `FloorLimit` / `ProbeLimit` | When omitted they inherit `MinLimit`, which defaults to 1. A single dependency failure then drops concurrency to 1, which is a shutdown. |
| `MaxLimit` | Use your **real concurrency capacity** (connection pool / worker count), not "leave some headroom": it is the only hard boundary left once the controller has backed off. |
| `InitialLimit` | Defaults to `MaxLimit/8`. A concurrency ceiling and a capacity are not proportional: `MaxLimit=1000` starts you at 125, which can be many times your real capacity. |
| `MinLimit` | Only the fallback for `FloorLimit` / `ProbeLimit`; it does nothing once both are set explicitly. |
| `Classify` | A request that fails without doing any work (cached 503, auth rejection, open circuit breaker) takes ~0 time. Unless it is marked `Unreachable` it reads as "the service is fast", and the limiter opens up exactly while the service is degrading. |

```go
func myClassify(err error) agilelimiter.Outcome {
    if errors.Is(err, ErrAuthRejected) || errors.Is(err, ErrCacheMiss) {
        return agilelimiter.Unreachable // no work done, ~0 duration, not a capacity signal
    }
    return agilelimiter.Reached
}
```

Admission is non-blocking: at the limit, `Do` returns `ErrOverloaded` immediately
instead of building a queue. Use `TryTake`/`Release` if you need to hold a slot
across multiple calls.

```go
err := lim.Do(func() error {
    return handleRequest(w, r)
})
if errors.Is(err, agilelimiter.ErrOverloaded) {
    http.Error(w, "overloaded", http.StatusTooManyRequests)
}
```

### HTTP server middleware

```go
import "github.com/Yiming1997/agilelimiter/httpserver"

root := http.NewServeMux()
root.Handle("/debug/limiter", httpserver.StatsHandler(lim))
root.Handle("/", httpserver.Wrap(mux, lim,
    httpserver.WithRetryAfter(2*time.Second)))

http.ListenAndServe(":8080", root)
```

`httpserver.Wrap` sheds with `429` when the limit is reached and records only
admitted requests. Keep `StatsHandler` outside the wrapped subtree so it remains
readable while the service is saturated.

`WithRetryAfter` is worth adding: a `429` without `Retry-After` makes clients
retry immediately and kill the service you just protected.

> The middleware takes a concurrency slot for **every** request, including cache
> hits that cost almost nothing. If most of your traffic is cheap, put the gate
> only around the expensive part (manual `TryTake` / `Release`) — the endpoint
> throughput difference is two orders of magnitude.

## Configuration

Most services only need these fields:

| Field | Meaning |
|---|---|
| `MaxRate` / `MaxLimit` | Hard ceiling. Required. |
| `FloorRate` / `FloorLimit` | Normal operating floor. Keep it near the static rate you trust. |
| `ProbeRate` / `ProbeLimit` | Floor while the downstream is unreachable. Keep it low. |
| `SLOTarget` | Latency target. **Required for inbound**, optional outbound. |
| `Classify` | Which errors mean "never reached the downstream". |
| `MinRate` / `MinLimit` | Compatibility fallback: inherited by `FloorRate` / `ProbeRate` when they are zero. |
| `InitialRate` / `InitialLimit` | Where it starts before the first feedback. Defaults to `MaxRate` / 8. |

Set `FloorLimit`, `ProbeLimit` and `InitialLimit` explicitly for inbound — see the
inbound configuration above.

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
