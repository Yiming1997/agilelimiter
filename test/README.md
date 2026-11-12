# agilelimiter performance harness (plugin-based)

The performance and scenario harness for agilelimiter. It follows the shape of
`go-agile-pool/test`: **every capability is an independent plugin; the host only
splits segments, plans dependencies and schedules lifecycles.** Adding a
capability means adding one file and one line in `plugins.go`.

`test/` is a separate Go module; `replace agilelimiter => ../` forces it to
compile the local library, never a published copy.

## Plugins

| Segment | Purpose | Key options (defaults) | Lifecycle |
|---|---|---|---|
| `--limiter` | create and hold the limiter | `kind`(rate) `preset`(none) `anchor`(unset) `min`(5) `max`(500) `initial`(0) `slo`(100ms) `tick`(250ms) plus all control-law knobs | Start creates+starts / End stops |
| `--downstream` | simulated downstream | `base`(20ms) `capacity`(150/s) `curve`(mm1) `slo`(100ms) `cache`(0) `cache-latency`(1ms) `origin-up`(true) `phases`(empty) | — |
| `--load` | drive concurrent load and summarise | `workers`(256) `settle`(3s) `dur`(6s) `refuse-sleep`(2ms) | — |
| `--metrics` | periodic sampling + summary | `interval`(1s) `format`(csv) `file`(metrics.csv) `wait-exit`(0) | Start sampler / End finalise |
| `--profile` | pprof around the session | `cpu`(false) `mem`(false) | Start CPU / End CPU+heap |
| `--hcache` | scenario: origin down, CDN still hits; hits must not be throttled | `hit`(0.95) `min`(5) `max`(500) `settle`(3s) `dur`(4s) `min-good`(0) | — |

`--limiter` covers both actuators: `kind=rate` is the outbound `Limiter`
(blocking `Take`), `kind=concurrency` is the inbound `ConcurrencyLimiter`
(non-blocking `TryTake`).

Dependencies: `--load ← --limiter + --downstream`, `--metrics ← --limiter`.
Missing dependencies are auto-inserted with defaults; misordered, duplicated or
cyclic segments are usage errors (exit code 2).

## Quick start

```text
go run . --list
go run . --help load

go run . --downstream base=20ms capacity=150 \
         --load workers=256 settle=3s dur=6s \
         --metrics interval=1s format=csv file=metrics.csv

go run . --limiter preset=latency-first slo=100ms max=300 \
         --downstream base=20ms capacity=150 \
         --load workers=256 settle=3s dur=6s

go run . --hcache
```

`--hcache` is the CDN case: the origin is down, 95% of requests are cached, and
the invariant is that cache hits keep flowing at edge capacity. It currently
FAILS because every hit consumes a rate slot, so backing off on the failing
misses throttles the hits too (see `TODO.md`). `--min-good` defaults to
`0.5 x hit x max` and the failure names the cause.

## Exit codes

- `0` success;
- `1` runtime error (plugin Start/Run/End failed, or a scenario assertion failed);
- `2` usage error.

Errors carry a `[plugin]` prefix.
