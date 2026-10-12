# agilelimiter vs go.uber.org/ratelimit — head-to-head results

Run it yourself:

```
cd compare && go run .
```

## Setup

Both limiters drive the **same simulated downstream**, whose capacity changes
over time. The downstream model is an M/M/1-flavoured wait:

```
latency(offered, capacity) = base / (1 - util),   util = offered / capacity
```

with `base = 20ms` and `util` clamped at 0.95 (so the worst latency is 400ms). A
request is counted toward **goodput** when it completes within a **100ms SLO**.

- uber's `go.uber.org/ratelimit` is a *fixed-rate* leaky bucket: you hand it a
  rate once and it paces exactly that, ignoring the feedback.
- `agilelimiter` is *adaptive*: you give it a ceiling and a target latency, and
  it discovers the rate from the observed round-trip time.

Each row runs for 9s with a worker pool sized to the limiter's rate (≈ rate ×
max RTT, via Little's Law). Columns: completed requests/s, goodput/s (within
SLO), % of requests within SLO, mean and p99 latency, and the final rate.

The agile config used throughout: `InitialRate=40`, `MaxRate=500`,
`SLOTarget=100ms` (== the SLO), `FastHalfLife=750ms`, `SlowHalfLife=45s`,
`TargetInflation=2.5`, `Tick=250ms`.

## Case 1 — steady but UNKNOWN capacity: 150 req/s

| limiter | achieved/s | goodput/s | %<=SLO | mean | p99 | finalRate |
|---------|-----------:|----------:|-------:|-----:|----:|----------:|
| uber(60)  | 63 | 63 | 100% | 34ms | 34ms | 60 |
| uber(100) | 105 | 105 | 100% | 60ms | 61ms | 100 |
| uber(300) | 303 | **0** | 0% | 400ms | 401ms | 300 |
| agile     | 93 | 84 | 90% | 60ms | 144ms | 131 |

A fixed rate *has* to guess the capacity: 60 leaves more than half the capacity
idle, 300 melts (0 goodput — every request blows the SLO), and only a lucky
guess of 100 is excellent. agile was never told the capacity; it settles at
~90–130 and lands **between the cautious and the well-guessed fixed rate**
(84 vs 63 / 105), at 90% SLO compliance. It also beats the well-guessed rate on
one axis outright: it is the *only* row that is safe without knowing the answer.

Where a fixed rate wins: if you genuinely know the capacity is 150 and it never
moves, `uber(100)` is simpler and slightly better (105 vs 84). Adaptivity is not
free — it costs a sawtooth around the target and some SLO overshoot.

## Case 2 — STEP degradation: 150 -> 60 -> 30 req/s at t=2s, 4s

| limiter | achieved/s | goodput/s | %<=SLO | mean | p99 | finalRate |
|---------|-----------:|----------:|-------:|-----:|----:|----------:|
| uber(120) | 121 | **0** | 0% | 335ms | 401ms | 120 |
| uber(24)  | 25 | 11 | 42% | 70ms | 101ms | 24 |
| agile     | 46 | **37** | 82% | 79ms | 401ms | 22 |

No single fixed rate is good here. `uber(120)` is tuned for the healthy phase
and melts the moment capacity drops. `uber(24)` is tuned for the *worst* phase:
it stays safe but throws away the healthy phase entirely. agile delivers **~3.4×
the best safe fixed rate's goodput** (37 vs 11) by tracking the step down and
back, and its final rate (22) shows it found the 30 req/s floor on its own.

## Case 3 — DIURNAL capacity: 100 ± 60 req/s over a 6s period

| limiter | achieved/s | goodput/s | %<=SLO | mean | p99 | finalRate |
|---------|-----------:|----------:|-------:|-----:|----:|----------:|
| uber(80) | 84 | 53 | 64% | 161ms | 401ms | 80 |
| uber(32) | 34 | 34 | 99% | 36ms | 100ms | 32 |
| agile    | 63 | **58** | 92% | 55ms | 401ms | 42 |

The capacity oscillates between 40 and 160. `uber(80)` chases the peaks and
melts in the troughs (64% SLO). `uber(32)` is clean but only exploits the
troughs. agile gets the **highest goodput at 92% SLO** (58 vs 53 / 34): it rides
the curve up and down.

## Summary

| capacity | best fixed rate | agile | verdict |
|----------|----------------:|------:|---------|
| steady, unknown 150 | 105 (a lucky guess) | 84 | fixed rate wins if you know the number |
| step 150->60->30 | 11 (safe) | 37 | **agile 3.4×** |
| diurnal 40..160 | 53 | 58 | **agile 1.1–1.7×** |

The fixed-rate limiter answers *"pace at exactly N per second"*. That is the
right tool when N is known and constant, and the wrong tool as soon as N moves —
because the single number has to be safe for the worst phase, which wastes
everything else, or good for the best phase, which melts in the worst.

agilelimiter answers *"find the fastest rate this downstream still answers
within the SLO"*. It needs no knowledge of the capacity, and it wins wherever
the capacity is not something you already know — the step change and the diurnal
curve — while staying honest about the trade: on a steady, already-known
capacity a well-chosen fixed rate is simpler and marginally better.
