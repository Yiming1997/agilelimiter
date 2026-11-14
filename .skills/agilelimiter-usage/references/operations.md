# Configuration, rollout, and troubleshooting

## Set capacity limits and latency targets

| Setting | Outbound | Inbound | How to choose |
| --- | --- | --- | --- |
| Ceiling | `MaxRate` | `MaxLimit` | Set a positive value. Use the RPS budget allocated to the instance for outbound calls or the number of in-flight requests one instance can safely handle for inbound work. |
| Normal floor | `FloorRate` | `FloorLimit` | Use a level the resource can sustain under healthy conditions. If capacity is unknown, start by setting only the ceiling. |
| Probe floor | `ProbeRate` | `ProbeLimit` | The minimum allowed when the resource is unreachable. Set it to at least 1 and at most the normal floor. |
| Initial setting | `InitialRate` | `InitialLimit` | Defaults to one eighth of the ceiling using integer division, clamped between the normal floor and ceiling. |

`MinRate` and `MinLimit` are compatibility fields that default to 1. An unset normal or probe floor inherits this value. Run `Validate()` before construction and inspect `Config()` afterward. Verify that the effective bounds satisfy `1 ≤ Probe ≤ Floor ≤ Initial ≤ Max`. A compatibility field above the ceiling may pass validation and raise the effective ceiling. In inbound mode, `Resolved.MaxRate` and the other `*Rate` fields returned by `Config()` are concurrency counts.

Match `SLOTarget` to the latency the limiter actually measures. Identify the protected resource and measurement boundaries before setting the threshold. If degraded capacity is below the normal floor, that floor still constrains backoff.

For outbound traffic across replicas, subtract other callers' allocations and divide the remaining budget by the **maximum number of replicas running at once**, including during rolling updates and autoscaling. For example, with a 600 RPS budget for this application and at most six equally allocated replicas, each replica gets at most 100 RPS. Use a gateway or quota service to enforce a strict shared quota across replicas. Set inbound `MaxLimit` according to each instance's processing capacity.

## Choose a preset and tail guard

For a first integration, start with the zero-value `PresetNone` and the default `Tick` of `1s`. Use load tests to decide whether to adjust the interval, backoff step, or tail guard.

| Preset | Default anchor | Ticks for median-based backoff / growth | Tail guard with an SLO |
| --- | --- | --- | --- |
| `PresetNone` | `AnchorSLO` with an SLO, `AnchorBaseline` without one | 2 / 2 with an SLO, 1 / 1 without one | Fraction above threshold, with a default tolerance of 5% |
| `PresetBalanced` | 2× baseline | 1 / 1 | Approximate `p99` |
| `PresetLatencyFirst` | `AnchorSLO`, which should be used with `SLOTarget` | 2 / 2 | Approximate `p99` |
| `PresetThroughputFirst` | 3.5× baseline | 2 / 1 | Approximate `p99` |

With `AnchorSLO`, the upper edge of the median latency window is `0.8 × SLOTarget`. When an SLO is set, the same value caps the baseline-anchored window. The tail guard is disabled when no SLO is set. `TailFactor` defaults to `1` and adjusts only the tail threshold. A positive `TailTolerance` enables the fraction-based guard, which uses a statistical margin that varies with sample count. Zero uses the preset's rule from the table. When the tail guard triggers, it backs off immediately without waiting for the median-based tick count.

Normal backoff is bounded by `Floor`. An unreachable outcome allows the limiter to drop as low as `Probe`, and the controller decides whether to back off based on outage signals. The probe floor is at least 1. To stop all calls, use application-level circuit breaking or pause job dispatch.

## Manage lifecycle and verify a canary rollout

1. Construct the limiter during initialization, log `Config()`, and call `Start()` before accepting traffic or starting workers. At shutdown, stop accepting traffic and dispatching work, drain in-flight work within the application's shutdown deadline, and then call `Stop()`. `Stop()` ends the control loop. Create a new limiter to start a fresh loop after stopping.
2. In a test environment, exercise normal traffic, sustained overload, an outage, and recovery. For outbound calls, measure actual send rate, admission wait, timeouts, and requests per second that meet the SLO. For inbound work, measure in-flight requests, rejection rate, and latency of admitted requests. Check that in-flight requests drain naturally after a limit reduction.
3. During a canary rollout, compare end-to-end latency, errors, and requests per second that meet the SLO with the previous policy's baseline. If a rollback condition is met, stop increasing traffic and preserve comparison data. Restore a known stable admission policy or the previous application version within the authorized scope.

## Monitor and troubleshoot

Sample `Config()`, `Rate()` or `Limit()`, and `InFlight()` for inbound work. Read `Samples`, `Used`, `P50`, `P99`, and `ColdStart` from `Stats()`. Outbound `Used` counts calls entering `Take()` per second, including callers still waiting. Inbound `Used` is peak concurrency during the tick. In fraction-based mode, compare `TailRatio` with `TailThreshold`. An outage tick may include one synthetic sample.

Mount the HTTP `StatsHandler` on an access-controlled management route outside the limiting middleware so concurrency and latency remain visible during overload. Verify the SLO with independent application metrics. The controller reports bucket lower bounds for quantiles, and buckets that straddle the SLO can undercount violations.

| Symptom | What to check |
| --- | --- |
| `Samples` stays at zero | Check `Start()`, the limiter instance serving the requests, sparse traffic, and samples skipped due to caching or cancellation. |
| Samples are present and latency is low, but the limit stays flat | Check the ceiling and compare `Used` with the current setting. When known usage is below half the current setting, a healthy median will not trigger an increase. |
| The limit holds or grows during an outage | Check whether fast failures are recorded as healthy latency. With neither an SLO nor an established baseline, a complete outage cannot produce an outage sample for backoff. |
| Latency remains high at the normal floor | Compare the floor with degraded capacity. Check for admission wait in the feedback and for traffic with different processing costs sharing the same limiter. |
| Outbound requests pile up | Compare actual send rate with admission wait. Check worker concurrency, queue length, and retry volume. |
| The controller's `p99` looks healthy while SLO attainment falls | Check end-to-end measurement boundaries, response body reads, rejection rate, and undercounting by histogram buckets that straddle the threshold. |
| The downstream overloads after scaling out | Check the per-instance ceiling, peak replica count during rollout, and traffic from other callers. |

## Verify the application's selected version

Run these commands from the target application's Go module directory. `go list -m` shows the selected module version and any `replace` directive. `go doc` shows the configuration fields in that build context. To confirm a default, inspect `Config()` on the running limiter or check the source in the `Dir` reported by `go list -m -json`. If the dependency has not been installed or its source is unavailable locally, start with the version or local replacement declared by the application.

```bash
go list -m -json github.com/Yiming1997/agilelimiter
go doc github.com/Yiming1997/agilelimiter Config
go doc github.com/Yiming1997/agilelimiter ConcurrencyConfig
```

For details on the control algorithm or measurement precision, see `../../../docs/design.md` in the agileLimiter source for the corresponding version.
