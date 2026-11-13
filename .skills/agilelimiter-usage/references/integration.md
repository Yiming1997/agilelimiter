# Integration guide

## Install and pin a version

agileLimiter is deployed with the Go application. For a new integration, use Go 1.24 or later and run `go get` from the application's module directory to pin the dependency. This example uses `v0.1.0`.

```bash
go get github.com/Yiming1997/agilelimiter@v0.1.0
```

For an existing integration, use the version selected by the application's `../../../go.mod`. When upgrading, verify the effective settings and behavior under load in staging.

## Choose an admission method

Share a limiter across requests that protect the same resource. Separate limiters can serve different dependencies, independent quotas, or workloads with substantially different processing costs. Use one admission path per attempt. When the application retries, admit and report each attempt separately.

| Scenario | Integration | Template |
| --- | --- | --- |
| Outbound HTTP | Wrap the client's existing transport with `httpclient.Wrap` during initialization. Reuse the client and limiter, preserving the application's timeout, redirect, and authentication settings. | [Outbound HTTP](../assets/outbound-http.go.tmpl) |
| Inbound HTTP | Wrap the protected handler with `httpserver.Wrap`. It returns HTTP `429` by default when full. Customize the response with `WithRetryAfter` or `WithReject`. | [Inbound HTTP](../assets/inbound-http.go.tmpl) |
| Synchronous functions | For outbound calls, use `Limiter.Do(fn)` or the package-level `agilelimiter.Do(lim, fn)` if the function returns a value. For inbound work, use `ConcurrencyLimiter.Do(fn)` and detect rejection with `errors.Is(err, agilelimiter.ErrOverloaded)`. | Use the core API directly |
| Manual admission or protocol interceptors | For outbound calls, start timing after `Take()` and report the outcome. For inbound work, defer exactly one `Release()` after a successful `TryTake()` and hold the permit until the operation finishes. For gRPC, call the core API from an application interceptor and consider mapping inbound rejection to `codes.ResourceExhausted`. | [Manual admission](../assets/manual-admission.go.tmpl) |

The HTTP templates return the wrapper and the limiter so the application can manage `Start()` and `Stop()` with its lifecycle. Set the package name to match the target application and wire up the limiter during initialization before starting workers or accepting traffic. See the [operations guide](operations.md) for lifecycle and configuration checks.

## Measure request latency

- **Outbound.** Start timing once the request is admitted. `httpclient.Wrap` measures the underlying `RoundTrip` through its return. Measure subsequent response body reads separately. Start end-to-end timing before admission so it includes the wait. Waiting in `Take()` cannot currently be interrupted by context cancellation or `http.Client.Timeout`.
- **Inbound.** Start timing after acquiring a permit. The HTTP adapter measures `ServeHTTP` through its return. Rejections from the wrapper itself produce no handler latency sample. Consider separate limits for streaming, long polling, and short requests when their resource use warrants it.

## Classify outcomes for feedback

The core `Do` and `ObserveOutcome` methods call a custom `Classify` only for non-nil errors. The default treats `context.Canceled` as a truncated measurement and records actual latency for other errors. The HTTP adapters use their own `WithVerdict` options and handle cancellations and truncation before applying a verdict. Delegate cases your custom verdict does not cover to `DefaultVerdict`. For example, use the [outbound verdict template](../assets/outbound-verdict.go.tmpl) to skip samples for `202 Accepted` when the response only acknowledges queueing and its latency does not reflect the protected work.

| Outcome | Feedback |
| --- | --- |
| `Reached` | Call `Observe(d)` to record actual latency and exit probe mode. Some failures still provide a valid sample if the call reached the dependency. |
| `Unreachable` | Call `Unavailable()` to report the failure and enter probe mode. The controller decides whether to back off on its next tick. |
| `Aborted` | Skip the observation. For a canceled call that already exceeded a configured SLO, use `ObserveTruncated(d)` to retain the elapsed time as a lower bound. |

The HTTP client defaults to `Unreachable` for dial-stage `net.OpError`, HTTP `429`, and all `5xx` responses. The HTTP server uses the same outcome for admitted handlers that return `429`, `502`, `503`, or `504`. It skips other `4xx` responses and records latency for `500`. Detected cache hits usually skip feedback but still consume admission capacity because detection happens after admission. If you can identify cache hits before the request, apply the origin limiter to the path that fetches from the origin.

## Verify locally

Check that a normal call reaches the target, a full inbound limiter rejects as expected, every manual permit is released, and the protected path produces observations. You can also run these examples from the agileLimiter repository root. The HTTP example starts a temporary server and a load client.

```bash
go run ./example
go run ./example/concurrency
go run ./example/httpserver
```
