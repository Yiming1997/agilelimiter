# agileLimiter canary rollout plan

## Scope and capacity budget

| Item | This rollout |
| --- | --- |
| Service, environment, application version, and dependency version | [Fill in] |
| Inbound or outbound, protected resource, and routes | [Fill in] |
| Current policy and application latency target | [Fill in] |
| Unit and per-instance Max / Floor / Probe / Initial | [Fill in with RPS or in-flight concurrency] |
| Outbound budget and maximum concurrent replica count | [Fill in the available downstream RPS, peak replica count during rollout, and per-replica budget. Omit for inbound.] |

## Verification and traffic increases

| Scenario | What to measure | Pass criteria and results |
| --- | --- | --- |
| Normal traffic | Effective configuration, samples, actual request volume, and requests per second that meet the SLO | [Fill in] |
| Sustained overload | Outbound admission wait or inbound concurrency and rejection rate | [Fill in] |
| Outage and recovery | Outcome classification, limiter adjustments, and recovery time | [Fill in] |
| Canary versus previous policy | End-to-end latency, error rate, and requests per second that meet the SLO | [Fill in canary scope, observation window, and criteria for increasing traffic] |

## Rollback

- Trigger and minimum duration — [Fill in]
- Configuration, policy, or application version to restore — [Fill in]
- Metrics and observation window for confirming recovery — [Fill in]
