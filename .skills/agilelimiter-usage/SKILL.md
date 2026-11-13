---
name: agilelimiter-usage
description: Integrate, configure, deploy, or troubleshoot agileLimiter in Go applications. Use for adaptive outbound rate limiting, inbound concurrency limiting, HTTP or manual admission, outcome classification, and traffic budgets across replicas.
---

# Integrate and deploy agileLimiter

Identify the resource to protect. Use the outbound `Limiter` to pace calls to a downstream dependency. Use the inbound `ConcurrencyLimiter` to protect your service's processing capacity. Work with the agileLimiter version selected by the target application and its existing integration.

- When writing or reviewing integration code, read the [integration guide](references/integration.md) for limiter sharing, HTTP and manual admission, request timing, outcome classification, and reusable Go templates.
- When choosing settings, preparing a rollout, checking runtime behavior, or troubleshooting, read the [operations guide](references/operations.md) for effective configuration, budgets across replicas, verification steps, and troubleshooting.

For integration tasks, make code and configuration changes within the requested scope. A rollout plan should specify the effective settings, verification metrics, and rollback criteria. Use the [canary rollout template](assets/rollout-plan.md) when preparing that deliverable. Carry out releases, traffic increases, and rollbacks within the environments and operations the user has authorized.
