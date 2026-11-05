// Package agilelimiter implements an adaptive, latency-driven rate limiter that
// protects a downstream dependency.
//
// Design in one sentence: a simple leaky bucket acts as the *actuator* (it
// paces outbound requests at whatever rate it is told), while a feedback
// controller decides *what that rate should be*, based on the downstream's
// observed latency. A static ceiling is enforced in two independent places, so
// the controller can never exceed the value you are willing to promise.
//
// Control loop (per tick):
//
//	p50 (per-tick histogram)  ─┐
//	slow baseline (one EWMA)  ─┼─> target window ─> AIMD / hold ─> clamp ─> bucket.SetRate
//	p99 (per-tick histogram)  ─┘   (baseline)       + p99 SLO guard
//
// The tick's median is compared against a window: with an SLOTarget, by default,
// 0.8 x SLOTarget; without one, (1+TargetInflation) x the slow EWMA baseline.
// While the median stays under it the controller probes the rate up; once it
// rises past it the controller backs off by a fixed step. Operating with some
// queuing — rather than backing off at the first sign of it — is what keeps
// utilization reasonable. The tail guard, by default, backs off when more than
// 5% of a tick is above the SLO by more than its statistical margin.
//
// Measurement discipline (important): feed Observe the *downstream* round-trip
// time — measured from the moment the request is actually sent, NOT from the
// moment it was planned. Feeding end-to-end latency that includes the limiter's
// own throttle wait creates a self-reinforcing feedback loop (backing off raises
// the measured latency, which causes more backing off) and collapses the rate.
//
// The planned-send timestamp returned by Take() is still useful — use it to
// build a coordinated-omission-safe end-to-end metric for SLO reporting:
//
//	t0 := lim.Take()                 // planned send time (blocks until granted)
//	start := time.Now()              // actually sent
//	resp := doRequest(ctx)
//	lim.Observe(time.Since(start))   // downstream RTT -> drives the controller
//	_ = time.Since(t0)               // CO-safe latency -> reporting only
package agilelimiter
