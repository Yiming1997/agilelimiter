package agilelimiter

// Step runs one control update. It lets the tests in package agilelimiter_test —
// the ones that need the HTTP adapters, which import this package — drive the
// controller on a virtual clock.
func Step(l *Limiter) { l.ctrl.step() }

// StepConcurrency is Step for a ConcurrencyLimiter.
func StepConcurrency(l *ConcurrencyLimiter) { l.ctrl.step() }
