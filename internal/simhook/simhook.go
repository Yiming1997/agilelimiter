// Package simhook lets the simulations in agilelimiter/sim drive the control
// loop one tick at a time on a virtual clock. The loop is unexported; an
// internal package is the one place outside it that can reach it without
// adding to the public API.
package simhook

// Step runs one control update on an *agilelimiter.Limiter or an
// *agilelimiter.ConcurrencyLimiter. Package agilelimiter sets it at init.
var Step func(limiter any)

// Use reports what the modelled traffic took from the knob this tick, as Take
// and TryTake would have counted it: calls that asked for a slot on a Limiter,
// the most requests in flight on a ConcurrencyLimiter. The simulations model
// admission themselves and never call Take or TryTake, so without it every
// tick would read as unknown usage.
var Use func(limiter any, used float64)
