// Package simhook lets the simulations in agilelimiter/sim drive the control
// loop one tick at a time on a virtual clock. The loop is unexported; an
// internal package is the one place outside it that can reach it without
// adding to the public API.
package simhook

// Step runs one control update on an *agilelimiter.Limiter or an
// *agilelimiter.ConcurrencyLimiter. Package agilelimiter sets it at init.
var Step func(limiter any)
