// Package clock is a minimal stand-in for github.com/benbjohnson/clock,
// providing only what go.uber.org/ratelimit uses at runtime: New(), plus a
// clock with Now() and Sleep(). It exists so the comparison builds without
// network access; it is not a general clock library.
package clock

import "time"

// Clock is a real-time clock.
type Clock struct{}

// New returns a clock backed by the standard library.
func New() *Clock { return &Clock{} }

// Now returns the current wall-clock time.
func (*Clock) Now() time.Time { return time.Now() }

// Sleep pauses the calling goroutine for d.
func (*Clock) Sleep(d time.Duration) { time.Sleep(d) }
