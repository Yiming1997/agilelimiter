package agilelimiter

import (
	"sync"
	"sync/atomic"
	"time"
)

// Clock is the minimal time source the limiter needs. Injecting it keeps the
// package testable with a virtual (fake) clock.
type Clock interface {
	Now() time.Time
	Sleep(time.Duration)
}

// realClock is the default Clock backed by the standard library.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
func (realClock) Sleep(d time.Duration) { time.Sleep(d) }

// bucket is the simplest possible leaky bucket, used purely as the actuator.
//
// It only guarantees one thing: on average, no more than one grant per
// perRequest interval. There is deliberately no slack/burst buffer here — the
// controller changes the rate often, and a burst buffer would blunt its
// response. Each Take() blocks the caller until its slot arrives.
type bucket struct {
	mu   sync.Mutex
	last time.Time // time of the last grant (virtual; may be in the future)
	// perRequest is updated by the controller at any time, so it is atomic to
	// avoid taking the mutex on the hot path just to read it.
	perRequest atomic.Int64 // nanoseconds per grant
	clock      Clock
}

func newBucket(rate int, per time.Duration, clock Clock) *bucket {
	b := &bucket{clock: clock}
	b.setRate(rate, per)
	return b
}

// setRate updates how fast the bucket grants permission. It is safe to call
// while other goroutines are in take(). Because `last` holds an absolute time
// that is independent of the rate, changing the rate never resets state and
// never produces a burst — only the next interval size changes.
func (b *bucket) setRate(rate int, per time.Duration) {
	if rate < 1 {
		rate = 1
	}
	b.perRequest.Store(int64(per / time.Duration(rate)))
}

// take blocks until the caller is allowed to proceed and returns the granted
// time. The very first call is always granted immediately.
func (b *bucket) take() time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.clock.Now()
	if b.last.IsZero() {
		b.last = now
		return now
	}

	next := b.last.Add(time.Duration(b.perRequest.Load()))
	if now.Before(next) {
		// Too early: sleep the remainder, then advance the schedule.
		b.clock.Sleep(next.Sub(now))
		b.last = next
	} else {
		// Idle for a while: realign to now (no burst accumulation).
		b.last = now
	}
	return b.last
}
