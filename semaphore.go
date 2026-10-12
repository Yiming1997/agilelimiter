package agilelimiter

import "sync/atomic"

// semaphore is the inbound actuator: it caps how many requests are processed
// *concurrently* at a limit the controller can change at any time.
//
// It is the inbound counterpart of bucket. Where bucket paces *how fast*
// requests leave (a leaky bucket at R req/s), semaphore bounds *how many* are in
// flight. Both are a single hot-path atomic update, and both take their target
// from the same controller — only the actuator differs.
//
// Unlike a standard semaphore it never blocks: tryAcquire returns false at the
// limit. That is deliberate. An inbound limiter that blocked would build a queue
// holding the very goroutines, sockets and connections it is meant to protect,
// so the fix (queuing) becomes worse than the disease. Refusing instead lets the
// caller shed load with a fast 429 / ResourceExhausted.
type semaphore struct {
	limit    atomic.Int64 // max concurrent requests, set by the controller
	inflight atomic.Int64 // requests currently being served
}

func newSemaphore(limit int) *semaphore {
	s := &semaphore{}
	s.setLimit(limit)
	return s
}

// setLimit hot-updates the concurrency limit. Clamped to at least 1: a limit of
// zero would refuse every request and stall the service with no way to recover.
func (s *semaphore) setLimit(n int) {
	if n < 1 {
		n = 1
	}
	s.limit.Store(int64(n))
}

// tryAcquire admits one request if there is room, without blocking. It returns
// false immediately when the limit is already reached.
//
// Note the limit can change while a request is in flight; if the controller
// lowers it below the current count, tryAcquire simply refuses until in-flight
// requests drain back under the new limit. That is exactly the desired backoff.
func (s *semaphore) tryAcquire() bool {
	for {
		n := s.inflight.Load()
		if n >= s.limit.Load() {
			return false
		}
		if s.inflight.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

// release frees one slot. Every successful tryAcquire must be paired with
// exactly one release.
func (s *semaphore) release() { s.inflight.Add(-1) }

// cap returns the current limit.
func (s *semaphore) cap() int { return int(s.limit.Load()) }

// inFlight returns the number of requests currently being served.
func (s *semaphore) inFlight() int { return int(s.inflight.Load()) }
