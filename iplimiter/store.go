package iplimiter

import (
	"context"
	"errors"
	"time"
)

var (
	ErrStopped  = errors.New("inbound limiter: store closed")
	ErrCapacity = errors.New("inbound limiter: key capacity reached")
	// ErrKeyTooLong reports a key longer than the store accepts. The key is
	// rejected rather than stored, so an unbounded identifier cannot turn
	// into unbounded memory.
	ErrKeyTooLong = errors.New("inbound limiter: key too long")
)

// Store manages per-key quotas. Keys are stored as supplied; callers can derive
// a stable HMAC when raw identifiers are unsuitable for the storage backend.
//
// An implementation may bound the key length. A key that exceeds the bound is
// rejected with ErrKeyTooLong by every method, including the read-only ones, so
// the contract is the same whichever call the caller makes. A caller that
// needs an unbounded identifier should hash it first.
type Store interface {
	// Take consumes one token; exhaustion is ok=false, not an error.
	// reset is an absolute Unix timestamp in nanoseconds.
	Take(ctx context.Context, key string) (tokens, remaining, reset uint64, ok bool, err error)
	// Get reports the current window; absent keys return zero.
	Get(ctx context.Context, key string) (tokens, remaining uint64, err error)
	// Set replaces the quota and restarts the key's window.
	Set(ctx context.Context, key string, tokens uint64, interval time.Duration) error
	// Burst grants bonus tokens until the current window ends.
	Burst(ctx context.Context, key string, tokens uint64) error
	Close(ctx context.Context) error
}
