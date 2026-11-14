package iplimiter

import (
	"context"
	"errors"
	"time"
)

var ErrStopped = errors.New("inbound limiter: store closed")

// Store manages per-key quotas. Keys are stored as supplied; callers can derive
// a stable HMAC when raw identifiers are unsuitable for the storage backend.
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
