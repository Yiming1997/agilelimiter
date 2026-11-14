// Package memorystore provides process-local, per-key fixed-window quotas.
// This memorystore package implements in-process, key-based, fixed-window rate limiting:
//it creates rate-limiting buckets on demand for each IP, user, or other identifier,
//recording the maximum limit, remaining limit, window end time, and last access time;
//it checks and deducts the limit when a request arrives,
//automatically restores it after the window expires, and supports querying the limit,
//resetting rules individually, and temporarily increasing the limit.
//It ensures safe concurrent operations and reduces lock contention through 64 shards and their respective locks,
//periodically cleans up expired and long-term idle buckets,
//and stops background tasks and releases storage resources when closed.
package memorystore

import (
	"context"
	"hash/maphash"
	"math"
	"sync"
	"time"

	limiter "github.com/Yiming1997/agilelimiter/inbound_limiter"
)

const shardCount = 64

// Config uses one token per second when Tokens or Interval is not positive.
type Config struct {
	Tokens        uint64
	Interval      time.Duration
	SweepInterval time.Duration // Default: six hours.
	SweepMinTTL   time.Duration // Minimum idle retention; default: twelve hours.
	DisablePurge  bool          // Only suitable for a bounded set of keys.
}

type store struct {
	tokens        uint64
	interval      time.Duration
	sweepInterval time.Duration
	sweepMinTTL   time.Duration
	seed          maphash.Seed
	shards        [shardCount]shard
	stop          chan struct{}
	done          chan struct{}
	closeOnce     sync.Once
}

// Lookup, quota updates and deletion share one shard lock. Bucket pointers
// never escape this lock, so eviction cannot leave a usable old bucket.
type shard struct {
	sync.Mutex
	entries map[string]*window
	closed  bool
}

type window struct {
	limit     uint64
	remaining uint64
	interval  time.Duration
	reset     time.Time
	touched   time.Time
}

var _ limiter.Store = (*store)(nil)

// New allocates a store. Close releases its keys and optional cleanup loop.
func New(cfg *Config) limiter.Store {
	c := Config{Tokens: 1, Interval: time.Second, SweepInterval: 6 * time.Hour, SweepMinTTL: 12 * time.Hour}
	if cfg != nil {
		if cfg.Tokens > 0 {
			c.Tokens = cfg.Tokens
		}
		if cfg.Interval > 0 {
			c.Interval = cfg.Interval
		}
		if cfg.SweepInterval > 0 {
			c.SweepInterval = cfg.SweepInterval
		}
		if cfg.SweepMinTTL > 0 {
			c.SweepMinTTL = cfg.SweepMinTTL
		}
		c.DisablePurge = cfg.DisablePurge
	}
	s := &store{
		tokens:        c.Tokens,
		interval:      c.Interval,
		sweepInterval: c.SweepInterval,
		sweepMinTTL:   c.SweepMinTTL,
		seed:          maphash.MakeSeed(),
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
	if c.DisablePurge {
		close(s.done)
	} else {
		go s.purge()
	}
	return s
}

// shardFor maps each key to a stable shard.
// Each shard has its own lock to reduce contention between unrelated keys.
func (s *store) shardFor(key string) *shard {
	hash := maphash.String(s.seed, key)
	index := hash % shardCount
	return &s.shards[index]
}

// entry Check if the bucket for this IP exists.
func (s *store) entry(p *shard, key string, now time.Time) *window {
	b := p.entries[key]
	if b == nil {
		if p.entries == nil {
			p.entries = make(map[string]*window)
		}
		b = &window{
			limit:     s.tokens,
			remaining: s.tokens,
			interval:  s.interval,
			reset:     now.Add(s.interval)}
		p.entries[key] = b
	}
	b.refresh(now)
	return b
}

// The credit limit is restored when the window expires, and the last access time is recorded.
func (b *window) refresh(now time.Time) {
	if !now.Before(b.reset) {
		// Preserve window boundaries without accumulating unused tokens.
		b.reset = now.Add(b.interval - now.Sub(b.reset)%b.interval)
		b.remaining = b.limit
	}
	b.touched = now
}

// Take Find the rate-limiting bucket based on the key,
// attempt to consume the quota once, and return whether the request is allowed to pass.
func (s *store) Take(_ context.Context, key string) (uint64, uint64, uint64, bool, error) {
	p := s.shardFor(key)
	p.Lock()
	defer p.Unlock()
	if p.closed {
		return 0, 0, 0, false, limiter.ErrStopped
	}
	b := s.entry(p, key, time.Now())
	ok := b.remaining > 0
	if ok {
		b.remaining--
	}
	return b.limit, b.remaining, uint64(b.reset.UnixNano()), ok, nil
}

// Get Query the maximum and current remaining amount of a given key,
// without deducting any amount or creating a new bucket.
func (s *store) Get(_ context.Context, key string) (uint64, uint64, error) {
	p := s.shardFor(key)
	p.Lock()
	defer p.Unlock()
	if p.closed {
		return 0, 0, limiter.ErrStopped
	}
	b := p.entries[key]
	if b == nil {
		return 0, 0, nil
	}
	b.refresh(time.Now())
	return b.limit, b.remaining, nil
}

// Set replaces the quota and restarts its window. Zero tokens deny requests.
// A non-positive interval uses the store's default interval.
func (s *store) Set(_ context.Context, key string, tokens uint64, interval time.Duration) error {
	p := s.shardFor(key)
	p.Lock()
	defer p.Unlock()
	if p.closed {
		return limiter.ErrStopped
	}
	if interval <= 0 {
		interval = s.interval
	}
	// Check if the shards are initialized.
	if p.entries == nil {
		p.entries = make(map[string]*window)
	}
	now := time.Now()
	p.entries[key] = &window{
		limit:     tokens,
		remaining: tokens,
		interval:  interval,
		reset:     now.Add(interval),
		touched:   now}
	return nil
}

// Burst Add a key to the IP temporarily
func (s *store) Burst(_ context.Context, key string, tokens uint64) error {
	p := s.shardFor(key)
	p.Lock()
	defer p.Unlock()
	if p.closed {
		return limiter.ErrStopped
	}
	b := s.entry(p, key, time.Now())
	if tokens > math.MaxUint64-b.remaining {
		b.remaining = math.MaxUint64
	} else {
		b.remaining += tokens
	}
	return nil
}

// Close Stop background cleanup and empty all buckets.
func (s *store) Close(_ context.Context) error {
	s.closeOnce.Do(func() {
		close(s.stop)
		for i := range s.shards {
			p := &s.shards[i]
			p.Lock()
			p.closed = true
			p.entries = nil
			p.Unlock()
		}
		<-s.done
	})
	return nil
}

// Regularly delete expired buckets that have not been used for a long time to
// prevent historical IP records from occupying memory indefinitely.
func (s *store) purge() {
	defer close(s.done)
	ticker := time.NewTicker(s.sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			s.purgeExpired(time.Now())
		}
	}
}

func (s *store) purgeExpired(now time.Time) {
	for i := range s.shards {
		p := &s.shards[i]
		p.Lock()
		for key, b := range p.entries {
			// A custom interval may exceed the idle TTL: keep active windows.
			if !now.Before(b.reset) && now.Sub(b.touched) >= s.sweepMinTTL {
				delete(p.entries, key)
			}
		}
		p.Unlock()
	}
}
