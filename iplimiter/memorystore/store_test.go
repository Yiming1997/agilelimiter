package memorystore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	iplimiter "github.com/Yiming1997/agilelimiter/iplimiter"
)

// newStore builds a store and closes it when the test ends.
func newStore(t *testing.T, cfg *Config) iplimiter.Store {
	t.Helper()
	s := New(cfg)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

// entryCount reports how many keys the store currently holds.
func entryCount(s *store) int {
	n := 0
	for i := range s.shards {
		s.shards[i].Lock()
		n += len(s.shards[i].entries)
		s.shards[i].Unlock()
	}
	return n
}

// A key longer than the bound is rejected by every method, and nothing is
// stored for it.
func TestKeyLengthBound(t *testing.T) {
	const maxLen = 32
	ctx := context.Background()
	s := newStore(t, &Config{Tokens: 2, Interval: time.Minute, MaxKeyLen: maxLen}).(*store)

	atLimit := strings.Repeat("k", maxLen)
	overLimit := strings.Repeat("k", maxLen+1)

	if _, _, _, ok, err := s.Take(ctx, atLimit); err != nil || !ok {
		t.Fatalf("Take(at limit) = ok %v, err %v; want ok true, no error", ok, err)
	}

	if _, _, _, _, err := s.Take(ctx, overLimit); !errors.Is(err, iplimiter.ErrKeyTooLong) {
		t.Errorf("Take(over limit) err = %v, want ErrKeyTooLong", err)
	}
	if _, _, err := s.Get(ctx, overLimit); !errors.Is(err, iplimiter.ErrKeyTooLong) {
		t.Errorf("Get(over limit) err = %v, want ErrKeyTooLong", err)
	}
	if err := s.Set(ctx, overLimit, 1, time.Minute); !errors.Is(err, iplimiter.ErrKeyTooLong) {
		t.Errorf("Set(over limit) err = %v, want ErrKeyTooLong", err)
	}
	if err := s.Burst(ctx, overLimit, 1); !errors.Is(err, iplimiter.ErrKeyTooLong) {
		t.Errorf("Burst(over limit) err = %v, want ErrKeyTooLong", err)
	}

	if got := entryCount(s); got != 1 {
		t.Fatalf("stored keys = %d, want 1 (the rejected key must not be stored)", got)
	}
}

// The bound counts bytes, not runes: a key of 100 three-byte runes is 300
// bytes and is over a 256-byte limit even though it is only 100 characters.
func TestKeyLengthBoundCountsBytes(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, &Config{Tokens: 1, Interval: time.Minute, MaxKeyLen: 64})

	key := strings.Repeat("中", 100) // 300 bytes
	if len(key) != 300 {
		t.Fatalf("test key is %d bytes, want 300", len(key))
	}
	if _, _, _, _, err := s.Take(ctx, key); !errors.Is(err, iplimiter.ErrKeyTooLong) {
		t.Errorf("Take(300-byte key) err = %v, want ErrKeyTooLong", err)
	}
}

// A store with no explicit bound still has one.
func TestDefaultMaxKeyLen(t *testing.T) {
	s := New(&Config{}).(*store)
	if s.maxKeyLen != DefaultMaxKeyLen {
		t.Fatalf("default maxKeyLen = %d, want %d", s.maxKeyLen, DefaultMaxKeyLen)
	}
}

// Past the key capacity a new key is refused, and the keys that are already
// there keep working.
func TestKeyCapacityBound(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, &Config{Tokens: 5, Interval: time.Minute, MaxKeys: 2})

	for _, key := range []string{"a", "b"} {
		if _, _, _, ok, err := s.Take(ctx, key); err != nil || !ok {
			t.Fatalf("Take(%q) = ok %v, err %v; want ok true, no error", key, ok, err)
		}
	}
	if _, _, _, _, err := s.Take(ctx, "c"); !errors.Is(err, iplimiter.ErrCapacity) {
		t.Fatalf("Take past capacity err = %v, want ErrCapacity", err)
	}
	if _, _, _, ok, err := s.Take(ctx, "a"); err != nil || !ok {
		t.Fatalf("Take(existing) at capacity = ok %v, err %v; want ok true, no error", ok, err)
	}
}

// Replacing a key does not consume a second slot.
func TestSetReplacingKeyDoesNotConsumeSlot(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, &Config{Tokens: 1, Interval: time.Minute, MaxKeys: 1}).(*store)

	if err := s.Set(ctx, "k", 5, time.Minute); err != nil {
		t.Fatalf("Set = %v, want no error", err)
	}
	if err := s.Set(ctx, "k", 9, time.Minute); err != nil {
		t.Fatalf("Set replacing an existing key = %v, want no error", err)
	}
	if got := s.keyCount.Load(); got != 1 {
		t.Fatalf("keyCount = %d, want 1 (the slot must not leak)", got)
	}
}

// A burst lasts only until the window rolls over: the bonus is not carried
// into the next window.
func TestBurstBonusDoesNotSurviveWindow(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, &Config{Tokens: 2, Interval: 50 * time.Millisecond}).(*store)

	if _, _, _, _, err := s.Take(ctx, "k"); err != nil {
		t.Fatalf("Take = %v, want no error", err)
	}
	if err := s.Burst(ctx, "k", 5); err != nil {
		t.Fatalf("Burst = %v, want no error", err)
	}
	if _, rem, _, _, err := s.Take(ctx, "k"); err != nil || rem != 5 {
		t.Fatalf("after Burst remaining = %d, err = %v; want 5", rem, err)
	}

	time.Sleep(80 * time.Millisecond) // let the window roll over

	if _, rem, _, _, err := s.Take(ctx, "k"); err != nil || rem != 1 {
		t.Fatalf("after rollover remaining = %d, err = %v; want 1 (the bonus is gone)", rem, err)
	}
}

// The window stays on the grid its first request anchored, however late the
// request that rolls it over arrives.
func TestWindowStaysOnGrid(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	w := &window{limit: 3, remaining: 3, interval: 10 * time.Second, reset: base.Add(3 * time.Second)}

	cases := []struct {
		name string
		now  time.Time
		want time.Time
	}{
		{"before the first boundary", base.Add(time.Second), base.Add(3 * time.Second)},
		{"just past the first boundary", base.Add(5 * time.Second), base.Add(13 * time.Second)},
		{"part way through the next window", base.Add(21*time.Second + 300*time.Millisecond), base.Add(23 * time.Second)},
		{"just past the next boundary", base.Add(33*time.Second + 900*time.Millisecond), base.Add(43 * time.Second)},
	}
	for _, tc := range cases {
		w.reset = base.Add(3 * time.Second)
		w.remaining = 0
		w.refresh(tc.now)
		if !w.reset.Equal(tc.want) {
			t.Errorf("%s: refresh(%v) -> reset %v, want %v", tc.name, tc.now, w.reset, tc.want)
		}
	}
}
