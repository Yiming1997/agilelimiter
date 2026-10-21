package agilelimiter

import (
	"testing"
	"time"
)

// The hot paths claim to be allocation-free and lock-free. Both are promises to
// the caller, so both are pinned here rather than left in a comment.

func BenchmarkObserve(b *testing.B) {
	l := New(Config{MinRate: 10, MaxRate: 1_000_000, InitialRate: 1000})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Observe(10 * time.Millisecond)
		}
	})
}

func BenchmarkHistogramRecord(b *testing.B) {
	h := &histogram{}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			h.record(10 * time.Millisecond)
		}
	})
}

func BenchmarkTryTakeRelease(b *testing.B) {
	l := NewConcurrency(ConcurrencyConfig{MinLimit: 1, MaxLimit: 1 << 20, InitialLimit: 1 << 20})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if l.TryTake() {
				l.Release()
			}
		}
	})
}

// BenchmarkTakeNoPacing measures the bucket's own overhead under contention.
//
// Per = 1ns with rate 1e9 makes perRequest truncate to 0, so take() never sleeps
// and the number is the mutex plus the clock. A paced benchmark would mostly
// measure the OS timer: a 1ns request on Windows can cost hundreds of
// microseconds, which says nothing about this code.
func BenchmarkTakeNoPacing(b *testing.B) {
	l := New(Config{MinRate: 1, MaxRate: 1_000_000_000, InitialRate: 1_000_000_000,
		Per: time.Nanosecond})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Take()
		}
	})
}

// BenchmarkControlStep is one tick: drain the 256-bucket histogram and run the
// control law. One sample per step isolates the scan from the recording cost.
func BenchmarkControlStep(b *testing.B) {
	l := New(Config{
		MinRate: 10, MaxRate: 1000, InitialRate: 500,
		SLOTarget: 200 * time.Millisecond,
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.hist.record(10 * time.Millisecond)
		l.ctrl.step()
	}
}

func TestHotPathsAreAllocationFree(t *testing.T) {
	fc := &fakeClock{now: time.Unix(1_000_000, 0)}
	l := New(Config{MinRate: 10, MaxRate: 1_000_000, InitialRate: 1000, Clock: fc})

	if n := testing.AllocsPerRun(200, func() { l.Observe(10 * time.Millisecond) }); n != 0 {
		t.Fatalf("Observe allocates %.2f times per call, want 0", n)
	}
	if n := testing.AllocsPerRun(200, func() { l.Take() }); n != 0 {
		t.Fatalf("Take allocates %.2f times per call, want 0", n)
	}

	cl := NewConcurrency(ConcurrencyConfig{MinLimit: 1, MaxLimit: 1 << 20, InitialLimit: 1 << 20})
	if n := testing.AllocsPerRun(200, func() {
		if cl.TryTake() {
			cl.Release()
		}
	}); n != 0 {
		t.Fatalf("TryTake/Release allocates %.2f times per call, want 0", n)
	}

	if n := testing.AllocsPerRun(200, func() { cl.Unavailable() }); n != 0 {
		t.Fatalf("Unavailable allocates %.2f times per call, want 0", n)
	}
}
