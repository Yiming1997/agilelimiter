package agilelimiter

import (
	"math/bits"
	"sync/atomic"
	"time"
)

// histBuckets is the number of log2 buckets. Bucket i holds values in the range
// [2^(i-1), 2^i) nanoseconds (bucket 0 catches d <= 0), so 64 buckets cover
// every duration that fits in a time.Duration (int64).
const histBuckets = 64

// histogram is a lock-free log-bucketed latency histogram. Recording is a single
// atomic add, so it is safe on the hot path. Resolution is ~2x, which is plenty
// for a control signal, and the histogram is reset on every snapshot — it
// measures "this tick", not all time.
type histogram struct {
	counts [histBuckets]atomic.Uint64
	total  atomic.Uint64
}

// record adds one observation. Safe for concurrent use.
func (h *histogram) record(d time.Duration) {
	var n uint64
	if d > 0 {
		n = uint64(d) // negative durations would wrap to a huge uint64, so guard.
	}
	idx := bits.Len64(n) // 0 for n == 0, otherwise 1..64
	if idx >= histBuckets {
		idx = histBuckets - 1
	}
	h.counts[idx].Add(1)
	h.total.Add(1)
}

// snapshot returns the (approximate) p50 and p99 for the observations recorded
// since the previous snapshot, then resets the histogram.
func (h *histogram) snapshot() (p50, p99 time.Duration, n uint64) {
	var counts [histBuckets]uint64
	for i := range h.counts {
		c := h.counts[i].Swap(0)
		counts[i] = c
		n += c
	}
	h.total.Swap(0)
	if n == 0 {
		return 0, 0, 0
	}
	return quantile(&counts, n, 0.50), quantile(&counts, n, 0.99), n
}

// quantile returns the lower bound of the bucket containing the q-th value.
// The maximum returned value is 2^62 ns, which fits in a time.Duration.
func quantile(counts *[histBuckets]uint64, total uint64, q float64) time.Duration {
	target := uint64(float64(total) * q)
	if target == 0 {
		target = 1
	}
	var cum uint64
	for i := 0; i < histBuckets; i++ {
		cum += counts[i]
		if cum >= target {
			if i == 0 {
				return time.Nanosecond
			}
			return time.Duration(uint64(1) << (i - 1))
		}
	}
	// Unreachable while counts is consistent with total; fall back to the top bound.
	return time.Duration(uint64(1) << (histBuckets - 2))
}
