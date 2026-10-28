package agilelimiter

import (
	"math"
	"math/bits"
	"sync/atomic"
	"time"
)

// Histogram geometry.
//
// Each octave (a factor-of-two range) is split into histSub equal-width
// sub-buckets, so a reported quantile is quantised to ~19% instead of a factor
// of two.
//
// Why the extra resolution matters here specifically: the controller compares a
// recent quantile against a baseline quantile, and with one bucket per octave
// that *ratio* is itself quantised to powers of two. The controller then cannot
// see congestion until latency has doubled — while the knee of a service's
// latency curve is exactly where it needs to see the first small increase.
//
// Measured on a service that degrades as base*max(1, inflight/parallel) with
// base=150ms, parallel=150: with octave buckets the reported p50 stayed pinned
// at 134ms for every concurrency from 150 through 268 (a true latency rise of
// 78%), so the controller saw a perfectly healthy signal and kept increasing
// until it hit MaxLimit. The ratio quantisation, not the constant offset, is
// what caused that blindness — a constant multiplier cancels out of a ratio.
const (
	histSubBits = 2                // 2 bits -> 4 sub-buckets per octave
	histSub     = 1 << histSubBits // 4
	histOctaves = 64               // covers every value that fits in int64
	histBuckets = histOctaves * histSub

	// histMaxExp is the largest exponent whose lowest sub-bucket bound
	// (2^exp) still fits comfortably in a time.Duration (int64 nanoseconds).
	histMaxExp = 62
)

// histogram is a lock-free log-bucketed latency histogram. Recording is a single
// atomic add, so it is safe on the hot path and allocation-free. Resolution is
// ~19% (sub-bucketed octaves), which is fine for a control signal, and the
// histogram is reset on every snapshot — it measures "this tick", not all time.
type histogram struct {
	counts [histBuckets]atomic.Uint64
	total  atomic.Uint64
}

// record adds one observation. Safe for concurrent use.
func (h *histogram) record(d time.Duration) {
	var n uint64
	if d > 0 {
		n = uint64(d) // negative durations would wrap to a huge uint64, so guard
	}
	h.counts[histIndex(n)].Add(1)
	h.total.Add(1)
}

// histIndex maps a value to its sub-bucket.
//
// Bucket index e*histSub+s covers [2^e*(1+s/4), 2^e*(1+(s+1)/4)). Values below
// 4ns fall back to whole nanoseconds, where a 25% error is meaningless anyway.
func histIndex(n uint64) int {
	if n == 0 {
		return 0
	}
	e := bits.Len64(n) - 1 // floor(log2(n))
	if e > histMaxExp {
		return histBuckets - 1
	}
	idx := e * histSub
	if shift := e - histSubBits; shift >= 0 {
		idx += int(n>>uint(shift)) & (histSub - 1)
	}
	return idx
}

// subBucketLower returns the lower bound of bucket idx: 2^e * (1 + s/4).
func subBucketLower(idx int) time.Duration {
	e := idx / histSub
	s := idx % histSub
	if e > histMaxExp {
		e, s = histMaxExp, histSub-1
	}
	base := uint64(1) << uint(e)
	return time.Duration(base + (base>>histSubBits)*uint64(s))
}

// snapshot returns the (approximate) p50 and p99 for the observations recorded
// since the previous snapshot, then resets the histogram.
func (h *histogram) snapshot() (p50, p99 time.Duration, n uint64) {
	p50, p99, n, _ = h.snapshotAbove(0)
	return p50, p99, n
}

// snapshotAbove is snapshot plus a count of the observations above tail, when
// tail > 0. That count is what the ratio-based tail guard needs: "how much of
// this tick was slow" is a different question from "how slow was the 99th
// percentile", and only the first one can tell a heterogeneous population (5% of
// requests are a report endpoint) from queueing (everything got slower).
//
// The histogram is reset either way, so this is a read-and-drain, not a peek.
func (h *histogram) snapshotAbove(tail time.Duration) (p50, p99 time.Duration, n, above uint64) {
	var counts [histBuckets]uint64
	for i := range h.counts {
		c := h.counts[i].Swap(0)
		counts[i] = c
		n += c
	}
	h.total.Swap(0)
	if n == 0 {
		return 0, 0, 0, 0
	}
	p50 = quantile(&counts, n, 0.50)
	p99 = quantile(&counts, n, 0.99)
	if tail > 0 {
		above = aboveCount(&counts, uint64(tail))
	}
	return p50, p99, n, above
}

// aboveCount returns how many observations are above v.
//
// Buckets are ordered by their lower bound, so this walks down from the top and
// stops at the first bucket that could contain v. That bucket cannot be split, so
// it is counted as *not* above: the count errs low by at most one bucket per
// tick, the same floor-quantisation the quantiles themselves have.
func aboveCount(counts *[histBuckets]uint64, v uint64) uint64 {
	var above uint64
	for i := histBuckets - 1; i >= 0; i-- {
		if uint64(subBucketLower(i)) <= v {
			break
		}
		above += counts[i]
	}
	return above
}

// quantile returns the lower bound of the bucket containing the q-th value,
// using nearest-rank: rank = ceil(q * total), clamped to [1, total].
//
// The ceiling matters at small counts. With truncation, q=0.99 over two samples
// ranks the *first* (faster) one, so a tick holding one fast and one timed-out
// request reported the timeout as healthy — the tail guard could not fire on the
// exact tick it was written for.
func quantile(counts *[histBuckets]uint64, total uint64, q float64) time.Duration {
	target := uint64(math.Ceil(float64(total) * q))
	if target == 0 {
		target = 1
	}
	if target > total {
		target = total
	}
	var cum uint64
	for i := 0; i < histBuckets; i++ {
		cum += counts[i]
		if cum >= target {
			return subBucketLower(i)
		}
	}
	// Unreachable while counts is consistent with total; fall back to the top bound.
	return subBucketLower(histBuckets - 1)
}
