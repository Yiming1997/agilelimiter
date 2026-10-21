package main

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agilelimiter"
)

// A downstream with a cache in front of it. Two populations share one pipe:
//
//	cache hit   ~1ms, consumes no backend capacity
//	cache miss  base/(1-util) against the backing store, C = 150 req/s
//
// The question is what the controller is told. "observe-all" feeds every request
// (today's default); "miss-only" feeds only the misses, i.e. only the requests
// that actually touch the backend.
type cachePhase struct {
	adm, ok, hits, misses, missOK atomic.Int64
	mu                            sync.Mutex
	missLats                      []time.Duration
}

func (p *cachePhase) add(d time.Duration, hit bool, slo time.Duration) {
	p.adm.Add(1)
	if d <= slo {
		p.ok.Add(1)
	}
	if hit {
		p.hits.Add(1)
		return
	}
	p.misses.Add(1)
	if d <= slo {
		p.missOK.Add(1)
	}
	p.mu.Lock()
	p.missLats = append(p.missLats, d)
	p.mu.Unlock()
}

func (p *cachePhase) missStats() (mean, p99 time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.missLats) == 0 {
		return 0, 0
	}
	l := append([]time.Duration(nil), p.missLats...)
	sort.Slice(l, func(i, j int) bool { return l[i] < l[j] })
	var sum time.Duration
	for _, d := range l {
		sum += d
	}
	return sum / time.Duration(len(l)), l[int(float64(len(l)-1)*0.99)]
}

const (
	cacheWarm1    = 40 * time.Second
	cacheCold     = 15 * time.Second
	cacheWarm2    = 15 * time.Second
	cacheCapacity = 150.0 // backing store, req/s
	cacheBase     = 150 * time.Millisecond
	cacheSLO      = 300 * time.Millisecond
)

var cacheBounds = []time.Duration{cacheWarm1, cacheWarm1 + cacheCold, cacheWarm1 + cacheCold + cacheWarm2}

// hitRatio: the cache is warm, gets invalidated for one phase, then warms again.
func hitRatio(el time.Duration) float64 {
	if el >= cacheWarm1 && el < cacheWarm1+cacheCold {
		return 0.02
	}
	return 0.95
}

func cachePhaseIndex(el time.Duration) int {
	for i, b := range cacheBounds {
		if el < b {
			return i
		}
	}
	return len(cacheBounds) - 1
}

func runCacheArm(observeAll bool) ([]*cachePhase, float64) {
	stats := make([]*cachePhase, len(cacheBounds))
	for i := range stats {
		stats[i] = &cachePhase{}
	}
	l := agilelimiter.New(agilelimiter.Config{
		MinRate:          10,
		MaxRate:          3000,
		InitialRate:      50,
		SLOTarget:        cacheSLO,
		FastHalfLife:     750 * time.Millisecond,
		SlowHalfLife:     10 * time.Second,
		SlowDownHalfLife: time.Second,
		UpStep:           0.05,
		DownStep:         0.30,
		Gain:             1.0,
		Deadband:         0.08,
		Tick:             250 * time.Millisecond,
	})
	l.Start()
	defer l.Stop()

	total := cacheBounds[len(cacheBounds)-1]
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for {
				el := time.Since(start)
				if el >= total {
					return
				}
				st := stats[cachePhaseIndex(el)]

				p := hitRatio(el)
				l.Take()
				hit := r.Float64() < p
				var d time.Duration
				if hit {
					d = time.Millisecond
				} else {
					util := l.Rate() * (1 - p) / cacheCapacity // backend load = misses only
					if util > 0.95 {
						util = 0.95
					}
					d = time.Duration(float64(cacheBase) / (1 - util))
				}
				begin := time.Now()
				time.Sleep(d)
				dd := time.Since(begin)

				if observeAll || !hit {
					l.Observe(dd)
				}
				st.add(dd, hit, cacheSLO)
			}
		}(int64(i))
	}
	wg.Wait()
	return stats, l.Rate()
}

func TestSweepCacheNoise(t *testing.T) {
	if testing.Short() {
		t.Skip("sweep is slow; skipped in -short")
	}
	names := []string{"p1 warm 40s", "p2 invalidated 15s", "p3 warm 15s"}
	secs := []float64{cacheWarm1.Seconds(), cacheCold.Seconds(), cacheWarm2.Seconds()}
	for _, observeAll := range []bool{true, false} {
		stats, final := runCacheArm(observeAll)
		label := "miss-only"
		if observeAll {
			label = "observe-all"
		}
		fmt.Printf("CACHE|%s|final_rate=%.0f\n", label, final)
		for i, st := range stats {
			adm := float64(st.adm.Load())
			if adm == 0 {
				continue
			}
			misses := float64(st.misses.Load())
			overall, missPct := 100*float64(st.ok.Load())/adm, 0.0
			if misses > 0 {
				missPct = 100 * float64(st.missOK.Load()) / misses
			}
			mean, p99 := st.missStats()
			fmt.Printf("CACHE|%s|%s|adm/s=%.0f|hit%%=%.0f|miss/s=%.0f|overall_good=%.1f%%|miss_good=%.1f%%|miss_mean=%dms|miss_p99=%dms\n",
				label, names[i], adm/secs[i], 100*float64(st.hits.Load())/adm, misses/secs[i],
				overall, missPct, mean/time.Millisecond, p99/time.Millisecond)
		}
	}
}
