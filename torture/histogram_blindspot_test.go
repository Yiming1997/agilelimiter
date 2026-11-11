package torture

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Yiming1997/agilelimiter"
)

// TestHistogramBucketAcrossSLOHidesTailLatency demonstrates numerical
// blindness caused by a histogram bucket that straddles the SLO. The workload
// is a two-mode distribution: 80% of requests take 50ms and 20% take 160ms.
// With a 150ms SLO, the latter are real violations.
//
// Durations from 134.217728ms through 167.772159ms share one histogram bucket
// whose reported lower bound is 134.217728ms.
func TestHistogramBucketAcrossSLOHidesTailLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end load test")
	}

	const (
		rate    = 600.0
		workers = 40
		dur     = 4 * time.Second
		slo     = 150 * time.Millisecond
	)

	cfg := tortureCfg()
	cfg.TailTolerance = 0.05
	var seq atomic.Int64
	srv, client, lim := newService(cfg, workload{
		base:      50 * time.Millisecond,
		tailEvery: 5,
		tail:      160 * time.Millisecond,
	}, &seq)
	defer srv.Close()
	defer lim.Stop()
	lim.Start()

	var (
		stats []agilelimiter.Stats
		stop  = make(chan struct{})
		wg    sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				stats = append(stats, lim.Stats())
			}
		}
	}()

	res := drivePaced(client, srv.URL, lim, rate, workers, dur)
	close(stop)
	wg.Wait()

	var maxP99 time.Duration
	maxTailRatio := 0.0
	for _, s := range stats {
		if s.P99 > maxP99 {
			maxP99 = s.P99
		}
		if s.TailRatio > maxTailRatio {
			maxTailRatio = s.TailRatio
		}
	}

	within := 0
	for _, ms := range res.latencies {
		if ms <= float64(slo/time.Millisecond) {
			within++
		}
	}
	admittedSLOPct := 0.0
	if len(res.latencies) > 0 {
		admittedSLOPct = 100 * float64(within) / float64(len(res.latencies))
	}

	t.Logf("offered=%.0f req/s  distribution=80%%@50ms+20%%@160ms  SLO=%v", rate, slo)
	t.Logf("p99(max)=%v tailRatio(max)=%.3f final=%3.0f shed=%5.1f%% admitted-SLO=%5.1f%%",
		maxP99, maxTailRatio, res.finalLimit, res.shedPct(), admittedSLOPct)

	if res.ok200 < 200 {
		t.Fatalf("only %d successful responses; the harness is broken, not the limiter", res.ok200)
	}
	if admittedSLOPct < 70 || admittedSLOPct > 90 {
		t.Errorf("admitted-SLO = %.1f%%, want 70%%..90%% to prove a material hidden tail",
			admittedSLOPct)
	}
	if maxP99 >= slo {
		t.Errorf("reported p99 = %v, want below %v despite the 160ms tail", maxP99, slo)
	}
	if maxTailRatio > 0.01 {
		t.Errorf("reported tail ratio = %.3f, want at most 0.01 for the hidden cross-SLO bucket",
			maxTailRatio)
	}
	if res.finalLimit < 50 {
		t.Errorf("blind arm ended at limit %.0f; expected it to remain open", res.finalLimit)
	}
	if got := res.shedPct(); got > 20 {
		t.Errorf("blind arm shed %.1f%%; expected less than 20%% while the tail remains invisible", got)
	}
}
