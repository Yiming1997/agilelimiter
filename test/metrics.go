package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

// metrics replaces the old main-level sampler with a lifecycle envelope plugin.
// Phase split:
//
//   - Start: parse options, open the file, launch the sampler goroutine. It
//     must happen before load runs so the whole run is covered — hence args
//     on Start (see Lifecycle).
//   - Run: nothing to do.
//   - End: sleep one interval so the sampler records the drained state, stop
//     the sampler, close the file and print the summary.
//
// The sampler reads the limiter handle and the load counters lazily from Store
// on every tick: both are normally provided before the first tick, but reading
// lazily avoids a hard ordering dependency (e.g. --metrics written before
// --load).
type metricsPlugin struct {
	interval float64 // sampling interval (seconds)
	format   string
	file     string
	waitExit int

	f     *os.File
	stop  chan struct{}
	wg    sync.WaitGroup
	start time.Time
	rt    *Runtime
	store *Store

	prevAdm     int64
	prevGood    int64
	prevHits    int64
	prevRefused int64
}

func (m *metricsPlugin) Name() string { return "metrics" }
func (m *metricsPlugin) Desc() string {
	return "sample limiter/runtime/load metrics periodically, summary on End"
}

func (m *metricsPlugin) Deps() []string { return []string{"limiter"} }

func (m *metricsPlugin) Options() []Option {
	return []Option{
		{Name: "interval", Default: "1", Help: "sampling interval (seconds); omit the segment to disable"},
		{Name: "format", Default: "csv", Help: "output format: csv/json"},
		{Name: "file", Default: "metrics.csv", Help: "output file path"},
		{Name: "wait-exit", Default: "0", Help: "extra observation seconds after the run (idle memory)"},
	}
}

func (m *metricsPlugin) Start(ctx context.Context, rt *Runtime, args []string) error {
	opts := ParseOptions(args)

	interval, err := GetFloat(opts, "interval", 1)
	if err != nil {
		return err
	}
	if interval <= 0 {
		return fmt.Errorf("option %q: must be > 0 (omit --metrics to disable sampling)", "interval")
	}
	format := GetString(opts, "format", "csv")
	if format != "csv" && format != "json" {
		return fmt.Errorf("option %q: unsupported value %q", "format", format)
	}
	file := GetString(opts, "file", "metrics.csv")
	waitExit, err := GetInt(opts, "wait-exit", 0)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create %s: %w", file, err)
	}

	m.interval = interval
	m.format = format
	m.file = file
	m.waitExit = waitExit
	m.f = f
	m.stop = make(chan struct{})
	m.start = time.Now()
	m.rt = rt
	m.store = rt.Store

	m.wg.Add(1)
	go m.sampleLoop()
	return nil
}

func (m *metricsPlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	return nil
}

func (m *metricsPlugin) End(ctx context.Context, rt *Runtime) error {
	defer m.f.Close()
	// Final sample tick: the run has drained and the limiter is still running;
	// sleeping one interval lets the sampler record the end state. With
	// wait-exit set, the sampler keeps running through the observation window
	// so runtime memory changes stay visible.
	if m.waitExit > 0 {
		fmt.Fprintf(rt.Out, "  Sampling for %.3fs...\n", m.interval)
		time.Sleep(time.Duration(m.interval * float64(time.Second)))
		fmt.Fprintf(rt.Out, "  Waiting %ds to observe runtime changes...\n", m.waitExit)
		time.Sleep(time.Duration(m.waitExit) * time.Second)
	} else {
		fmt.Fprintf(rt.Out, "  Waiting %.3fs for final metrics collection...\n", m.interval)
		time.Sleep(time.Duration(m.interval * float64(time.Second)))
	}

	close(m.stop)
	m.wg.Wait()

	elapsed := time.Since(m.start)
	fmt.Fprintf(rt.Out, "  Elapsed:       %s\n", elapsed)
	fmt.Fprintf(rt.Out, "  Metrics:       %s (%s)\n", m.file, m.format)
	fmt.Fprintln(rt.Out, "  Done.")
	return nil
}

// sampleLoop ticks every interval and writes a limiter/runtime/load row. The
// CSV header is written on the first tick.
func (m *metricsPlugin) sampleLoop() {
	defer m.wg.Done()
	tick := time.NewTicker(time.Duration(m.interval * float64(time.Second)))
	defer tick.Stop()

	csvHeader := "run_sec,rate,baseline_ms,fast_ms,samples,p50_ms,p99_ms," +
		"tail_ratio,tail_threshold,cold_start," +
		"adm_s,good_s,hits_s,refused_s,goroutines,heap_alloc_mb\n"
	headerWritten := false

	for {
		select {
		case <-tick.C:
			if m.format == "csv" && !headerWritten {
				fmt.Fprint(m.f, csvHeader)
				headerWritten = true
			}
			m.sample()
		case <-m.stop:
			return
		}
	}
}

// sample reads one row of state. Limiter and load data are optional reads: if
// the handle is not up yet (or --load did not run), zeroes are used.
func (m *metricsPlugin) sample() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	var st StatsView
	if h, ok := Get[limiterHandle](m.store, "limiter", "handle"); ok {
		s := h.Stats()
		st = StatsView{
			Rate: s.Rate, Baseline: s.Baseline, Fast: s.Fast,
			Samples: s.Samples, P50: s.P50, P99: s.P99,
			TailRatio: s.TailRatio, TailThreshold: s.TailThreshold,
			ColdStart: s.ColdStart,
		}
	}

	var adm, good, hits, refused int64
	if s, ok := Get[*loadStats](m.store, "load", "stats"); ok {
		adm = s.admitted.Load()
		good = s.good.Load()
		hits = s.hits.Load()
		refused = s.refused.Load()
	}
	// Deltas turn the cumulative counters into per-second rates for the row.
	secs := m.interval
	admS := float64(adm-m.prevAdm) / secs
	goodS := float64(good-m.prevGood) / secs
	hitsS := float64(hits-m.prevHits) / secs
	refusedS := float64(refused-m.prevRefused) / secs
	m.prevAdm, m.prevGood, m.prevHits, m.prevRefused = adm, good, hits, refused

	runSec := time.Since(m.start).Seconds()
	goroutines := runtime.NumGoroutine()
	heapMB := float64(memStats.Alloc) / 1024 / 1024

	switch m.format {
	case "csv":
		fmt.Fprintf(m.f, "%.3f,%.1f,%d,%d,%d,%d,%d,%.3f,%.3f,%v,%.1f,%.1f,%.1f,%.1f,%d,%.2f\n",
			runSec, st.Rate,
			st.Baseline.Milliseconds(), st.Fast.Milliseconds(), st.Samples,
			st.P50.Milliseconds(), st.P99.Milliseconds(),
			st.TailRatio, st.TailThreshold, st.ColdStart,
			admS, goodS, hitsS, refusedS,
			goroutines, heapMB)
	case "json":
		fmt.Fprintf(m.f, `{"run_sec":%.3f,"rate":%.1f,"baseline_ms":%d,"fast_ms":%d,`+
			`"samples":%d,"p50_ms":%d,"p99_ms":%d,"tail_ratio":%.3f,"tail_threshold":%.3f,`+
			`"cold_start":%v,"adm_s":%.1f,"good_s":%.1f,"hits_s":%.1f,"refused_s":%.1f,`+
			`"goroutines":%d,"heap_alloc_mb":%.2f}`+"\n",
			runSec, st.Rate,
			st.Baseline.Milliseconds(), st.Fast.Milliseconds(), st.Samples,
			st.P50.Milliseconds(), st.P99.Milliseconds(),
			st.TailRatio, st.TailThreshold, st.ColdStart,
			admS, goodS, hitsS, refusedS,
			goroutines, heapMB)
	}
}

// StatsView is a small, copyable view of agilelimiter.Stats. It exists so the
// sampler does not have to import the package just to name the fields it
// prints; the mapping happens once, above.
type StatsView struct {
	Rate          float64
	Baseline      time.Duration
	Fast          time.Duration
	Samples       uint64
	P50           time.Duration
	P99           time.Duration
	TailRatio     float64
	TailThreshold float64
	ColdStart     bool
}
