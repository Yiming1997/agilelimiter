package main

// hcache is a self-contained scenario, in the spirit of the h* family in the
// go-agile-pool harness: it builds its own limiter and downstream, drives
// load, and asserts one invariant.
//
// The scenario is the CDN case: the origin is down, but most requests are
// served from the edge cache. A correct admission policy keeps letting cache
// hits through at the edge's capacity, so goodput stays high even though every
// miss fails. A rate limiter that charges every cache hit a slot, however,
// backs off on the misses and then throttles the hits too: goodput collapses
// to roughly MinRate. This scenario measures that collapse and fails with a
// message that names the cause.
//
// It is intentionally self-contained (it borrows no session --limiter or
// --downstream) so the numbers are deterministic and the scenario can run
// alone.

import (
	"context"
	"fmt"
	"time"

	"agilelimiter"
)

type hcachePlugin struct{}

func (hcachePlugin) Name() string { return "hcache" }
func (hcachePlugin) Desc() string {
	return "scenario: origin down, CDN still serves hits; cache hits must not be throttled"
}

func (hcachePlugin) Options() []Option {
	return []Option{
		{Name: "min", Default: "5", Help: "MinRate (the outage probe rate)"},
		{Name: "max", Default: "500", Help: "MaxRate"},
		{Name: "initial", Default: "40", Help: "InitialRate"},
		{Name: "slo", Default: "100ms", Help: "SLOTarget"},
		{Name: "tick", Default: "250ms", Help: "control-loop period"},
		{Name: "hit", Default: "0.95", Help: "cache hit ratio in [0,1]"},
		{Name: "cache-latency", Default: "1ms", Help: "latency of a cache hit"},
		{Name: "base", Default: "20ms", Help: "healthy origin latency (unused while down)"},
		{Name: "capacity", Default: "150", Help: "origin capacity (unused while down)"},
		{Name: "origin-down-latency", Default: "500ms", Help: "round trip of a failing miss"},
		{Name: "workers", Default: "256", Help: "concurrent callers"},
		{Name: "settle", Default: "5s", Help: "warm-up not scored (let the controller reach MinRate)"},
		{Name: "dur", Default: "4s", Help: "measured duration"},
		{Name: "min-good", Default: "0", Help: "required good/s; 0 derives 0.5 x hit x max"},
	}
}

func (hcachePlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	opts := ParseOptions(args)

	min, err := GetInt(opts, "min", 5)
	if err != nil {
		return err
	}
	max, err := GetInt(opts, "max", 500)
	if err != nil {
		return err
	}
	initial, err := GetInt(opts, "initial", 40)
	if err != nil {
		return err
	}
	slo, err := GetDuration(opts, "slo", 100*time.Millisecond)
	if err != nil {
		return err
	}
	tick, err := GetDuration(opts, "tick", 250*time.Millisecond)
	if err != nil {
		return err
	}
	hit, err := GetFloat(opts, "hit", 0.95)
	if err != nil {
		return err
	}
	if hit < 0 || hit > 1 {
		return fmt.Errorf("option %q: must be within [0,1]", "hit")
	}
	cacheLatency, err := GetDuration(opts, "cache-latency", time.Millisecond)
	if err != nil {
		return err
	}
	base, err := GetDuration(opts, "base", 20*time.Millisecond)
	if err != nil {
		return err
	}
	capacity, err := GetFloat(opts, "capacity", 150)
	if err != nil {
		return err
	}
	originDownLatency, err := GetDuration(opts, "origin-down-latency", 500*time.Millisecond)
	if err != nil {
		return err
	}
	workers, err := GetInt(opts, "workers", 256)
	if err != nil {
		return err
	}
	settle, err := GetDuration(opts, "settle", 5*time.Second)
	if err != nil {
		return err
	}
	dur, err := GetDuration(opts, "dur", 4*time.Second)
	if err != nil {
		return err
	}
	minGood, err := GetFloat(opts, "min-good", 0)
	if err != nil {
		return err
	}
	if workers < 1 || dur <= 0 {
		return fmt.Errorf("workers must be >= 1 and dur > 0")
	}
	if minGood <= 0 {
		// The edge alone can answer hit x max; half of that is a generous
		// lower bound and still far above MinRate, so a collapse is caught.
		minGood = 0.5 * hit * float64(max)
	}

	cfg := agilelimiter.Config{
		MinRate: min, MaxRate: max, InitialRate: initial,
		SLOTarget: slo, Tick: tick,
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	l := agilelimiter.New(cfg)
	l.Start()
	defer l.Stop()

	curve, err := curveFn("mm1")
	if err != nil {
		return err
	}
	model := &downstreamModel{
		Base: base, Capacity: capacity,
		Curve: curve, CurveName: "mm1", SLOValue: slo,
		CacheHit: hit, CacheLatency: cacheLatency,
		OriginUp: false, OriginDownLatency: originDownLatency,
	}

	stats := &loadStats{}
	driveLoad(ctx, &rateHandle{l: l}, model, loadOptions{
		Workers: workers, Settle: settle, Duration: dur,
		RefuseSleep: 2 * time.Millisecond,
	}, stats)
	res := stats.snapshot()

	secs := dur.Seconds()
	goodPerSec := float64(res.Good) / secs
	admPerSec := float64(res.Admitted) / secs
	total := res.Hits + res.Misses
	var hitPct float64
	if total > 0 {
		hitPct = 100 * float64(res.Hits) / float64(total)
	}

	fmt.Fprintf(rt.Out,
		"[hcache] hit%%=%.1f origin=down good/s=%.1f admitted/s=%.1f "+
			"final_rate=%.1f min=%d max=%d (cache could answer ~%.0f/s)\n",
		hitPct, goodPerSec, admPerSec, l.Rate(), min, max, hit*float64(max))

	if res.Hits == 0 {
		return report("hcache", rt, false,
			"no cache hits were admitted at all; the scenario did not exercise the edge")
	}
	if goodPerSec < minGood {
		return report("hcache", rt, false,
			"cache hits are being throttled: %.1f good/s < %.1f required while the CDN could "+
				"answer ~%.0f/s. Every hit is charged a rate slot, so backing off on the down "+
				"origin also blocks traffic the edge could serve (TODO.md: cache hits must not "+
				"consume an origin-protecting slot)",
			goodPerSec, minGood, hit*float64(max))
	}
	return report("hcache", rt, true,
		"%.1f good/s while every miss failed (>= %.1f): cache hits are not throttled",
		goodPerSec, minGood)
}

// report prints a PASS/FAIL line and converts a failure into the error the
// host reports with the [plugin] prefix.
func report(name string, rt *Runtime, ok bool, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	if ok {
		fmt.Fprintf(rt.Out, "[%s] PASS %s\n", name, msg)
		return nil
	}
	fmt.Fprintf(rt.Out, "[%s] FAIL %s\n", name, msg)
	return fmt.Errorf("%s", msg)
}
