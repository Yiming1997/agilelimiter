package main

import (
	"fmt"
	"testing"
	"time"

	"agilelimiter"
)

// Inbound benchmarks: the same simulated service, the same client population,
// the same SLO. Only the admission strategy differs.
//
//	service   handling time = base * max(1, inflight/parallel)
//	uber      Take() blocks, so excess demand is queued by the caller
//	agile     TryTake() refuses, so excess demand is shed immediately
//
// Each arm runs three phases; the first is discarded and the other two are
// merged. The first phase is not representative for a mechanical reason: a few
// thousand clients all start at the same instant, and a rate limiter cannot
// serialise them within its own inter-arrival time, so it grants a burst that
// has nothing to do with steady state (uber at 7500/s measured 8344 adm/s
// including it, above the model's own 7500/s ceiling).
//
// One benchmark iteration is one slice of traffic, not one request, so
// per-request ns/op is meaningless here. The numbers worth reporting are
// business ones, emitted as custom metrics:
//
//	go test -run '^$' -bench BenchmarkInbound -benchtime 1x .
//
//	goodput/s        requests completing within the SLO, per second
//	slo%             share of admitted requests that met the SLO
//	adm/s            admitted requests per second
//	refuse/s         requests refused immediately (agile only)
//	peak-inflight    highest concurrency observed
const (
	bmDur      = 2 * time.Second
	bmPhases   = 3
	bmBase     = 20 * time.Millisecond
	bmParallel = 150.0
	bmSLO      = 100 * time.Millisecond
	bmClients  = 2000
)

func bmScenario() inboundScenario {
	return inboundScenario{name: "bench-inbound", base: bmBase,
		par: bmParallel, clients: bmClients, slo: bmSLO}
}

func bmReport(b *testing.B, st *sweepStats, secs float64) {
	adm := float64(st.adm.Load())
	good := float64(st.good.Load())
	pct := 0.0
	if adm > 0 {
		pct = 100 * good / adm
	}
	b.ReportMetric(good/secs, "goodput/s")
	b.ReportMetric(pct, "slo%")
	b.ReportMetric(adm/secs, "adm/s")
	b.ReportMetric(float64(st.refus.Load())/secs, "refuse/s")
	b.ReportMetric(float64(st.peak.Load()), "peak-inflight")
}

// BenchmarkInbound measures the steady state: capacity is constant and the fixed
// rate is a guess about it, right or wrong.
func BenchmarkInbound(b *testing.B) {
	sc := bmScenario()
	ceiling := sc.ceiling()

	arms := []struct {
		label string
		mk    func() inboundLimiter
	}{
		{"agile", agileInArm(sc)},
		{fmt.Sprintf("uber-0.50C-%.0f", 0.50*ceiling), uberInArm(int(0.50 * ceiling))},
		{fmt.Sprintf("uber-0.75C-%.0f", 0.75*ceiling), uberInArm(int(0.75 * ceiling))},
		{fmt.Sprintf("uber-1.00C-%.0f", 1.00*ceiling), uberInArm(int(1.00 * ceiling))},
		{fmt.Sprintf("uber-1.25C-%.0f", 1.25*ceiling), uberInArm(int(1.25 * ceiling))},
	}

	for _, a := range arms {
		b.Run(a.label, func(b *testing.B) {
			var last *sweepStats
			for i := 0; i < b.N; i++ {
				lim := a.mk()
				stats := runInbound(lim, sc, bmDur, bmPhases)
				lim.stop()
				last = mergeStats(stats[1:])
			}
			bmReport(b, last, bmDur.Seconds()*(bmPhases-1))
		})
	}
}

// BenchmarkInboundCapacityDrop loses 60% of the service's concurrency after the
// first phase: no single fixed rate is safe in both states, which is the case an
// adaptive admission policy is supposed to earn its keep on.
func BenchmarkInboundCapacityDrop(b *testing.B) {
	sc := bmScenario()
	sc.drop, sc.dropAt, sc.dropFor = 0.4, bmDur, bmDur // 150 -> 60 for phase 2, then back
	ceiling := sc.ceiling()

	arms := []struct {
		label string
		mk    func() inboundLimiter
	}{
		{"agile", agileInArm(sc)},
		{fmt.Sprintf("uber-tuned-healthy-%.0f", ceiling), uberInArm(int(ceiling))},
		{fmt.Sprintf("uber-tuned-healthy-90pct-%.0f", 0.9*ceiling), uberInArm(int(0.9 * ceiling))},
		{fmt.Sprintf("uber-tuned-degraded-%.0f", 0.4*ceiling), uberInArm(int(0.4 * ceiling))},
	}

	for _, a := range arms {
		b.Run(a.label, func(b *testing.B) {
			var last *sweepStats
			for i := 0; i < b.N; i++ {
				lim := a.mk()
				stats := runInbound(lim, sc, bmDur, bmPhases)
				lim.stop()
				last = mergeStats(stats[1:])
			}
			bmReport(b, last, bmDur.Seconds()*(bmPhases-1))
		})
	}
}

// agileColdArm mirrors agileInArm but with a configurable starting limit, so the
// ramp can be measured instead of hidden by a warm start.
func agileColdArm(sc inboundScenario, initial int) func() inboundLimiter {
	return func() inboundLimiter {
		l := agilelimiter.NewConcurrency(agilelimiter.ConcurrencyConfig{
			MinLimit:        10,
			MaxLimit:        500,
			InitialLimit:    initial,
			SLOTarget:       sc.slo,
			FastHalfLife:    750 * time.Millisecond,
			SlowHalfLife:    10 * time.Second,
			DownStep:        0.30,
			UpStep:          0.05,
			Deadband:        0.08,
			TargetInflation: 2.5,
			Tick:            250 * time.Millisecond,
		})
		l.Start()
		return &agileInbound{l: l}
	}
}

// BenchmarkInboundColdStart measures the first two seconds from a limiter that
// starts low, which is exactly what the README tells you to do. A fixed rate has
// no ramp; an adaptive one has to earn its rate back at 5% per tick.
func BenchmarkInboundColdStart(b *testing.B) {
	sc := bmScenario()
	ceiling := sc.ceiling()

	arms := []struct {
		label string
		mk    func() inboundLimiter
	}{
		{"agile-init10", agileColdArm(sc, 10)},
		{"agile-init100", agileColdArm(sc, 100)},
		{fmt.Sprintf("uber-fixed-%.0f", ceiling), uberInArm(int(ceiling))},
	}

	for _, a := range arms {
		b.Run(a.label, func(b *testing.B) {
			var last *sweepStats
			for i := 0; i < b.N; i++ {
				lim := a.mk()
				stats := runInbound(lim, sc, bmDur, 1) // first phase only, no discard
				lim.stop()
				last = stats[0]
			}
			bmReport(b, last, bmDur.Seconds())
		})
	}
}

// BenchmarkInboundScaleUp doubles the service's capacity for the middle phase.
// A rate tuned for the smaller capacity is safe and wastes half the service; one
// tuned for the larger melts before the capacity arrives. The interesting
// question is how much of the doubling the controller captures inside one phase.
func BenchmarkInboundScaleUp(b *testing.B) {
	sc := bmScenario()
	sc.drop, sc.dropAt, sc.dropFor = 2.0, bmDur, bmDur // 150 -> 300 parallel
	ceiling := sc.ceiling()

	arms := []struct {
		label string
		mk    func() inboundLimiter
	}{
		{"agile", agileInArm(sc)},
		{fmt.Sprintf("uber-tuned-1.00C-%.0f", ceiling), uberInArm(int(ceiling))},
		{fmt.Sprintf("uber-tuned-2.00C-%.0f", 2*ceiling), uberInArm(int(2 * ceiling))},
	}

	for _, a := range arms {
		b.Run(a.label, func(b *testing.B) {
			var last *sweepStats
			for i := 0; i < b.N; i++ {
				lim := a.mk()
				stats := runInbound(lim, sc, bmDur, bmPhases)
				lim.stop()
				last = mergeStats(stats[1:])
			}
			bmReport(b, last, bmDur.Seconds()*(bmPhases-1))
		})
	}
}

// BenchmarkInboundFastDip drops capacity for one second instead of two: a dip
// shorter than the controller's own ramp-up time constant. This is the boundary
// where adapting stops paying, because there is not enough time to re-expand.
func BenchmarkInboundFastDip(b *testing.B) {
	const fast = time.Second
	sc := bmScenario()
	sc.drop, sc.dropAt, sc.dropFor = 0.4, fast, fast
	ceiling := sc.ceiling()

	arms := []struct {
		label string
		mk    func() inboundLimiter
	}{
		{"agile", agileInArm(sc)},
		{fmt.Sprintf("uber-tuned-degraded-%.0f", 0.4*ceiling), uberInArm(int(0.4 * ceiling))},
	}

	for _, a := range arms {
		b.Run(a.label, func(b *testing.B) {
			var last *sweepStats
			for i := 0; i < b.N; i++ {
				lim := a.mk()
				stats := runInbound(lim, sc, fast, 3)
				lim.stop()
				last = mergeStats(stats[1:])
			}
			bmReport(b, last, fast.Seconds()*2)
		})
	}
}