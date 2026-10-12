package agilelimiter

import (
	"math"
	"sync"
	"time"
)

// controller turns the latency signal into a target rate.
//
// It compares a *fast* EWMA of the recent median latency against a *slow* EWMA
// that plays the role of a healthy baseline, and tolerates the latency rising
// to (1 + TargetInflation) x baseline before backing off. Operating with some
// queuing — rather than backing off at the first sign of it — is what keeps
// utilization reasonable.
//
//	fast ~ baseline                -> healthy, probe up (additive increase)
//	fast well above the tolerance  -> congested, back off (proportional)
//	in between                     -> hold (deadband)
//
// The baseline is updated asymmetrically: it falls quickly when latency improves
// (fast recovery) but rises only slowly, so a sustained overload cannot drag the
// baseline up to the congested value and freeze the controller. This is the
// single most important safeguard — see the README "Pitfalls" section.
type controller struct {
	mu sync.Mutex

	cfg     Config
	hist    *histogram
	setRate func(rate int)

	fast  float64 // fast EWMA of p50, nanoseconds
	slow  float64 // slow EWMA baseline of p50, nanoseconds
	rate  float64 // current target rate, requests per second
	ready bool

	fastAlpha     float64
	slowUpAlpha   float64
	slowDownAlpha float64
}

func newController(cfg Config, hist *histogram, setRate func(rate int)) *controller {
	return &controller{
		cfg:           cfg,
		hist:          hist,
		setRate:       setRate,
		rate:          float64(cfg.InitialRate),
		fastAlpha:     ewmaAlpha(cfg.FastHalfLife, cfg.Tick),
		slowUpAlpha:   ewmaAlpha(cfg.SlowHalfLife, cfg.Tick),
		slowDownAlpha: ewmaAlpha(cfg.SlowDownHalfLife, cfg.Tick),
	}
}

// ewmaAlpha converts a half-life and a step interval into an EWMA smoothing
// factor: alpha = 1 - 2^(-dt/halfLife).
func ewmaAlpha(halfLife, dt time.Duration) float64 {
	if halfLife <= 0 || dt <= 0 {
		return 1
	}
	return 1 - math.Exp(-math.Ln2*float64(dt)/float64(halfLife))
}

// step runs one control update. It is driven by a ticker in production and
// called directly in tests. It is a no-op when no samples arrived this tick.
func (c *controller) step() {
	c.mu.Lock()
	defer c.mu.Unlock()

	p50, p99, n := c.hist.snapshot()
	if n == 0 {
		return
	}
	v := float64(p50)

	// First real sample seeds both filters (no adjustment yet).
	if !c.ready {
		c.fast, c.slow, c.ready = v, v, true
		return
	}

	c.fast += c.fastAlpha * (v - c.fast)
	if v < c.slow {
		c.slow += c.slowDownAlpha * (v - c.slow) // improving: follow down quickly
	} else {
		c.slow += c.slowUpAlpha * (v - c.slow) // degrading: creep up slowly
	}

	// The acceptable latency window: up to (1+TargetInflation) x baseline, with
	// a deadband around it so noise in the hold zone doesn't move the rate.
	high := c.slow * (1 + c.cfg.TargetInflation)
	low := high * (1 - c.cfg.Deadband)

	var factor float64
	switch {
	case c.fast <= low:
		factor = 1 + c.cfg.UpStep // healthy: additive-increase probe
	case c.fast >= high:
		over := (c.fast - high) / high
		factor = 1 - math.Min(c.cfg.DownStep, c.cfg.Gain*over)
	default:
		factor = 1 // within the band: hold
	}

	// Tail guard: if the worst-case latency blows past the SLO, force a full
	// backoff step regardless of the median-based decision.
	if c.cfg.SLOTarget > 0 && p99 > c.cfg.SLOTarget {
		if floor := 1 - c.cfg.DownStep; factor > floor {
			factor = floor
		}
	}

	c.rate = clampRate(c.rate*factor, c.cfg.MinRate, c.cfg.MaxRate)
	c.setRate(int(math.Round(c.rate)))
}

// currentRate returns the last computed rate.
func (c *controller) currentRate() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rate
}

// stats returns a snapshot of the controller's internals for observability.
func (c *controller) stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Rate:     c.rate,
		Baseline: time.Duration(c.slow),
		Fast:     time.Duration(c.fast),
	}
}

func clampRate(r float64, minRate, maxRate int) float64 {
	if r < float64(minRate) {
		return float64(minRate)
	}
	if r > float64(maxRate) {
		return float64(maxRate)
	}
	return r
}
