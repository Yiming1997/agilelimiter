package agilelimiter

import (
	"math"
	"sync"
	"time"
)

// sloWindowFraction is how much of the SLO the controller is willing to aim at.
// Aiming at the SLO itself means ordinary jitter pushes half the requests over it.
const sloWindowFraction = 0.8

// tailSigma is how many standard errors above the tolerance the observed ratio
// has to be before the guard fires.
//
// Three is the conventional "this is not noise" bar, and it is deliberately a
// statistical convention rather than a tuned number: the caller supplies the
// policy (what share of traffic may be slow) and the controller supplies the
// evidence bar, derived from the sample count it actually has. That is what
// makes "set the tolerance to 2x the cohort" unnecessary — on a busy service the
// margin is small, and on a quiet one it is large because the measurement is
// weaker.
const tailSigma = 3.0

// controller turns the latency signal into a target rate.
//
// The simplified law is intentionally small:
//
//	baseline <- one asymmetric, rise-capped EWMA of p50
//	p50 above the window  -> back off by a fixed step
//	p50 below the window  -> probe up, doubling until the first backoff
//	p99 above the tail    -> back off, using either a plain ratio or p99
//
// The baseline falls quickly when latency improves and rises only slowly, so a
// sustained overload cannot drag the tolerance up with it.
type controller struct {
	mu sync.Mutex

	cfg     Config
	hist    *histogram
	setRate func(rate int)

	fast  float64 // this tick's p50, nanoseconds (Stats only)
	slow  float64 // slow EWMA baseline of p50, nanoseconds
	rate  float64 // current target rate, requests per second
	ready bool
	// probe is true after an unreachable call and until a normal observed call
	// arrives. It lets the rate fall to ProbeRate without making that low floor
	// the normal operating floor.
	probe bool

	// unreachable counts the calls since the last control step that never
	// reached the downstream. step compares it with the calls that did, and only
	// a tick they dominate counts as an outage; see step.
	unreachable uint64

	// coldStart is true until the controller has seen congestion. While it is
	// true a healthy tick multiplies the rate instead of adding UpStep.
	coldStart bool

	// above/below count consecutive ticks on each side of the band. A tick in
	// the band resets both; an action resets the counter that produced it.
	above, below int

	// over is the smoothed fraction of samples above the tail target. It only
	// exists when TailTolerance > 0.
	over      float64
	overAlpha float64

	// tailThreshold is the effective firing threshold: the caller's tolerance
	// plus tailSigma standard errors, recomputed each tick from the sample
	// count. It is only meaningful when TailTolerance > 0.
	tailThreshold float64

	lastP50, lastP99 time.Duration
	lastSamples      uint64

	slowUpAlpha   float64
	slowDownAlpha float64
}

func newController(cfg Config, hist *histogram, setRate func(rate int)) *controller {
	return &controller{
		cfg:           cfg,
		hist:          hist,
		setRate:       setRate,
		rate:          float64(cfg.InitialRate),
		coldStart:     true,
		slowUpAlpha:   ewmaAlpha(cfg.SlowHalfLife, cfg.Tick),
		slowDownAlpha: ewmaAlpha(cfg.SlowDownHalfLife, cfg.Tick),
		// Two ticks: short enough to react to a blowout, long enough that one
		// slow sample in a quiet tick decays instead of repeating.
		overAlpha: ewmaAlpha(2*cfg.Tick, cfg.Tick),
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

	// A tick is an outage when the calls that never arrived outnumber the ones
	// that did two to one. Anything less is not evidence of one: a refused call
	// in a hundred says the downstream answered the other ninety-nine, and a
	// half-and-half tick is a partial failure the caller's breaker owns — at one
	// half exactly, rounding alone would flip the verdict from tick to tick.
	//
	// Any refusal used to record the synthetic sample, and with fewer than 100
	// samples a tick the nearest-rank p99 *is* the maximum, so that one sample
	// fired the tail guard: 1% refused held 5 req/s against a clean 63. The
	// converse failed in ratio mode, where one sample among many is a negligible
	// share: 80% refused climbed to MaxRate.
	//
	// So an outage tick backs off directly, ungated like the tail guard, and
	// records one synthetic sample — which is what keeps a tick with no real
	// sample at all from being skipped as empty.
	outage := false
	if u := c.unreachable; u > 0 {
		c.unreachable = 0
		if u >= 2*c.hist.total.Load() {
			if s, ok := c.outageValue(); ok {
				c.hist.record(s)
				outage = true
			}
		}
	}

	p50, p99, n, above := c.hist.snapshotAbove(c.snapshotTail())
	c.lastP50, c.lastP99, c.lastSamples = p50, p99, n
	if n == 0 {
		return
	}
	v := float64(p50)

	// First real sample seeds the baseline; no adjustment yet.
	if !c.ready {
		c.fast, c.slow, c.ready = v, v, true
		return
	}

	// One signal, one baseline. The raw median is used directly: the tick
	// counters below reject a single noisy sample, and there is no lagging
	// filter to keep reporting congestion after the stall has ended.
	c.fast = v
	if v < c.slow {
		c.slow += c.slowDownAlpha * (v - c.slow)
	} else {
		delta := c.slowUpAlpha * (v - c.slow)
		if maxRise := c.slow * c.cfg.MaxBaselineRise; delta > maxRise {
			delta = maxRise
		}
		c.slow += delta
	}

	high, low := c.window()
	factor := 1.0

	switch {
	case v >= high:
		c.above++
		c.below = 0
		if c.above >= c.cfg.BackoffTicks {
			factor = 1 - c.cfg.DownStep
			c.above = 0
			c.coldStart = false
		}
	case v <= low:
		c.below++
		c.above = 0
		if c.below >= c.cfg.RecoverTicks {
			if c.rampUp() {
				factor = c.cfg.SlowStartGain
			} else {
				factor = 1 + c.cfg.UpStep
			}
			c.below = 0
		}
	default:
		c.above, c.below = 0, 0
	}

	// One tail rule: a plain exceedance ratio or the p99 quantile.
	//
	// The tolerance is the caller's policy; the margin is the controller's
	// evidence. A per-tick exceedance ratio is a binomial proportion, so its
	// standard error is sqrt(p(1-p)/n) — which is why the same tolerance needs a
	// wider margin on a low-traffic service than on a busy one. The EWMA
	// averages about 1/varianceFactor ticks, so sigma is scaled by
	// sqrt(alpha/(2-alpha)) to describe the smoothed estimate.
	if tol := c.cfg.TailTolerance; tol > 0 {
		c.over += c.overAlpha * (float64(above)/float64(n) - c.over)
		sigma := math.Sqrt(tol*(1-tol)/float64(n)) * math.Sqrt(c.overAlpha/(2-c.overAlpha))
		c.tailThreshold = tol + tailSigma*sigma
	}
	if tail := c.tailTarget(); tail > 0 {
		switch {
		case c.cfg.TailTolerance > 0:
			if c.over > c.tailThreshold && float64(above) > c.cfg.TailTolerance*float64(n) {
				if floor := 1 - c.cfg.DownStep; factor > floor {
					factor = floor
				}
				c.coldStart = false
			}
		case p99 > tail:
			if floor := 1 - c.cfg.DownStep; factor > floor {
				factor = floor
			}
			c.coldStart = false
		}
	}
	if outage {
		if floor := 1 - c.cfg.DownStep; factor > floor {
			factor = floor
		}
		c.coldStart = false
	}

	c.rate = clampRate(c.rate*factor, c.floor(), c.cfg.MaxRate)
	if c.rate >= float64(c.cfg.MaxRate) {
		c.coldStart = false // nothing left to discover above the ceiling
	}
	c.setRate(int(math.Round(c.rate)))
}

// rampUp reports whether this healthy tick should probe multiplicatively rather
// than additively: only before the first observed congestion.
func (c *controller) rampUp() bool {
	return c.coldStart && c.cfg.SlowStartGain > 1
}

// tailTarget is the latency at which the p99 guard fires, or zero when the guard
// is off.
func (c *controller) tailTarget() time.Duration {
	if c.cfg.SLOTarget <= 0 {
		return 0
	}
	return time.Duration(float64(c.cfg.SLOTarget) * c.cfg.TailFactor)
}

// snapshotTail is the threshold the histogram should count observations above,
// or zero to skip the count.
func (c *controller) snapshotTail() time.Duration {
	if c.cfg.TailTolerance <= 0 {
		return 0
	}
	return c.tailTarget()
}

// window returns the tolerated latency band for this tick, as absolute
// nanoseconds.
func (c *controller) window() (high, low float64) {
	ceiling := float64(0)
	if c.cfg.SLOTarget > 0 {
		ceiling = float64(c.cfg.SLOTarget) * sloWindowFraction
	}

	if c.cfg.Anchor == AnchorSLO && ceiling > 0 {
		high = ceiling
	} else {
		high = c.slow * (1 + c.cfg.TargetInflation)
		if ceiling > 0 && high > ceiling {
			// Never aim above the SLO.
			high = ceiling
		}
	}
	return high, high * (1 - c.cfg.Deadband)
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
		Rate:          c.rate,
		Baseline:      time.Duration(c.slow),
		Fast:          time.Duration(c.fast),
		Samples:       c.lastSamples,
		P50:           c.lastP50,
		P99:           c.lastP99,
		TailRatio:     c.over,
		TailThreshold: c.tailThreshold,
		ColdStart:     c.coldStart,
	}
}

// floor is the lower clamp for the current mode. It is called with c.mu held.
func (c *controller) floor() int {
	if c.probe {
		return c.cfg.ProbeRate
	}
	return c.cfg.FloorRate
}

// clearProbe returns the controller to the normal floor after a normal sample.
func (c *controller) clearProbe() {
	c.mu.Lock()
	c.probe = false
	c.mu.Unlock()
}

// outageFactor is how far above the tolerated window a synthetic sample for an
// unreachable downstream is placed.
const outageFactor = 1.5

// noteUnreachable records one call that never reached the downstream. It enters
// probe mode at once, so the floor may fall to ProbeRate; whether the tick is an
// outage is decided in step, once the tick's calls can be counted.
func (c *controller) noteUnreachable() {
	c.mu.Lock()
	c.probe = true
	c.unreachable++
	c.mu.Unlock()
}

// outageSample returns the latency an outage tick records, so that "the
// connection was refused" reads as congestion instead of as a zero-latency
// healthy response.
//
// At most ONE synthetic sample is recorded per outage tick. The sample is a
// made-up number, not a measurement, so its count carries no information about
// how much traffic was refused. Letting it scale with the refusal rate made the
// ratio-mode tail guard read "10% of calls were refused" as "10% of the traffic
// was slow" — above the default 5% tolerance, so the guard fired every tick and
// pinned the rate to the floor. Measured: 5% refusals held 227 req/s, 10%
// collapsed to 9, and the same collapse happened whether the injected value was
// 75ms or 1500ms, which is what identified the frequency rather than the
// magnitude as the cause.
func (c *controller) outageSample() (time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.outageValue()
}

// outageValue is outageSample with c.mu held.
func (c *controller) outageValue() (time.Duration, bool) {
	if slo := float64(c.cfg.SLOTarget); slo > 0 {
		v := slo * outageFactor
		if r := c.slow * outageFactor; r > v {
			v = r
		}
		return time.Duration(v), true
	}
	if !c.ready || c.slow <= 0 {
		return 0, false
	}
	return time.Duration(c.slow * (1 + c.cfg.TargetInflation) * outageFactor), true
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
