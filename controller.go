package agilelimiter

import (
	"math"
	"sync"
	"time"
)

// sloWindowFraction is how much of the SLO the controller is willing to aim at.
// Some headroom below the SLO is needed because the fast EWMA lags the true
// latency: aiming exactly at the SLO means overshooting it on every probe.
const sloWindowFraction = 0.8

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

	// coldStart is true until the controller has evidence of congestion, i.e. it
	// knows where the knee is. While it is true a healthy tick multiplies the
	// rate (SlowStartGain) instead of adding UpStep, so an unknown capacity is
	// found in log2 ticks rather than in ln/ln(1+UpStep) ticks. See the
	// SlowStartGain field for why the asymmetry justifies this.
	coldStart bool

	// above/below count how many consecutive ticks the fast signal has spent on
	// each side of the tolerated band, for the BackoffTicks/RecoverTicks
	// hysteresis. A tick inside the band resets both.
	above, below int

	// what the last completed tick saw, for Stats
	lastP50, lastP99 time.Duration
	lastSamples      uint64

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
		coldStart:     true,
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
	// Retain what this tick saw for Stats. With no samples both quantiles are
	// zero and Samples is zero, which is how a caller can tell "the controller
	// held its rate" apart from "the controller was blind".
	c.lastP50, c.lastP99, c.lastSamples = p50, p99, n
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
		// Degrading. A slower alpha alone is not enough: under sustained
		// congestion the baseline still creeps up toward the congested value,
		// which drags `high` up with it and lets the controller declare a
		// congested service healthy — congestion would be widening its own
		// tolerance. Measured: baseline drifted +43% in 10s with a 5s half-life,
		// and with the 45s default it is worse. So the upward drift is hard
		// capped per tick, which keeps the target anchored through an overload
		// while still permitting a genuine, permanent slowdown to be absorbed
		// over minutes.
		delta := c.slowUpAlpha * (v - c.slow)
		if maxRise := c.slow * c.cfg.MaxBaselineRise; delta > maxRise {
			delta = maxRise
		}
		c.slow += delta
	}

	// The acceptable latency band for this tick. Which anchor it is measured
	// against — and therefore how much of the SLO's headroom it leaves — is the
	// one thing Anchor changes.
	high, low := c.window()

	// A step needs BackoffTicks/RecoverTicks consecutive ticks on the same side
	// of the band: one tick of jitter then costs nothing. This matters more than
	// the size of the steps, because the steps are asymmetric (-30% vs +5%), so
	// an alternating signal does not cancel out — it ratchets the rate down.
	var factor float64 = 1
	switch {
	case c.fast <= low:
		c.below++
		c.above = 0
		if c.below < c.cfg.RecoverTicks {
			break // not yet enough agreement to spend a probe
		}
		// Healthy: probe up. Additively, unless a multiplicative probe is
		// warranted — either because we have never seen congestion (we do not know
		// where the knee is, so doubling finds it in log2 ticks), or because the
		// latency has come back to half the tolerated window (we know exactly
		// where the knee is, and we are nowhere near it). Overshooting on the way
		// up is cheap (DownStep is 0.30, so two ticks bring us back);
		// undershooting is not (UpStep is 0.05).
		if c.rampUp(high) {
			factor = c.cfg.SlowStartGain
		} else {
			factor = 1 + c.cfg.UpStep
		}
	case c.fast >= high:
		c.above++
		c.below = 0
		if c.above < c.cfg.BackoffTicks {
			break // not yet enough agreement to spend a step
		}
		over := (c.fast - high) / high
		factor = 1 - math.Min(c.cfg.DownStep, c.cfg.Gain*over)
		c.coldStart = false // we found the knee; ordinary AIMD from here on
	default:
		c.above, c.below = 0, 0 // within the band: hold
	}

	// Tail guard: if the worst-case latency blows past the SLO, force a full
	// backoff step regardless of the median-based decision.
	//
	// Note what this is at low sample counts. quantile uses nearest-rank, so with
	// n samples "p99" is the n-th order statistic below n=100 — i.e. the slowest
	// sample. One slow request in a quiet tick therefore costs a full -DownStep.
	// That is the honest meaning of a 99th percentile over ten samples, not a
	// bug: if you want the guard to need a real tail, give it more samples per
	// tick by raising Tick.
	if c.cfg.SLOTarget > 0 && p99 > c.cfg.SLOTarget {
		if floor := 1 - c.cfg.DownStep; factor > floor {
			factor = floor
		}
		c.coldStart = false // the tail is congestion evidence too
	}

	c.rate = clampRate(c.rate*factor, c.cfg.MinRate, c.cfg.MaxRate)
	if c.rate >= float64(c.cfg.MaxRate) {
		c.coldStart = false // nothing left to discover up there
	}
	c.setRate(int(math.Round(c.rate)))
}

// rampUp reports whether this healthy tick should probe multiplicatively rather
// than additively.
//
// Two cases qualify. Cold start: the limiter has never seen congestion, so it
// has no idea where the knee is. The other is recovery, and it is opt-in: the
// observed latency is at or below RecoverRamp of the tolerated window, which
// means the downstream is not merely healthy but far from the knee — the
// situation after it comes back from an outage, when an additive ramp would
// take ln(MaxRate/MinRate) ticks to undo a backoff that took a handful.
//
// The recovery case is off by default (RecoverRamp == 0) because doubling is
// also how you overshoot, and overshooting a downstream whose latency curve is
// a cliff is expensive. See Config.RecoverRamp for the measured trade-off.
func (c *controller) rampUp(high float64) bool {
	if c.cfg.SlowStartGain <= 1 {
		return false
	}
	return c.coldStart || c.fast <= high*c.cfg.RecoverRamp
}

// window returns the tolerated latency band for this tick, as absolute
// nanoseconds.
//
// Under AnchorBaseline the band moves with the healthy baseline — up to
// (1+TargetInflation) x baseline, with a deadband under it so noise in the hold
// zone does not move the rate — and the SLO caps it from above.
//
// That cap is not symmetric, and the asymmetry is the whole reason AnchorSLO
// exists: capping can only *lower* the window, never raise it. So when the
// baseline is small the SLO contributes nothing at all, and the controller
// treats a latency that is still a quarter of the SLO as congestion merely
// because it is 2.5x the fastest response the downstream ever gave. Targeting a
// latency the SLO will reject is self-defeating by construction, so the cap
// itself is right; what is wrong is letting the ratio decide a backoff when the
// absolute number has plenty of room.
//
// Under AnchorSLO the band is sloWindowFraction x SLOTarget outright, and the
// baseline is not consulted. The deadband still applies, so the hold zone is the
// top 5% below the threshold rather than the whole distance to the SLO.
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
			// Never aim above the SLO: a permissive TargetInflation (the 1.0
			// default tolerates 2x baseline) would otherwise park the controller
			// on the SLO boundary, where ordinary jitter pushes half the requests
			// over it.
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
		Rate:      c.rate,
		Baseline:  time.Duration(c.slow),
		Fast:      time.Duration(c.fast),
		Samples:   c.lastSamples,
		P50:       c.lastP50,
		P99:       c.lastP99,
		ColdStart: c.coldStart,
	}
}

// outageFactor is how far above the tolerated window a synthetic sample for an
// unreachable downstream is placed. It only has to land above `high`, and above
// SLOTarget so that the p99 tail guard fires as well.
const outageFactor = 1.5

// outageSample returns the latency to record when a call never reached the
// downstream, so that "the connection was refused" reads as congestion instead
// of as a zero-latency healthy response.
//
// The second result is false when there is nothing to anchor against — no
// SLOTarget and no baseline yet. Recording a made-up number then would seed the
// baseline with it and teach the controller the wrong healthy latency, so the
// caller must record nothing instead.
func (c *controller) outageSample() (time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

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
