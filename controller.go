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

	// over is the smoothed fraction of samples above the tail target, and
	// overAlpha is its EWMA factor. They only exist in ratio mode.
	over      float64
	overAlpha float64
	// tailThreshold is the effective firing threshold: the caller's tolerance plus
	// tailSigma standard errors, recomputed each tick from the sample count.
	tailThreshold float64

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
		// Two ticks: short enough to react to a sudden blowout, long enough that
		// one slow sample in a quiet tick decays instead of repeating.
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

	p50, p99, n, above := c.hist.snapshotAbove(c.snapshotTail())
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
		// Healthy: probe up. Additively, unless we have never seen congestion — then
		// we do not know where the knee is and doubling finds it in log2 ticks.
		// Overshooting on the way up is cheap (DownStep is 0.30, so two ticks bring
		// us back); undershooting is not (UpStep is 0.05).
		if c.rampUp() {
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

	// Tail guard: if the worst-case latency blows past the tail target, force a
	// backoff step regardless of the median-based decision.
	//
	// Two modes, and the difference matters. With TailTolerance == 0 the test is
	// "is the 99th percentile above the target", which at low sample counts is
	// literally "was the slowest request of this tick above it" and which cannot
	// tell a heterogeneous population from queueing. With a tolerance the test is
	// "what fraction of this tick was above the target, smoothed", so 5% of
	// requests that are simply a different endpoint no longer read as congestion,
	// while a real blowout (most of the tick over the line) still does.
	//
	// The tail guard deliberately skips the BackoffTicks hysteresis: a tail that
	// blows through the target is a safety valve, and delaying it is not worth the
	// noise it would filter.
	if tail := c.tailTarget(); tail > 0 {
		switch {
		case c.cfg.TailTolerance > 0:
			c.over += c.overAlpha * (float64(above)/float64(n) - c.over)

			// The tolerance is the caller's policy; the margin is the controller's
			// evidence. A per-tick exceedance ratio is a binomial proportion, so its
			// standard error is sqrt(p(1-p)/n) — which is why the same tolerance
			// needs a wider margin on a low-traffic service than on a busy one. The
			// EWMA averages about 1/varianceFactor ticks, so sigma is scaled by
			// sqrt(alpha/(2-alpha)) to describe the smoothed estimate.
			tol := c.cfg.TailTolerance
			sigma := math.Sqrt(tol*(1-tol)/float64(n)) * math.Sqrt(c.overAlpha/(2-c.overAlpha))
			c.tailThreshold = tol + tailSigma*sigma

			if c.over > c.tailThreshold {
				excess := (c.over - tol) / tol
				if floor := 1 - math.Min(c.cfg.DownStep, c.cfg.Gain*excess); factor > floor {
					factor = floor
				}
				c.coldStart = false
			}
		case p99 > tail:
			if floor := 1 - c.cfg.DownStep; factor > floor {
				factor = floor
			}
			c.coldStart = false // the tail is congestion evidence too
		}
	}

	c.rate = clampRate(c.rate*factor, c.cfg.MinRate, c.cfg.MaxRate)
	if c.rate >= float64(c.cfg.MaxRate) {
		c.coldStart = false // nothing left to discover up there
	}
	c.setRate(int(math.Round(c.rate)))
}

// rampUp reports whether this healthy tick should probe multiplicatively rather
// than additively: only while the limiter has never seen congestion, because then
// it has no idea where the knee is and doubling finds it in log2 ticks.
//
// It used to also fire on recovery — latency far below the window — and that was
// removed. It bought 25 -> 52 req/s when capacity came back and cost 59 -> 46 when
// capacity dropped, because doubling is also how you overshoot a cliff; a half
// measure with a real downside is worse than a documented "we do not do this".
// See docs/design.md §9.6 for the numbers.
func (c *controller) rampUp() bool {
	return c.coldStart && c.cfg.SlowStartGain > 1
}

// tailTarget is the latency at which the p99 guard fires, or zero when the guard
// is off.
//
// It is SLOTarget scaled by TailFactor because SLOTarget is asked to be three
// things at once: the window cap (0.8x SLOTarget), the AnchorSLO target, and this
// guard's threshold — and the guard wants a larger number than the other two. A
// per-tick p99 crossing the number you also promise as an SLO is normal for a
// service that meets its SLO, and the guard costs -DownStep on every tick it
// fires, so a guard pinned at 1.0x fires on about half the ticks and walks the
// rate to MinRate. Raising SLOTarget to fix that would also widen the window and
// move the anchor; this knob moves only the guard.
//
// The default factor is 1, which is the behaviour every measurement in the docs
// was taken with: it is a knob, not a silent change of the old law.
func (c *controller) tailTarget() time.Duration {
	if c.cfg.SLOTarget <= 0 {
		return 0
	}
	return time.Duration(float64(c.cfg.SLOTarget) * c.cfg.TailFactor)
}

// tailSigma is how many standard errors above the tolerance the observed ratio
// has to be before the guard fires.
//
// Three is the conventional "this is not noise" bar, and it is deliberately a
// statistical convention rather than a tuned number: the caller supplies the
// policy (what share of traffic may be slow) and the controller supplies the
// evidence bar, derived from the sample count it actually has. That is what makes
// "set the tolerance to 2x the cohort" unnecessary — on a busy service the margin
// is small, and on a quiet one it is large because the measurement is weaker.
const tailSigma = 3.0

// snapshotTail is the threshold the histogram should count observations above,
// or zero to skip the count. Only ratio mode needs it: quantile mode reads p99.
func (c *controller) snapshotTail() time.Duration {
	if c.cfg.TailTolerance <= 0 {
		return 0
	}
	return c.tailTarget()
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
