package agilelimiter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Anchor selects what the congestion threshold is measured against.
type Anchor int

const (
	// AnchorUnset is the zero value: let Preset decide, and fall back to
	// AnchorBaseline when there is no preset.
	//
	// It exists because AnchorBaseline has to be a real, choosable value — so the
	// zero value cannot also mean "baseline" without making "the caller did not
	// say" indistinguishable from "the caller asked for the baseline", and then
	// a preset could never switch an unsuspecting config to the SLO anchor.
	AnchorUnset Anchor = iota

	// AnchorBaseline is the relative controller: back off when recent latency
	// rises above (1+TargetInflation) x the *fastest latency this downstream has
	// been seen to produce*. It protects the knee of the latency curve and needs
	// no SLO, but it has no notion of how much SLO headroom is left: a downstream
	// answering in 20ms that degrades to 50ms is 2.5x its own baseline, so the
	// default law backs off to MinRate even though 50ms may be a quarter of the
	// SLO, and it then stays there for minutes (MinRate -> MaxRate at UpStep per
	// tick). See docs/design.md §9.
	AnchorBaseline

	// AnchorSLO is the absolute controller: the backoff threshold is
	// sloWindowFraction x SLOTarget and nothing else, so a latency rise that
	// stays well inside the SLO costs nothing. The healthy baseline is still
	// tracked (Stats, outage anchoring) but it never decides a backoff.
	//
	// The trade-off is the mirror image: it keeps probing up while the SLO has
	// headroom, so it cannot find a knee that sits below the SLO. Use it when the
	// SLO is the contract and the downstream degrades gracefully until it, which
	// is the usual case for a dependency with a published latency target.
	//
	// Requires SLOTarget > 0; with no SLO configured it degrades to
	// AnchorBaseline.
	AnchorSLO
)

// rateWindow is the window a rate is expressed over. Every rate in this package
// means requests per second; it used to be a Config field, which only invited the
// question "can I express this per minute?" — you cannot.
const rateWindow = time.Second

// Config configures a Limiter. Zero fields take sensible defaults (see withDefaults).
//
// The fields are grouped by who should decide them. The first group is the whole
// configuration for most services; the rest are escape hatches, and each banner
// says what it costs to move them.
type Config struct {
	// ── what you must decide ─────────────────────────────────────────────

	// Preset fills the *policy* fields you leave at zero: the anchor, the tick
	// hysteresis, the tolerated multiple. Supply MaxRate — and, for
	// PresetLatencyFirst, SLOTarget — and nothing else to get a tuned limiter.
	//
	// Zero (PresetNone) fills nothing, so a config without a preset behaves
	// exactly as it did before presets existed. Any field you set explicitly
	// wins over the preset; see applyPreset for the exact table.
	Preset Preset

	// MinRate and MaxRate bound the dynamic rate, in requests per second.
	// MaxRate is the *hard ceiling* you are willing to promise — the one number
	// the caller must supply. MinRate is the outage probe rate: the rate the
	// limiter keeps retrying at when the downstream is down, so it must be small
	// enough for the downstream to absorb (times your instance count) and big
	// enough that recovery is noticed quickly. Its default of 1 is fine.
	MinRate int
	MaxRate int
	// InitialRate is the starting rate. When zero it is derived from MaxRate
	// (MaxRate/8, at least MinRate) rather than set to MinRate: the controller
	// comes down fast and goes up slowly, so guessing high costs a couple of
	// ticks while guessing low costs ln(MaxRate/MinRate) of them.
	InitialRate int

	// SLOTarget enables the tail-latency guard: when the per-tick p99 exceeds
	// it, the controller forces a full backoff step. Zero disables the guard.
	//
	// It plays two other roles: it caps the relative window from above (the
	// controller never aims past sloWindowFraction x SLOTarget), and under
	// AnchorSLO it *is* the window. The guard itself stays ungated by
	// BackoffTicks: a tail that blows through the SLO is a safety valve, and
	// delaying it one tick is not worth the noise it would filter.
	SLOTarget time.Duration

	// ---- the tail guard: optional, and worth one look ----

	// TailFactor scales SLOTarget for the tail guard only: the guard fires when
	// the per-tick p99 exceeds SLOTarget x TailFactor. The window cap
	// (0.8 x SLOTarget) and the AnchorSLO target are unaffected.
	//
	// 1.0 (the default) is the historical behaviour: the guard fires the moment
	// the p99 crosses the SLO. That is too strict for most services. A per-tick
	// p99 crossing a number you also promise as an SLO is *normal* when you meet
	// that SLO, and the guard takes -30% on every tick it fires, so firing on
	// half the ticks is enough to walk the rate to MinRate. Measured on a
	// downstream with no queueing at all: 5% of requests that always cost 300ms
	// took the rate from 300 to 21 req/s.
	//
	// 1.5-2.0 is the recommended range when the tail is made of different request
	// types (a report endpoint, a large payload, a cold shard) rather than of
	// queueing. It is a separate knob from SLOTarget because raising that would
	// also widen the window cap and move the SLO anchor.
	TailFactor float64

	// TailTolerance switches the tail guard from a quantile test to a ratio test:
	// it fires when the smoothed fraction of requests above the tail target
	// exceeds this number. Zero (the default) keeps the historical test, "is the
	// per-tick p99 above the target".
	//
	// The ratio test exists because a quantile cannot tell a heterogeneous
	// population from queueing: 5% of requests that are simply a different
	// endpoint put the p99 permanently above the target, and the guard then takes
	// -30% per tick until the rate is at MinRate — measured on a downstream with no
	// queueing at all, 300 -> 21 req/s. A ratio says "5% was slow", which against
	// a 10% tolerance is not congestion; when the whole tick is slow the ratio is
	// 100% and the guard fires as before.
	//
	// 0.05-0.10 is the useful range: it is the share of traffic you are willing to
	// see above the target. The ratio is smoothed over about two ticks, so one slow
	// request in a quiet tick decays instead of punishing the rate.
	TailTolerance float64

	// ---- policy: a Preset sets these; override only if you disagree ----

	// Anchor chooses what the backoff threshold is measured against: the
	// downstream's own fastest latency (the default) or the absolute SLO. See
	// Anchor for what each one buys and costs.
	Anchor Anchor

	// BackoffTicks and RecoverTicks are the time hysteresis: the rate only moves
	// after that many *consecutive* ticks agree about which side of the window
	// the latency is on. This is CoDel's interval idea reduced to a counter, and
	// it is the cheapest defence against a noisy signal: with the asymmetric
	// steps (-DownStep vs +UpStep) a single-tick spike and a single-tick dip do
	// not cancel out — they ratchet the rate down — so filtering them by
	// agreement matters more than the size of the steps.
	//
	// A tick in the deadband resets both counters: the two sides must agree in a
	// row, not merely add up.
	//
	// Zero takes the default: 1 (no hysteresis, the historical behaviour) under
	// AnchorBaseline, and 2 under AnchorSLO, where the threshold is fixed and
	// jitter across it is therefore the main false-positive risk.
	BackoffTicks int
	RecoverTicks int

	// ---- the scale of the control law: moving these re-tunes every gain ----

	// EWMA half-lives for the control signal.
	FastHalfLife     time.Duration // recent latency
	SlowHalfLife     time.Duration // healthy baseline (creeps up slowly)
	SlowDownHalfLife time.Duration // baseline follows improvements down

	// MaxBaselineRise caps how fast the healthy baseline may creep upward, as a
	// fraction per tick. It is the hard limit on "baseline pollution": without
	// it a sustained overload drags the baseline up toward the congested
	// latency, the target window rises with it, and the controller freezes in
	// the overloaded state — congestion widens its own tolerance. Keep it small
	// (well under 1% per tick); a genuinely permanent slowdown is then absorbed
	// over minutes rather than seconds. Zero takes the default.
	MaxBaselineRise float64

	// Tick is the control-loop period.
	Tick time.Duration

	// TargetInflation is how far above the healthy baseline latency the
	// controller lets the fast signal rise before it backs off. 1.0 means "accept
	// up to 2x the baseline". Setting it too low makes the limiter back off at
	// the first sign of queuing and run at very low utilization.
	TargetInflation float64
	// Gain scales the proportional backoff: factor = 1 - Gain*(excess/high),
	// capped at DownStep. E.g. 1.0.
	Gain float64
	// UpStep / DownStep cap the fractional change per tick (asymmetry: down
	// faster than up). E.g. 0.05 up, 0.30 down.
	UpStep   float64
	DownStep float64

	// SlowStartGain is the multiplicative increase used while the controller has
	// never yet had to back off — i.e. while it does not know where the knee is.
	// The default 2.0 doubles each tick, so reaching MaxRate costs
	// log2(MaxRate/InitialRate) ticks instead of ln(MaxRate/MinRate)/ln(1+UpStep).
	// With 10 -> 500 and a 1s tick that is 6 ticks instead of 80. This is TCP's
	// slow start for the same reason: the asymmetry that makes additive increase
	// safe — backing off is 6x faster than probing up — is also what makes
	// starting slow unnecessarily expensive.
	//
	// Overshoot costs one or two ticks (DownStep is 0.30), so the risk is
	// bounded; set 1 to get pure AIMD from the first tick.
	SlowStartGain float64
	// Deadband is the hold band around the target latency, as a fraction of it.
	Deadband float64

	// ---- correctness, not tuning ----

	// Classify maps a failed call to what the controller is told about it. It is
	// only consulted for non-nil errors; a nil error always means Reached.
	//
	// The default (nil) treats every error as Reached, which is right for a
	// downstream that answered you — a 5xx or a timeout is a real round trip and
	// a real congestion signal. It is wrong for a downstream that never answered:
	// a refused connection returns in a few microseconds, the controller reads
	// that as "the downstream is fast and healthy", and it raises the rate into a
	// dependency that is already down. Return Unreachable for those. Return
	// Aborted for caller-side cancellations, which say nothing about the
	// downstream at all.
	Classify func(err error) Outcome

	// ---- test seam ----

	// Clock defaults to the real clock. Inject a fake one in tests.
	Clock Clock
}

// Outcome says what the controller should be told about a call's result. It
// exists because "did this call fail?" is not the question that matters — the
// question is "did it reach the downstream?".
type Outcome int

const (
	// Reached means the call reached the downstream and came back, whether it
	// came back fast (2xx), slow (timeout) or unhappy (5xx). Its measured round
	// trip is a genuine congestion signal and is recorded as usual.
	Reached Outcome = iota

	// Unreachable means the call never reached the downstream: connection
	// refused, DNS failure, TLS handshake rejected, an already-open circuit
	// breaker. Its round trip is ~0 and says nothing about the downstream's
	// health, so it is replaced by a synthetic "congested" sample that makes the
	// controller back off toward MinRate — the rate it keeps probing with.
	Unreachable

	// Aborted means the caller gave up, or the call never started. It is not a
	// downstream signal, so the controller is told nothing at all.
	Aborted
)

// Preset bundles the knobs that are *policy* rather than fact, so a caller only
// has to supply the two things only they can know: how much they are willing to
// promise (MaxRate) and how slow is still acceptable (SLOTarget).
//
// A preset fills only the fields the caller left at their zero value, so any
// explicit setting still wins. PresetNone (the zero value) fills nothing, which
// is why adding this field changed no existing behaviour.
type Preset int

const (
	// PresetNone is the zero value: fill nothing, use the library defaults.
	PresetNone Preset = iota

	// PresetBalanced is the default law spelled out: the relative anchor at 2x
	// baseline, single-tick decisions, no fast recovery, every gain at its
	// default. It is equivalent to PresetNone by construction; it exists so a
	// config can say what it means, and so a service can pin its behaviour
	// against a future change of defaults.
	PresetBalanced

	// PresetLatencyFirst trades utilization for the SLO: the threshold becomes
	// the SLO itself (0.8 x SLOTarget) instead of a multiple of the baseline, and
	// a step needs two consecutive ticks. Requires SLOTarget. Use it when the
	// SLO is a contract and the downstream degrades gracefully until it — see
	// AnchorSLO for the trade-off.
	PresetLatencyFirst

	// PresetThroughputFirst tolerates more queueing (3.5x baseline) and also
	// needs two consecutive ticks, which is the shape that came out of the
	// scenario matrix best on a downstream with a knee. Set MaxRate to the knee:
	// this preset will happily run at the ceiling.
	PresetThroughputFirst
)

func (p Preset) String() string {
	switch p {
	case PresetBalanced:
		return "balanced"
	case PresetLatencyFirst:
		return "latency-first"
	case PresetThroughputFirst:
		return "throughput-first"
	default:
		return "none"
	}
}

// applyPreset fills the policy fields the caller left at zero.
//
// The table is deliberately small: a preset that differs in more than a handful
// of fields is not a preset, it is a different controller, and nobody can reason
// about the difference. Note that no preset touches Tick or the half-lives —
// those are the scale of the control law, not a policy choice, and moving them
// silently re-tunes every gain.
func applyPreset(c Config) Config {
	switch c.Preset {
	case PresetLatencyFirst:
		if c.Anchor == AnchorUnset {
			c.Anchor = AnchorSLO
		}
		if c.BackoffTicks <= 0 {
			c.BackoffTicks = 2
		}
		if c.RecoverTicks <= 0 {
			c.RecoverTicks = 2
		}
	case PresetThroughputFirst:
		if c.Anchor == AnchorUnset {
			c.Anchor = AnchorBaseline
		}
		if c.TargetInflation <= 0 {
			c.TargetInflation = 2.5
		}
		if c.BackoffTicks <= 0 {
			c.BackoffTicks = 2
		}
		if c.RecoverTicks <= 0 {
			c.RecoverTicks = 1
		}
	case PresetBalanced, PresetNone:
		// Nothing to do: the defaults below *are* the balanced law.
	}
	return c
}

func withDefaults(c Config) Config {
	c = applyPreset(c)
	if c.Anchor == AnchorUnset {
		c.Anchor = AnchorBaseline
	}
	if c.MinRate < 1 {
		c.MinRate = 1
	}
	if c.MaxRate < c.MinRate {
		c.MaxRate = c.MinRate
	}
	if c.InitialRate <= 0 {
		// Start at a fraction of the ceiling, not at the floor. Coming down from
		// too high costs about a seventh of what coming up from too low does, and
		// slow start finds the knee in a handful of ticks either way.
		c.InitialRate = c.MaxRate / 8
	}
	if c.InitialRate < c.MinRate {
		c.InitialRate = c.MinRate
	}
	if c.InitialRate > c.MaxRate {
		c.InitialRate = c.MaxRate
	}
	if c.FastHalfLife <= 0 {
		c.FastHalfLife = 3 * time.Second
	}
	if c.SlowHalfLife <= 0 {
		c.SlowHalfLife = 45 * time.Second
	}
	if c.SlowDownHalfLife <= 0 {
		c.SlowDownHalfLife = c.SlowHalfLife / 9
	}
	if c.MaxBaselineRise <= 0 {
		// 0.2% per tick: at the default 1s tick, ~12% per minute. Slow enough
		// that a transient overload cannot move the target, fast enough that a
		// genuine permanent slowdown is absorbed within minutes.
		c.MaxBaselineRise = 0.002
	}
	if c.Tick <= 0 {
		c.Tick = time.Second
	}
	if c.TargetInflation <= 0 {
		c.TargetInflation = 1.0
	}
	if c.Gain <= 0 {
		c.Gain = 1.0
	}
	if c.UpStep <= 0 {
		c.UpStep = 0.05
	}
	if c.DownStep <= 0 {
		c.DownStep = 0.30
	}
	if c.SlowStartGain <= 0 {
		c.SlowStartGain = 2.0
	}
	if c.Deadband <= 0 {
		c.Deadband = 0.05
	}
	// A caller (or a preset) can ask for the SLO anchor without supplying an SLO.
	// The config keeps the request — Config() then *reports* the fallback instead
	// of hiding it, and Validate() rejects it — but the tick defaults follow the
	// law that will actually run, which is the relative one.
	sloAnchor := c.Anchor == AnchorSLO && c.SLOTarget > 0
	if c.BackoffTicks <= 0 {
		c.BackoffTicks = 1
		if sloAnchor {
			c.BackoffTicks = 2
		}
	}
	if c.RecoverTicks <= 0 {
		c.RecoverTicks = 1
		if sloAnchor {
			c.RecoverTicks = 2
		}
	}
	if c.TailFactor <= 0 {
		c.TailFactor = 1 // the guard fires at the SLO, which is the historical behaviour
	}
	if c.TailTolerance < 0 {
		c.TailTolerance = 0 // a negative tolerance is meaningless; zero means "quantile mode"
	}
	if c.Clock == nil {
		c.Clock = realClock{}
	}
	return c
}

// Limiter is an adaptive, downstream-protecting rate limiter. Call Take before
// each outbound request and Observe the downstream round-trip time afterwards.
type Limiter struct {
	cfg    Config
	bucket *bucket
	hist   *histogram
	ctrl   *controller

	startOnce sync.Once
	stopOnce  sync.Once
	stop      chan struct{}
	wg        sync.WaitGroup
}

// New builds a Limiter. It does not start the control loop; call Start (or drive
// ctrl.step yourself, as tests do).
func New(cfg Config) *Limiter {
	cfg = withDefaults(cfg)
	b := newBucket(cfg.InitialRate, rateWindow, cfg.Clock)
	h := &histogram{}
	l := &Limiter{
		cfg:    cfg,
		bucket: b,
		hist:   h,
		stop:   make(chan struct{}),
	}
	l.ctrl = newController(cfg, h, func(rate int) { b.setRate(rate, rateWindow) })
	return l
}

// Take blocks until this caller is allowed to send, and returns the planned
// send time. Use it to build a coordinated-omission-safe latency metric.
func (l *Limiter) Take() time.Time { return l.bucket.take() }

// Observe records one downstream round-trip time. Feed it the latency measured
// from the moment the request was actually sent, NOT including this limiter's
// own throttle wait. Safe for concurrent use and allocation-free.
func (l *Limiter) Observe(rtt time.Duration) { l.hist.record(rtt) }

// Do runs fn as one rate-limited downstream call: it takes a slot from the
// bucket, times fn, and feeds the outcome back to the controller. Callers never
// touch Take/Observe directly, so they cannot accidentally feed the wrong
// latency — the clock starts the moment the slot is granted, which is exactly
// the downstream RTT the controller wants.
//
// fn must be the downstream call itself, performed synchronously and without
// internal retries: the observed value is one attempt's round trip. If fn sends
// asynchronously and returns before the response arrives, Do cannot measure the
// RTT — use Take/Observe by hand in that case.
//
// How a failure is reported to the controller is decided by Config.Classify. By
// default an error is still observed as an ordinary round trip, which is right
// for a downstream that answered (a timeout, a 5xx); classify the "never
// reached" errors as Unreachable so a refused connection cannot masquerade as a
// fast healthy response.
func (l *Limiter) Do(fn func() error) error {
	l.Take()
	start := l.cfg.Clock.Now()
	err := fn()
	l.observeOutcome(l.cfg.Clock.Now().Sub(start), err)
	return err
}

// Do is the generic form of Limiter.Do, for calls that also return a value:
//
//	resp, err := agilelimiter.Do(lim, func() (*http.Response, error) {
//		return client.Do(req)
//	})
func Do[T any](l *Limiter, fn func() (T, error)) (T, error) {
	l.Take()
	start := l.cfg.Clock.Now()
	v, err := fn()
	l.observeOutcome(l.cfg.Clock.Now().Sub(start), err)
	return v, err
}

// defaultOutcome is what the controller is told when Config.Classify is nil.
//
// A canceled call is the one failure the library can classify on its own: the
// caller gave up, the downstream never answered, and the truncated round trip
// would otherwise be recorded as a suspiciously fast success. DeadlineExceeded
// is deliberately NOT included — that one means the downstream was too slow,
// which is a real signal and must be observed.
func defaultOutcome(err error) Outcome {
	if err != nil && errors.Is(err, context.Canceled) {
		return Aborted
	}
	return Reached
}

// observeOutcome tells the controller what a call's result says about the
// downstream. See Outcome for why the error alone is not the question.
func (l *Limiter) observeOutcome(d time.Duration, err error) {
	out := defaultOutcome(err)
	if err != nil && l.cfg.Classify != nil {
		out = l.cfg.Classify(err)
	}
	switch {
	case out == Unreachable:
		l.Unavailable()
	case out == Aborted:
		// Nothing: the downstream never expressed an opinion.
	default:
		l.hist.record(d)
	}
}

// Unavailable records one call that never reached the downstream — a refused
// connection, a DNS failure, an already-open circuit breaker. Record it instead
// of Observe for that attempt: its round trip is ~0 and would read as a healthy
// response, so the controller would raise the rate into a dependency that is
// already down. This feeds a synthetic "congested" sample instead, which backs
// the rate off toward MinRate; that floor is what keeps probing, so recovery is
// still detected.
//
// It is a no-op before the controller has anything to anchor a synthetic value
// against: with no SLOTarget and no samples yet, there is no honest number to
// record, and inventing one would corrupt the baseline.
//
// Use this from the manual Take/Observe path, or return Unreachable from
// Config.Classify to have Do call it for you. Never do both for the same
// attempt: that records two samples for one call.
func (l *Limiter) Unavailable() {
	if s, ok := l.ctrl.outageSample(); ok {
		l.hist.record(s)
	}
}

// ObserveOutcome records how one call ended, applying Config.Classify to the
// error. Use it from an integration that measures and classifies calls itself —
// a http.RoundTripper, a gRPC interceptor — so the error policy stays in one
// place instead of being reimplemented at every seam. It is exactly what Do does
// with the same two values.
func (l *Limiter) ObserveOutcome(d time.Duration, err error) { l.observeOutcome(d, err) }

// Rate returns the current target rate in requests per second.
func (l *Limiter) Rate() float64 { return l.ctrl.currentRate() }

// Stats is a point-in-time snapshot of the controller's internals, for
// observability. Track these over time; an adaptive limiter you cannot see into
// is one you cannot debug.
type Stats struct {
	Rate     float64       // current target rate, requests per second
	Baseline time.Duration // slow EWMA playing the role of the healthy baseline
	Fast     time.Duration // fast EWMA of the recent median latency

	// What the last completed tick observed. Samples is the number of
	// observations that tick saw; P50 and P99 are that tick's quantiles, and are
	// zero when Samples is zero.
	//
	// Samples is the signal that the controller is *blind*, not stable: with no
	// samples a step changes nothing, so a frozen rate looks exactly like a
	// steady one. It goes to zero under real conditions — low traffic, or a
	// pipeline that deliberately stops observing (cache hits, for instance). Alert
	// on Samples == 0 for more than a few ticks.
	Samples uint64
	P50     time.Duration
	P99     time.Duration

	// TailRatio is the smoothed fraction of this tick's requests that were above
	// the tail target. It is the number the ratio-mode guard compares against
	// TailTolerance, and it stays zero unless TailTolerance is set.
	//
	// Export it: a non-zero TailRatio with a rate that is not moving tells you the
	// guard is at its threshold and the population, not the load, is deciding.
	TailRatio float64
	// TailThreshold is what TailRatio is compared against: TailTolerance plus
	// tailSigma standard errors, recomputed every tick from the sample count. It
	// is zero in quantile mode.
	//
	// Export it because it turns "why did the guard not fire?" into arithmetic: a
	// low-traffic service has a small n, therefore a wide margin, therefore a
	// threshold far above its tolerance.
	TailThreshold float64

	// ColdStart is true until the controller has had to back off once (or has
	// saturated at MaxRate). While it is true the controller doubles each healthy
	// tick instead of adding UpStep: it is still looking for the knee. The flag
	// never resets for the life of the limiter, so a recovery from an outage is
	// an ordinary additive ramp, not a second slow start.
	ColdStart bool
}

// Stats returns a snapshot of the controller's internal signals.
func (l *Limiter) Stats() Stats { return l.ctrl.stats() }

// Start launches the control loop in the background. Calling it more than once
// is a no-op: a second loop would double the step rate, and since every gain in
// the control law is a per-tick increment, that silently doubles them too.
func (l *Limiter) Start() {
	l.startOnce.Do(func() {
		l.wg.Add(1)
		go func() {
			defer l.wg.Done()
			t := time.NewTicker(l.cfg.Tick)
			defer t.Stop()
			for {
				select {
				case <-l.stop:
					return
				case <-t.C:
					l.ctrl.step()
				}
			}
		}()
	})
}

// Stop halts the control loop. Safe to call more than once.
func (l *Limiter) Stop() {
	l.stopOnce.Do(func() { close(l.stop) })
	l.wg.Wait()
}

// Resolved is a printable snapshot of the configuration a limiter is actually
// running with: presets applied, defaults filled in, and an SLO anchor that
// could not be honoured reported instead of hidden.
//
// Config itself cannot be logged usefully — it carries two functions — and
// "what did my config turn into" is the first question of every debugging
// session. An adaptive limiter you cannot see into is one you cannot debug.
type Resolved struct {
	// Kind is "rate" for a Limiter and "concurrency" for a ConcurrencyLimiter.
	// For the latter the three *Rate fields below hold the concurrency bounds.
	Kind   string
	Preset string

	MaxRate     int
	MinRate     int
	InitialRate int

	// Anchor is "baseline", "slo", or "baseline (AnchorSLO requested without
	// SLOTarget)". The last form is a config that asked for the absolute anchor
	// and is running the relative one.
	Anchor string
	// Window describes the tolerated band. Only its static part can be known
	// before the first sample — the baseline it is measured against does not
	// exist yet — so it is a formula, not a number.
	Window    string
	SLOTarget time.Duration
	// Tail describes when the p99 guard fires: "200ms (2.0x SLOTarget)", or
	// "off (SLOTarget unset)".
	Tail       string
	TailFactor float64
	// TailTolerance is the ratio-mode threshold; zero means the guard is in
	// quantile mode (the historical behaviour).
	TailTolerance float64

	Tick             time.Duration
	FastHalfLife     time.Duration
	SlowHalfLife     time.Duration
	SlowDownHalfLife time.Duration
	MaxBaselineRise  float64

	TargetInflation float64
	Gain            float64
	UpStep          float64
	DownStep        float64
	SlowStartGain   float64
	Deadband        float64

	BackoffTicks int
	RecoverTicks int
}

// Config returns the configuration this limiter is actually running with.
func (l *Limiter) Config() Resolved { return resolve(l.cfg, "rate") }

// Config returns the configuration this limiter is actually running with. The
// MaxRate/MinRate/InitialRate fields hold the concurrency bounds; Kind says so.
func (l *ConcurrencyLimiter) Config() Resolved { return resolve(l.cfg, "concurrency") }

func resolve(cfg Config, kind string) Resolved {
	r := Resolved{
		Kind:             kind,
		Preset:           cfg.Preset.String(),
		MaxRate:          cfg.MaxRate,
		MinRate:          cfg.MinRate,
		InitialRate:      cfg.InitialRate,
		SLOTarget:        cfg.SLOTarget,
		TailFactor:       cfg.TailFactor,
		TailTolerance:    cfg.TailTolerance,
		Tick:             cfg.Tick,
		FastHalfLife:     cfg.FastHalfLife,
		SlowHalfLife:     cfg.SlowHalfLife,
		SlowDownHalfLife: cfg.SlowDownHalfLife,
		MaxBaselineRise:  cfg.MaxBaselineRise,
		TargetInflation:  cfg.TargetInflation,
		Gain:             cfg.Gain,
		UpStep:           cfg.UpStep,
		DownStep:         cfg.DownStep,
		SlowStartGain:    cfg.SlowStartGain,
		Deadband:         cfg.Deadband,
		BackoffTicks:     cfg.BackoffTicks,
		RecoverTicks:     cfg.RecoverTicks,
	}

	sloWindow := time.Duration(float64(cfg.SLOTarget) * sloWindowFraction)
	ratio := fmt.Sprintf("%.2fx baseline", 1+cfg.TargetInflation)
	switch {
	case cfg.Anchor == AnchorSLO && cfg.SLOTarget > 0:
		r.Anchor = "slo"
		r.Window = fmt.Sprintf("%s (%.1f x SLOTarget)", sloWindow, sloWindowFraction)
	case cfg.Anchor == AnchorSLO:
		r.Anchor = "baseline (AnchorSLO requested without SLOTarget)"
		r.Window = ratio
	case cfg.SLOTarget > 0:
		r.Anchor = "baseline"
		r.Window = fmt.Sprintf("%s, capped at %s", ratio, sloWindow)
	default:
		r.Anchor = "baseline"
		r.Window = ratio
	}

	if cfg.SLOTarget <= 0 {
		r.Tail = "off (SLOTarget unset)"
	} else {
		target := time.Duration(float64(cfg.SLOTarget) * cfg.TailFactor)
		if cfg.TailTolerance > 0 {
			r.Tail = fmt.Sprintf("%.0f%%+%.0fσ of requests above %s (%.1fx SLOTarget)",
				cfg.TailTolerance*100, tailSigma, target, cfg.TailFactor)
		} else {
			r.Tail = fmt.Sprintf("p99 > %s (%.1fx SLOTarget)", target, cfg.TailFactor)
		}
	}
	return r
}

// Validate reports the configurations that New would otherwise repair silently,
// and the ones it cannot make sense of at all.
//
// It validates the config you *wrote*, not the resolved one: every zero field
// means "use the default", which is always valid. Call it before New if you want
// a bad config to fail loudly — New keeps its signature, because a library that
// starts returning errors on upgrade breaks every caller at once.
//
// The repairs it catches are worth knowing by name, because each one used to
// happen in silence: MinRate above MaxRate was clamped, Deadband of 1 turned
// every latency into "healthy", and an AnchorSLO without SLOTarget quietly ran
// the relative law.
func (c Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	if c.MaxRate <= 0 {
		add("MaxRate is required: it is the rate you promise the downstream")
	} else if c.MinRate > c.MaxRate {
		add("MinRate (%d) exceeds MaxRate (%d): the floor would be the ceiling", c.MinRate, c.MaxRate)
	}
	if c.InitialRate < 0 || (c.MaxRate > 0 && c.InitialRate > c.MaxRate) {
		add("InitialRate (%d) must be within [0, MaxRate=%d]", c.InitialRate, c.MaxRate)
	}
	if c.Anchor == AnchorSLO && c.SLOTarget <= 0 {
		add("Anchor=AnchorSLO needs SLOTarget: the absolute anchor has no other threshold to use")
	}
	if c.SLOTarget > 0 && c.SLOTarget < 10*time.Millisecond {
		add("SLOTarget %s is below 10ms, which is usually a unit mistake", c.SLOTarget)
	}
	if c.TailFactor != 0 && c.TailFactor < 1 {
		add("TailFactor %.2f must be at least 1: the guard would fire before the latency you called unacceptable", c.TailFactor)
	}
	if c.TailTolerance < 0 || c.TailTolerance >= 1 {
		add("TailTolerance %.2f must be within [0, 1): 1 means every request may be slow", c.TailTolerance)
	}
	if c.Tick < 0 || c.Tick > 10*time.Second {
		add("Tick %s must be within (0s, 10s]", c.Tick)
	}
	if c.TargetInflation < 0 {
		add("TargetInflation %.3f must not be negative", c.TargetInflation)
	}
	if c.Gain < 0 {
		add("Gain %.3f must not be negative", c.Gain)
	}
	if c.UpStep < 0 || c.DownStep < 0 {
		add("UpStep and DownStep must not be negative")
	}
	if c.DownStep > 1 {
		add("DownStep %.3f would take the rate to zero in one tick", c.DownStep)
	}
	if c.SlowStartGain != 0 && c.SlowStartGain < 1 {
		add("SlowStartGain %.3f is below 1: it would decay the rate while healthy", c.SlowStartGain)
	}
	if c.Deadband < 0 || c.Deadband >= 1 {
		add("Deadband %.3f must be within [0, 1): 1 makes every latency count as healthy", c.Deadband)
	}
	if c.BackoffTicks < 0 || c.RecoverTicks < 0 {
		add("BackoffTicks and RecoverTicks must not be negative")
	}
	if c.MaxBaselineRise < 0 {
		add("MaxBaselineRise %.4f must not be negative", c.MaxBaselineRise)
	}
	// A half-life shorter than a few ticks degenerates the filter into a
	// pass-through (alpha -> 1) and the setting stops meaning anything.
	if c.Tick > 0 {
		if c.FastHalfLife > 0 && c.FastHalfLife < 3*c.Tick {
			add("FastHalfLife %s is under 3x Tick (%s): the filter stops smoothing", c.FastHalfLife, c.Tick)
		}
		if c.SlowHalfLife > 0 && c.SlowHalfLife < 3*c.Tick {
			add("SlowHalfLife %s is under 3x Tick (%s): the baseline stops being a baseline", c.SlowHalfLife, c.Tick)
		}
	}
	return errors.Join(errs...)
}
