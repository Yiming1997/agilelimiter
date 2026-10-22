package agilelimiter

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Config configures a Limiter. Zero fields take sensible defaults (see withDefaults).
type Config struct {
	// MinRate and MaxRate bound the dynamic rate, in requests per second.
	// MaxRate is the *hard ceiling* you are willing to promise.
	MinRate int
	MaxRate int
	// InitialRate is the starting rate. Cold-start low and let the controller
	// ramp up — starting at MaxRate risks hitting the downstream before the
	// first feedback arrives.
	InitialRate int
	// Per is the time window the rate is expressed over. Default: one second.
	Per time.Duration

	// SLOTarget enables the tail-latency guard: when the per-tick p99 exceeds
	// it, the controller forces a full backoff step. Zero disables the guard.
	SLOTarget time.Duration

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
	// Deadband is the hold band around the target latency, as a fraction of it.
	Deadband float64

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

func withDefaults(c Config) Config {
	if c.Per <= 0 {
		c.Per = time.Second
	}
	if c.MinRate < 1 {
		c.MinRate = 1
	}
	if c.MaxRate < c.MinRate {
		c.MaxRate = c.MinRate
	}
	if c.InitialRate <= 0 {
		c.InitialRate = c.MinRate
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
	if c.Deadband <= 0 {
		c.Deadband = 0.05
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
	b := newBucket(cfg.InitialRate, cfg.Per, cfg.Clock)
	h := &histogram{}
	l := &Limiter{
		cfg:    cfg,
		bucket: b,
		hist:   h,
		stop:   make(chan struct{}),
	}
	l.ctrl = newController(cfg, h, func(rate int) { b.setRate(rate, cfg.Per) })
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
