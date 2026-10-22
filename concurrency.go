package agilelimiter

import (
	"errors"
	"sync"
	"time"
)

// ErrOverloaded is returned by ConcurrencyLimiter.Do when the concurrency limit
// is already reached. Map it to HTTP 429 (StatusTooManyRequests) or gRPC
// codes.ResourceExhausted.
var ErrOverloaded = errors.New("agilelimiter: concurrency limit reached")

// ConcurrencyConfig configures a ConcurrencyLimiter. It mirrors Config, but the
// bounds are a *concurrency limit* (how many requests may be in flight at once)
// rather than a rate. The control-law knobs are identical and share the same
// defaults.
type ConcurrencyConfig struct {
	// MinLimit and MaxLimit bound the dynamic concurrency limit, in requests.
	// MaxLimit is the *hard ceiling* you are willing to promise the service.
	// MaxLimit is required: there is no sensible default for it.
	MinLimit int
	MaxLimit int
	// InitialLimit is the starting concurrency. Cold-start low and let the
	// controller ramp up — starting at MaxLimit risks flooding yourself before
	// the first feedback arrives.
	InitialLimit int

	// SLOTarget enables the tail-latency guard: when the per-tick p99 exceeds
	// it, the controller forces a full backoff step. Zero disables the guard.
	SLOTarget time.Duration

	// EWMA half-lives for the control signal.
	FastHalfLife     time.Duration // recent latency
	SlowHalfLife     time.Duration // healthy baseline (creeps up slowly)
	SlowDownHalfLife time.Duration // baseline follows improvements down

	// MaxBaselineRise caps how fast the healthy baseline may creep upward, as a
	// fraction per tick. Zero takes the default. See Config.MaxBaselineRise.
	MaxBaselineRise float64

	// Tick is the control-loop period.
	Tick time.Duration

	// TargetInflation is how far above the healthy baseline latency the fast
	// signal may rise before the controller backs off. 1.0 means "accept up to
	// 2x the baseline".
	TargetInflation float64
	// Gain scales the proportional backoff, capped at DownStep. E.g. 1.0.
	Gain float64
	// UpStep / DownStep cap the fractional change per tick (down faster than up).
	UpStep   float64
	DownStep float64
	// Deadband is the hold band around the target latency, as a fraction of it.
	Deadband float64

	// Classify maps a failed request to what the controller is told about it. It
	// is only consulted for non-nil errors; a nil error always means Reached.
	//
	// The default (nil) treats every error as Reached. That is wrong for a
	// handler that fails without doing any work — a cached 503 while a dependency
	// is down, an auth rejection, a circuit breaker returning early. Its handling
	// time is ~0, the controller reads it as "this service is fast", and it raises
	// the limit while the service is actually degrading. Return Unreachable for
	// those; return Aborted for client-side cancellations.
	Classify func(err error) Outcome

	// Clock defaults to the real clock. Inject a fake one in tests.
	Clock Clock
}

// ConcurrencyLimiter is an adaptive, self-protecting concurrency limiter for the
// *inbound* side of a service: it caps how many requests are handled at once and
// admits or refuses each one non-blockingly.
//
// It shares its brain with Limiter — the same fast/slow EWMA baseline, target
// window and p99 guard — and swaps only the actuator, from a leaky bucket to a
// semaphore. The signal is the handler's own handling time; the target is a
// concurrency count instead of a rate.
//
//	limit climbs   ->   latency rises past the window   ->   limit backs off
//
// A request that cannot get a slot is refused immediately (ErrOverloaded) rather
// than queued, which is what keeps the limiter from amplifying an overload.
type ConcurrencyLimiter struct {
	cfg  Config // internal tuning; the Rate fields hold the limit bounds
	sem  *semaphore
	hist *histogram
	ctrl *controller

	startOnce sync.Once
	stopOnce  sync.Once
	stop      chan struct{}
	wg        sync.WaitGroup
}

// NewConcurrency builds a ConcurrencyLimiter. It does not start the control loop;
// call Start (or drive ctrl.step yourself, as tests do).
//
// Internally it reuses the outbound controller verbatim: that controller only
// multiplies a scalar by a factor and clamps it, so it is agnostic to whether the
// scalar is a rate or a concurrency limit. We map the limit bounds onto its
// bound fields and point its setter at the semaphore.
func NewConcurrency(cc ConcurrencyConfig) *ConcurrencyLimiter {
	cfg := withDefaults(Config{
		MinRate:          cc.MinLimit,
		MaxRate:          cc.MaxLimit,
		InitialRate:      cc.InitialLimit,
		SLOTarget:        cc.SLOTarget,
		FastHalfLife:     cc.FastHalfLife,
		SlowHalfLife:     cc.SlowHalfLife,
		SlowDownHalfLife: cc.SlowDownHalfLife,
		MaxBaselineRise:  cc.MaxBaselineRise,
		Tick:             cc.Tick,
		TargetInflation:  cc.TargetInflation,
		Gain:             cc.Gain,
		UpStep:           cc.UpStep,
		DownStep:         cc.DownStep,
		Deadband:         cc.Deadband,
		Classify:         cc.Classify,
		Clock:            cc.Clock,
	})

	sem := newSemaphore(cfg.InitialRate)
	h := &histogram{}
	l := &ConcurrencyLimiter{
		cfg:  cfg,
		sem:  sem,
		hist: h,
		stop: make(chan struct{}),
	}
	l.ctrl = newController(cfg, h, func(limit int) { sem.setLimit(limit) })
	return l
}

// TryTake admits the caller if there is room under the current limit and reports
// whether it was admitted. It never blocks: at the limit it returns false at
// once, so an inbound caller sheds load instead of queuing. Every true result
// must be paired with exactly one Release.
func (l *ConcurrencyLimiter) TryTake() bool { return l.sem.tryAcquire() }

// Release frees one slot taken by TryTake.
func (l *ConcurrencyLimiter) Release() { l.sem.release() }

// Observe records the handling time of one admitted request. Feed it the
// handler's own duration, measured from the moment it was admitted to when it
// finished. Because admission never blocks, this is clean by construction — there
// is no throttle wait to accidentally contaminate the signal. Safe for
// concurrent use and allocation-free.
func (l *ConcurrencyLimiter) Observe(d time.Duration) { l.hist.record(d) }

// Do admits a request if there is room and runs fn, observing fn's outcome; when
// the limit is reached it returns ErrOverloaded without running fn. The slot is
// released even if fn panics.
//
//	resp, err := do(...)
//	if errors.Is(err, ErrOverloaded) { http.Error(w, "overloaded", 429); return }
//
// As with Limiter.Do, Config.Classify decides what a failure tells the
// controller. By default a failed handler is still observed as an ordinary
// handling time; classify the "failed without doing any work" errors as
// Unreachable so they cannot pass for a fast, healthy handler.
func (l *ConcurrencyLimiter) Do(fn func() error) error {
	if !l.sem.tryAcquire() {
		return ErrOverloaded
	}
	start := l.cfg.Clock.Now()
	defer l.sem.release()
	err := fn()
	l.observeOutcome(l.cfg.Clock.Now().Sub(start), err)
	return err
}

// observeOutcome tells the controller what a request's result says about the
// service. See Limiter.observeOutcome and Outcome.
func (l *ConcurrencyLimiter) observeOutcome(d time.Duration, err error) {
	out := defaultOutcome(err) // context.Canceled -> Aborted, see defaultOutcome
	if err != nil && l.cfg.Classify != nil {
		out = l.cfg.Classify(err)
	}
	switch {
	case out == Unreachable:
		l.Unavailable()
	case out == Aborted:
		// Nothing: the service never expressed an opinion.
	default:
		l.hist.record(d)
	}
}

// Unavailable records one request that failed without the service doing any
// work — a dependency is down and the handler answers 503 immediately, a circuit
// breaker is open, a request is rejected before touching the hot path. Record it
// instead of Observe for that request: its handling time is ~0 and would read as
// a fast, healthy service, so the controller would raise the limit while the
// service is struggling. It feeds a synthetic "congested" sample that backs the
// limit off toward MinLimit.
//
// It is a no-op before the controller has anything to anchor a synthetic value
// against: with no SLOTarget and no samples yet there is no honest number to
// record, and inventing one would corrupt the baseline.
//
// Use this from the manual TryTake/Observe path, or return Unreachable from
// Config.Classify to have Do call it for you. Never do both for the same
// request.
func (l *ConcurrencyLimiter) Unavailable() {
	if s, ok := l.ctrl.outageSample(); ok {
		l.hist.record(s)
	}
}

// ObserveOutcome records how one request ended, applying Config.Classify to the
// error. Use it from an integration that admits, times and classifies requests
// itself, so the error policy stays in one place. It is exactly what Do does
// with the same two values.
func (l *ConcurrencyLimiter) ObserveOutcome(d time.Duration, err error) {
	l.observeOutcome(d, err)
}

// Limit returns the current concurrency limit, in requests.
func (l *ConcurrencyLimiter) Limit() int { return l.sem.cap() }

// InFlight returns the number of requests currently being served.
func (l *ConcurrencyLimiter) InFlight() int { return l.sem.inFlight() }

// Stats returns a snapshot of the controller's internal signals. For a
// concurrency limiter, the Rate field holds the current limit, not a rate.
func (l *ConcurrencyLimiter) Stats() Stats { return l.ctrl.stats() }

// Start launches the control loop in the background. Calling it more than once
// is a no-op: a second loop would double the step rate, and since every gain in
// the control law is a per-tick increment, that silently doubles them too.
func (l *ConcurrencyLimiter) Start() {
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
func (l *ConcurrencyLimiter) Stop() {
	l.stopOnce.Do(func() { close(l.stop) })
	l.wg.Wait()
}
