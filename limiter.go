package agilelimiter

import (
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

	// Clock defaults to the real clock. Inject a fake one in tests.
	Clock Clock
}

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

	stopOnce sync.Once
	stop     chan struct{}
	wg       sync.WaitGroup
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
// bucket, times fn, and feeds the measured round-trip time back to the
// controller. Callers never touch Take/Observe directly, so they cannot
// accidentally feed the wrong latency — the clock starts the moment the slot is
// granted, which is exactly the downstream RTT the controller wants.
//
// fn must be the downstream call itself, performed synchronously and without
// internal retries: the observed value is one attempt's round trip. If fn sends
// asynchronously and returns before the response arrives, Do cannot measure the
// RTT — use Take/Observe by hand in that case. A non-nil error is returned
// unchanged, and its round trip is still observed: a timeout is a real signal
// that the downstream is struggling.
func (l *Limiter) Do(fn func() error) error {
	l.Take()
	start := l.cfg.Clock.Now()
	err := fn()
	l.Observe(l.cfg.Clock.Now().Sub(start))
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
	l.Observe(l.cfg.Clock.Now().Sub(start))
	return v, err
}

// Rate returns the current target rate in requests per second.
func (l *Limiter) Rate() float64 { return l.ctrl.currentRate() }

// Stats is a point-in-time snapshot of the controller's internals, for
// observability. Track these over time; an adaptive limiter you cannot see into
// is one you cannot debug.
type Stats struct {
	Rate     float64       // current target rate, requests per second
	Baseline time.Duration // slow EWMA playing the role of the healthy baseline
	Fast     time.Duration // fast EWMA of the recent median latency
}

// Stats returns a snapshot of the controller's internal signals.
func (l *Limiter) Stats() Stats { return l.ctrl.stats() }

// Start launches the control loop in the background.
func (l *Limiter) Start() {
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
}

// Stop halts the control loop. Safe to call more than once.
func (l *Limiter) Stop() {
	l.stopOnce.Do(func() { close(l.stop) })
	l.wg.Wait()
}
