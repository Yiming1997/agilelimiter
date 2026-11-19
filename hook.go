package agilelimiter

import "time"

// Hooks groups the callbacks that run around each Do call. They let a caller
// impose its own admission discipline in front of the adaptive actuator — a
// token/leaky bucket, a per-tenant cap, a priority gate — without replacing the
// controller.
//
// Admit hooks run before a slot is taken from the bucket, in order. A non-nil
// error stops the call: the remaining Admit hooks and fn are skipped, no slot is
// taken and nothing is observed, and Do returns that error. A blocking Admit
// hook is the place for custom flow control.
//
// Observe hooks run after fn and after the call's result has been fed to the
// controller, in order. Each one receives an Observation — the measured
// duration, the call's error, and the Outcome the controller was told — so it
// can log, export metrics or trace without guessing. They are post-call side
// effects and cannot change the result; they should be fast and must not block,
// because they run on the caller's goroutine after the sample is recorded.
//
// Hooks are set once, before serving traffic; SetHooks refuses a second call.
// Because the pointer never changes after that, Do can read it without
// synchronization. Do not mutate a Hooks (its slices or handlers) once it is set.
type Hooks struct {
	Admit   []AdmitHook
	Observe []ObserveHook
}

// AdmitHook is an admission hook. It runs before admission, may block, and a
// non-nil error rejects the call (see Hooks).
type AdmitHook func() error

// Observation is what an Observe hook sees about the call that just completed.
// It is read-only: the controller has already been told what to do with it.
type Observation struct {
	// Duration is the measured round trip (or handling time) fed to the
	// controller. It excludes any throttle wait before the call.
	Duration time.Duration
	// Err is the call's error, nil on success.
	Err error
	// Outcome is how the call was classified and reported: Reached (observed),
	// Unreachable (outage), or Aborted (nothing recorded).
	Outcome Outcome
}

// ObserveHook is a post-call hook. It runs after the call has been observed and
// cannot change its result. It returns nothing on purpose: attribution is
// Config.Classify, and a hook that could return an error here would only raise
// the question of whether that error should reach the caller.
type ObserveHook func(Observation)

// admit runs the Admit handlers in order and stops at the first error.
func (h *Hooks) admit() error {
	for _, f := range h.Admit {
		if f == nil {
			continue
		}
		if err := f(); err != nil {
			return err
		}
	}
	return nil
}

// observe runs the Observe handlers in order, passing each the call's
// observation.
func (h *Hooks) observe(obs Observation) {
	for _, f := range h.Observe {
		if f == nil {
			continue
		}
		f(obs)
	}
}
