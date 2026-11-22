// Package grpcclient adapts an outbound agilelimiter.Limiter to
// google.golang.org/grpc as a grpc.UnaryClientInterceptor, and decides — once,
// in one function — what each call tells the controller.
//
// # The problem this package exists to keep out of the core
//
// The controller regulates a rate by watching a duration. That only works while
// the duration measures the thing that is scarce. Plenty of calls do not: an
// error the connection refused in a microsecond, a deadline exceeded before
// anything was sent, a local fallback that never touched the downstream. Fed
// unfiltered into one p50, these make a saturated dependency look fast. The
// library cannot fix that from the inside: only the caller knows what a call
// means. So this package asks once, with one hook, and otherwise stays out of
// the way.
//
//	conn, err := grpc.NewClient(addr,
//		grpc.WithTransportCredentials(insecure.NewCredentials()),
//		grpc.WithUnaryInterceptor(grpcclient.NewUnaryInterceptor(lim)),
//	)
//
// A Verdict — returning agilelimiter.Reached, Unreachable or Aborted — is the
// whole policy surface. DefaultVerdict is the behaviour you get without one: it
// is deliberately small and readable, and meant to be copied and edited rather
// than configured through options.
package grpcclient

import (
	"context"
	"errors"
	"time"

	"github.com/Yiming1997/agilelimiter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Verdict decides what one call tells the controller. It is the only policy hook
// this package has, and it is called once per call, on the caller's goroutine, so
// it must not block or allocate.
//
//	method is the full method name (e.g. /pkg.Service/Method), err is exactly what
//	the invoker returned (and may be nil), and d is the wall time spent inside it.
//
// The three answers are the three things the controller can be told:
//
//	Reached      record d as one observation (the default for an answered call)
//	Unreachable  treat it as an outage: back off toward ProbeRate
//	Aborted      record nothing: this call says nothing about capacity
//
// What separates them is not "success or failure" but "did this call use the
// resource we are protecting". A deadline exceeded is a genuine signal and is
// Reached; a connection that never opened is not.
type Verdict func(method string, err error, d time.Duration) agilelimiter.Outcome

// Option customizes the interceptor.
type Option func(*config)

type config struct {
	verdict Verdict
}

// WithVerdict replaces the verdict. Pass nil to restore DefaultVerdict.
func WithVerdict(v Verdict) Option {
	return func(c *config) { c.verdict = v }
}

// NewUnaryInterceptor returns a grpc.UnaryClientInterceptor that takes a slot
// from lim before each call, measures the call, and applies the verdict to the
// result.
//
// A nil verdict means DefaultVerdict.
func NewUnaryInterceptor(lim *agilelimiter.Limiter, opts ...Option) grpc.UnaryClientInterceptor {
	var cfg config
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.verdict == nil {
		cfg.verdict = DefaultVerdict
	}
	i := &unaryInterceptor{lim: lim, verdict: cfg.verdict}
	return i.intercept
}

type unaryInterceptor struct {
	lim     *agilelimiter.Limiter
	verdict Verdict
}

func (i *unaryInterceptor) intercept(ctx context.Context, method string, req, reply any,
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	i.lim.Take()
	start := time.Now()
	err := invoker(ctx, method, req, reply, cc, opts...)
	d := time.Since(start)

	// A canceled call is a truncated measurement, not a fast response: the call
	// stopped because the caller gave up, not because the downstream was quick,
	// so d is only a lower bound. ObserveTruncated keeps it when that bound is
	// already past the SLO, which is evidence of a slow downstream however the
	// call would have ended. That is a fact about the measurement rather than a
	// policy choice, so it is enforced here — replacing the verdict cannot change
	// it.
	if isCanceled(err) {
		i.lim.ObserveTruncated(d)
		return err
	}

	switch i.verdict(method, err, d) {
	case agilelimiter.Unreachable:
		i.lim.Unavailable()
	case agilelimiter.Aborted:
		// Nothing: this call is not evidence about capacity.
	default:
		i.lim.Observe(d)
	}
	return err
}

// isCanceled reports whether the call ended because the caller gave up: either
// the raw context error or its gRPC status form.
func isCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled
}

// DefaultVerdict is what NewUnaryInterceptor does when no verdict is supplied.
// It is small on purpose: copy it and edit it rather than looking for an option
// that tweaks one branch.
//
//	codes.Canceled          Aborted      the caller gave up (handled by the
//	                                     interceptor before the verdict runs)
//	codes.Unavailable       Unreachable  refused / DNS / connection lost: the
//	                                     call never reached the downstream
//	codes.ResourceExhausted Unreachable  "do not send more" (gRPC's 429)
//	anything else           Reached      including DeadlineExceeded, which is a
//	                                     genuine congestion signal
//
// The canceled case is handled by the interceptor before the verdict runs, so a
// replacement verdict cannot lose it; it appears here only so that calling
// DefaultVerdict directly behaves the same way.
//
// One thing it deliberately does not do, because it needs knowledge this
// function does not have: an error your own library invents — an open circuit
// breaker, a rejected payload — is not a status error, so it is observed. Add a
// case if those return fast:
//
//	case errors.Is(err, ErrBreakerOpen):
//		return agilelimiter.Unreachable
func DefaultVerdict(_ string, err error, _ time.Duration) agilelimiter.Outcome {
	switch {
	case isCanceled(err):
		return agilelimiter.Aborted
	case err != nil && (status.Code(err) == codes.Unavailable ||
		status.Code(err) == codes.ResourceExhausted):
		return agilelimiter.Unreachable
	default:
		return agilelimiter.Reached
	}
}
