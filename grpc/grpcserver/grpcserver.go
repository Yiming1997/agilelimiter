// Package grpcserver adapts an inbound agilelimiter.ConcurrencyLimiter to
// google.golang.org/grpc: it sheds load with codes.ResourceExhausted when the
// service is saturated, and feeds each admitted request's handling time back to
// the controller.
//
// # One hook, no built-in opinion about what a response means
//
// A handler that fails instantly reports a ~0 handling time, and so does one
// served from a cache, one rejected by an auth check, and a validation error.
// Fed unfiltered into one p50 they make a struggling service look fast. Only the
// caller knows what a response means, so this package asks once:
//
//	srv := grpc.NewServer(
//		grpc.UnaryInterceptor(grpcserver.NewUnaryInterceptor(lim,
//			grpcserver.WithVerdict(myVerdict))),
//	)
//
// DefaultVerdict is the behaviour you get without one: small, readable, and meant
// to be copied and edited rather than configured through options.
package grpcserver

import (
	"context"
	"errors"
	"time"

	"github.com/Yiming1997/agilelimiter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Verdict decides what one admitted request tells the controller. It is the only
// policy hook this interceptor has, and it is called once per request, after the
// handler ran, so it must not block or allocate.
//
//	method is the full method name (info.FullMethod), err is what the handler
//	returned (and may be nil), and d is the handler's own duration.
//
// The three answers are the three things the controller can be told:
//
//	Reached      record d as one observation (the default for real work)
//	Unreachable  treat it as an outage: back off toward ProbeLimit
//	Aborted      record nothing: this request says nothing about capacity
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

// NewUnaryInterceptor returns a grpc.UnaryServerInterceptor that admits each
// request through lim. An admitted request runs handler and its handling time is
// reported to the controller through the verdict. A request that cannot get a
// slot is refused with codes.ResourceExhausted rather than queued — shedding,
// not queueing, is what keeps the limiter from amplifying an overload.
//
// The measured time is handler's own duration: admission never blocks, so there
// is no queue wait to contaminate the signal, and refused requests are never
// measured. The slot is released even if handler panics.
//
// A nil verdict means DefaultVerdict.
func NewUnaryInterceptor(lim *agilelimiter.ConcurrencyLimiter, opts ...Option) grpc.UnaryServerInterceptor {
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
	lim     *agilelimiter.ConcurrencyLimiter
	verdict Verdict
}

func (i *unaryInterceptor) intercept(ctx context.Context, req any, info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (any, error) {
	if !i.lim.TryTake() {
		return nil, status.Error(codes.ResourceExhausted, "server overloaded")
	}
	defer i.lim.Release() // released even if handler panics

	start := time.Now()
	resp, err := handler(ctx, req)
	d := time.Since(start)

	// A canceled request is a truncated measurement: the handler returned because
	// the client went away, not because it was fast, so d is only a lower bound.
	// ObserveTruncated keeps it when that bound is already past the SLO — the
	// requests clients give up on are the slowest ones, and dropping them all
	// blinded the limiter in the overload it exists for. That is a fact about the
	// measurement rather than a policy choice, so it is enforced here — replacing
	// the verdict cannot change it.
	if ctx.Err() != nil || isCanceled(err) {
		i.lim.ObserveTruncated(d)
		return resp, err
	}

	switch i.verdict(info.FullMethod, err, d) {
	case agilelimiter.Unreachable:
		i.lim.Unavailable()
	case agilelimiter.Aborted:
		// Nothing: this request is not evidence about capacity.
	default:
		i.lim.Observe(d)
	}
	return resp, err
}

func isCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled
}

// DefaultVerdict is what NewUnaryInterceptor does when no verdict is supplied.
// It is small on purpose: copy it and edit it rather than looking for an option
// that tweaks one branch.
//
//	codes.Canceled            Aborted      the client went away (handled by the
//	                                       interceptor before the verdict runs)
//	codes.ResourceExhausted,
//	codes.Unavailable         Unreachable  "cannot serve right now"
//	client errors
//	(InvalidArgument, NotFound,
//	 PermissionDenied, ...)   Aborted      refused before the work: a 404 / 401
//	                                       answers in microseconds and says
//	                                       nothing about capacity
//	anything else             Reached      including codes.Internal, which is
//	                                       usually a bug: backing off would turn
//	                                       a failing endpoint into a smaller one
//
// Two things it deliberately does not do, because they need knowledge this
// function does not have:
//
//   - Health checks and cheap probes have no opinion about their own cost:
//     exclude them by method name.
//   - A long-lived stream holds one slot for its whole life and drags the median
//     up for every other request: give it its own limiter.
func DefaultVerdict(_ string, err error, _ time.Duration) agilelimiter.Outcome {
	switch {
	case isCanceled(err):
		return agilelimiter.Aborted
	case err != nil && (status.Code(err) == codes.ResourceExhausted ||
		status.Code(err) == codes.Unavailable):
		return agilelimiter.Unreachable
	case err != nil && isRefusedCode(status.Code(err)):
		return agilelimiter.Aborted
	default:
		return agilelimiter.Reached
	}
}

// isRefusedCode is the gRPC codes that mean "refused before the expensive work",
// the analogue of the 4xx band in the HTTP middleware: a scanner's NotFound, a
// wave of Unauthenticated, a validation InvalidArgument.
func isRefusedCode(c codes.Code) bool {
	switch c {
	case codes.InvalidArgument, codes.NotFound, codes.PermissionDenied,
		codes.Unauthenticated, codes.FailedPrecondition, codes.AlreadyExists,
		codes.Aborted, codes.OutOfRange:
		return true
	}
	return false
}
