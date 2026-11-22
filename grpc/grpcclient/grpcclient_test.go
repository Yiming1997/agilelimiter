package grpcclient

import (
	"context"
	"testing"
	"time"

	"github.com/Yiming1997/agilelimiter"
	"github.com/Yiming1997/agilelimiter/grpc/internal/grpctest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newLimiter starts a limiter with a fast tick so a control step happens well
// inside the test.
func newLimiter(t *testing.T, cfg agilelimiter.Config) *agilelimiter.Limiter {
	t.Helper()
	cfg.Tick = 10 * time.Millisecond
	cfg.SlowHalfLife = 10 * time.Millisecond
	lim := agilelimiter.New(cfg)
	lim.Start()
	t.Cleanup(lim.Stop)
	return lim
}

// waitForBaseline waits until the controller has a baseline, or the deadline
// passes.
func waitForBaseline(lim *agilelimiter.Limiter, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if lim.Stats().Baseline > 0 {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// The default is a pure function, so its rules are testable directly instead of
// only through a running limiter.
func TestDefaultVerdictRules(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want agilelimiter.Outcome
	}{
		{"nil observed", nil, agilelimiter.Reached},
		{"codes.Canceled skipped", status.Error(codes.Canceled, "x"), agilelimiter.Aborted},
		{"raw context.Canceled skipped", context.Canceled, agilelimiter.Aborted},
		{"Unavailable backs off", status.Error(codes.Unavailable, "x"), agilelimiter.Unreachable},
		{"ResourceExhausted backs off", status.Error(codes.ResourceExhausted, "x"), agilelimiter.Unreachable},
		{"DeadlineExceeded observed", status.Error(codes.DeadlineExceeded, "x"), agilelimiter.Reached},
		{"Internal observed", status.Error(codes.Internal, "x"), agilelimiter.Reached},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := DefaultVerdict("/pkg.Svc/M", c.err, time.Millisecond); got != c.want {
				t.Fatalf("DefaultVerdict = %v, want %v", got, c.want)
			}
		})
	}
}

func TestUnaryInterceptorPassesThrough(t *testing.T) {
	lim := newLimiter(t, agilelimiter.Config{MinRate: 10, MaxRate: 1000, InitialRate: 1000})
	ic := NewUnaryInterceptor(lim)

	calls := 0
	invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		calls++
		return nil
	}
	if err := ic(context.Background(), "/pkg.Svc/M", nil, nil, nil, invoker); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("underlying invoker calls = %d, want 1", calls)
	}
}

// driveFor keeps invoking ic for d. The limiter is driven by its own ticker, so
// the measurement has to run long enough for a few control steps.
func driveFor(t *testing.T, ic grpc.UnaryClientInterceptor, err error, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		time.Sleep(time.Millisecond)
		return err
	}
	for time.Now().Before(deadline) {
		if e := ic(context.Background(), "/pkg.Svc/M", nil, nil, nil, invoker); e != nil && e != err {
			t.Fatal(e)
		}
	}
}

// A refused connection or a ResourceExhausted returns in microseconds. Without
// the verdict that reads as a fast, healthy call and the controller accelerates
// into the service that just refused it. A deadline exceeded is a real round
// trip and must be observed normally.
func TestStatusClassification(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		verdict Verdict
		backoff bool
	}{
		{"Unavailable backs off", status.Error(codes.Unavailable, "x"), nil, true},
		{"ResourceExhausted backs off", status.Error(codes.ResourceExhausted, "x"), nil, true},
		{"DeadlineExceeded is observed", status.Error(codes.DeadlineExceeded, "x"), nil, false},
		{"a custom verdict turns it off", status.Error(codes.Unavailable, "x"),
			func(string, error, time.Duration) agilelimiter.Outcome { return agilelimiter.Reached }, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			lim := newLimiter(t, agilelimiter.Config{
				MinRate: 10, MaxRate: 200, InitialRate: 100,
				SLOTarget: time.Second,
			})
			var opts []Option
			if c.verdict != nil {
				opts = append(opts, WithVerdict(c.verdict))
			}
			ic := NewUnaryInterceptor(lim, opts...)
			driveFor(t, ic, c.err, 300*time.Millisecond)

			got := lim.Rate()
			if c.backoff && got > 50 {
				t.Fatalf("rate %v did not back off toward MinRate on %v", got, c.err)
			}
			if !c.backoff && got < 150 {
				t.Fatalf("rate %v fell on %v, which must be observed normally", got, c.err)
			}
		})
	}
}

// Replacing the verdict must not be able to reintroduce "a canceled call reads as
// a fast healthy response": the interceptor filters that before the verdict runs,
// because it is a broken measurement rather than a policy choice.
func TestCanceledCallIsNotObservedEvenWithAPermissiveVerdict(t *testing.T) {
	lim := newLimiter(t, agilelimiter.Config{MinRate: 10, MaxRate: 1000, InitialRate: 1000})
	ic := NewUnaryInterceptor(lim, WithVerdict(
		func(string, error, time.Duration) agilelimiter.Outcome { return agilelimiter.Reached }))

	invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return context.Canceled
	}
	for i := 0; i < 5; i++ {
		if err := ic(context.Background(), "/pkg.Svc/M", nil, nil, nil, invoker); err != context.Canceled {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	}
	if waitForBaseline(lim, 300*time.Millisecond) {
		t.Fatalf("a canceled call reached the controller (baseline %v); the interceptor must filter it", lim.Stats().Baseline)
	}
}

// End to end through a real in-process server: the interceptor runs in the gRPC
// chain, takes a slot, and feeds a successful call back to the controller.
func TestUnaryInterceptorEndToEnd(t *testing.T) {
	lim := newLimiter(t, agilelimiter.Config{MinRate: 10, MaxRate: 1000, InitialRate: 1000})
	srv := &grpctest.Service{Handle: func(context.Context) error {
		time.Sleep(time.Millisecond)
		return nil
	}}
	conn := grpctest.Start(t, srv, nil, []grpc.DialOption{
		grpc.WithUnaryInterceptor(NewUnaryInterceptor(lim)),
	})

	for i := 0; i < 5; i++ {
		if err := grpctest.Call(context.Background(), conn); err != nil {
			t.Fatal(err)
		}
	}
	if !waitForBaseline(lim, time.Second) {
		t.Fatal("a successful call through the interceptor must feed the controller")
	}
}
