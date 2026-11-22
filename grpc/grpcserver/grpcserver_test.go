package grpcserver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Yiming1997/agilelimiter"
	"github.com/Yiming1997/agilelimiter/grpc/internal/grpctest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newLimiter builds a started limiter. A high InitialLimit makes admission the
// default so pass-through tests are not shed.
func newLimiter(t *testing.T, cfg agilelimiter.ConcurrencyConfig) *agilelimiter.ConcurrencyLimiter {
	t.Helper()
	lim := agilelimiter.NewConcurrency(cfg)
	lim.Start()
	t.Cleanup(lim.Stop)
	return lim
}

// sizeOneLimiterHeld returns a limiter with a single slot, already occupied, so
// any request through the interceptor is deterministically shed.
func sizeOneLimiterHeld(t *testing.T) *agilelimiter.ConcurrencyLimiter {
	t.Helper()
	lim := newLimiter(t, agilelimiter.ConcurrencyConfig{MinLimit: 1, MaxLimit: 1, InitialLimit: 1})
	if !lim.TryTake() {
		t.Fatal("could not occupy the single slot")
	}
	t.Cleanup(lim.Release)
	return lim
}

// The default is a pure function, so its rules are testable without a limiter.
func TestDefaultVerdictRules(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want agilelimiter.Outcome
	}{
		{"nil observed", nil, agilelimiter.Reached},
		{"codes.Canceled skipped", status.Error(codes.Canceled, "x"), agilelimiter.Aborted},
		{"ResourceExhausted backs off", status.Error(codes.ResourceExhausted, "x"), agilelimiter.Unreachable},
		{"Unavailable backs off", status.Error(codes.Unavailable, "x"), agilelimiter.Unreachable},
		{"InvalidArgument skipped", status.Error(codes.InvalidArgument, "x"), agilelimiter.Aborted},
		{"NotFound skipped", status.Error(codes.NotFound, "x"), agilelimiter.Aborted},
		{"PermissionDenied skipped", status.Error(codes.PermissionDenied, "x"), agilelimiter.Aborted},
		{"Internal observed", status.Error(codes.Internal, "x"), agilelimiter.Reached},
		{"DeadlineExceeded observed", status.Error(codes.DeadlineExceeded, "x"), agilelimiter.Reached},
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
	lim := newLimiter(t, agilelimiter.ConcurrencyConfig{MaxLimit: 10, InitialLimit: 10})
	ic := NewUnaryInterceptor(lim)

	info := &grpc.UnaryServerInfo{FullMethod: "/pkg.Svc/M"}
	ran := false
	handler := func(ctx context.Context, req any) (any, error) {
		ran = true
		return "reply", nil
	}
	resp, err := ic(context.Background(), nil, info, handler)
	if err != nil {
		t.Fatal(err)
	}
	if resp != "reply" || !ran {
		t.Fatalf("response = %v, ran = %v; want the handler's result passed through", resp, ran)
	}
	if got := lim.InFlight(); got != 0 {
		t.Fatalf("inflight after request = %d, want 0 (slot leaked)", got)
	}
}

func TestUnaryInterceptorShedsWhenFull(t *testing.T) {
	lim := sizeOneLimiterHeld(t)
	ic := NewUnaryInterceptor(lim)

	info := &grpc.UnaryServerInfo{FullMethod: "/pkg.Svc/M"}
	ran := false
	handler := func(ctx context.Context, req any) (any, error) { ran = true; return nil, nil }

	_, err := ic(context.Background(), nil, info, handler)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("code = %v, want ResourceExhausted", status.Code(err))
	}
	if ran {
		t.Fatal("handler ran despite the limiter being full")
	}
}

func TestUnaryInterceptorReleasesSlotOnPanic(t *testing.T) {
	lim := newLimiter(t, agilelimiter.ConcurrencyConfig{MaxLimit: 5, InitialLimit: 5})
	ic := NewUnaryInterceptor(lim)

	info := &grpc.UnaryServerInfo{FullMethod: "/pkg.Svc/M"}
	handler := func(ctx context.Context, req any) (any, error) { panic("boom") }

	func() {
		defer func() { _ = recover() }() // swallow the handler's panic
		_, _ = ic(context.Background(), nil, info, handler)
	}()

	if got := lim.InFlight(); got != 0 {
		t.Fatalf("inflight after panic = %d, want 0 (slot leaked)", got)
	}
}

// newFastLimiter starts a limiter with a 10ms tick and a wide range, so a few
// hundred milliseconds of traffic is enough to see the limit move.
func newFastLimiter(t *testing.T) *agilelimiter.ConcurrencyLimiter {
	t.Helper()
	lim := agilelimiter.NewConcurrency(agilelimiter.ConcurrencyConfig{
		MinLimit: 10, MaxLimit: 200, InitialLimit: 100,
		SLOTarget:        time.Second,
		Tick:             10 * time.Millisecond,
		SlowHalfLife:     10 * time.Millisecond,
		SlowDownHalfLife: 10 * time.Millisecond,
	})
	lim.Start()
	t.Cleanup(lim.Stop)
	return lim
}

// driveFor invokes ic for d from more concurrent callers than newFastLimiter's
// starting limit. One caller at a time would never use more than one slot, and a
// limit nobody uses is not raised however healthy the latency.
func driveFor(t *testing.T, ic grpc.UnaryServerInterceptor, handler grpc.UnaryHandler, d time.Duration) {
	t.Helper()
	info := &grpc.UnaryServerInfo{FullMethod: "/pkg.Svc/M"}
	deadline := time.Now().Add(d)
	var wg sync.WaitGroup
	for i := 0; i < 150; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				_, _ = ic(context.Background(), nil, info, handler)
			}
		}()
	}
	wg.Wait()
}

// fastHandler answers every request with the given error after a short delay.
func fastHandler(err error) grpc.UnaryHandler {
	return func(ctx context.Context, req any) (any, error) {
		time.Sleep(time.Millisecond)
		return nil, err
	}
}

// A handler that answers Unavailable instantly reports a ~0 handling time, which
// without this would read as a fast healthy service and raise the limit.
func TestUnavailableStatusBacksOff(t *testing.T) {
	for _, code := range []codes.Code{codes.ResourceExhausted, codes.Unavailable} {
		t.Run(code.String(), func(t *testing.T) {
			lim := newFastLimiter(t)
			driveFor(t, NewUnaryInterceptor(lim), fastHandler(status.Error(code, "busy")), 300*time.Millisecond)
			if got := lim.Limit(); got > 50 {
				t.Fatalf("limit %d did not back off toward MinLimit on %v", got, code)
			}
		})
	}
}

// codes.Internal is a bug, not a capacity signal: backing off would turn a
// failing endpoint into a smaller failing endpoint and fix nothing.
func TestBugStatusIsObservedNormally(t *testing.T) {
	lim := newFastLimiter(t)
	driveFor(t, NewUnaryInterceptor(lim), fastHandler(status.Error(codes.Internal, "boom")), 300*time.Millisecond)
	if got := lim.Limit(); got < 150 {
		t.Fatalf("limit %d fell on codes.Internal; an application bug must not back the limiter off", got)
	}
}

// A scanner hammering unknown methods gets NotFound in microseconds. Observed,
// they outnumber the real requests, drag the median and the baseline to ~0, and
// leave the real latency reading as congestion once the scan stops.
func TestClientErrorIsNotObserved(t *testing.T) {
	lim := newFastLimiter(t)
	driveFor(t, NewUnaryInterceptor(lim), fastHandler(status.Error(codes.NotFound, "nope")), 300*time.Millisecond)
	if got := lim.Limit(); got != 100 {
		t.Fatalf("limit moved to %d on codes.NotFound; client errors must not be observed", got)
	}
	if s := lim.Stats(); s.Samples != 0 {
		t.Fatalf("%d samples recorded from codes.NotFound, want 0", s.Samples)
	}
}

// End to end through a real in-process server: a saturated limiter sheds the
// call, and the ResourceExhausted reaches the client over the wire.
func TestUnaryInterceptorEndToEnd(t *testing.T) {
	lim := sizeOneLimiterHeld(t)
	srv := &grpctest.Service{Handle: func(context.Context) error { return nil }}
	conn := grpctest.Start(t, srv, []grpc.ServerOption{
		grpc.UnaryInterceptor(NewUnaryInterceptor(lim)),
	}, nil)

	if err := grpctest.Call(context.Background(), conn); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("code = %v, want ResourceExhausted", status.Code(err))
	}
}
