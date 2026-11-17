package httplimit

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	iplimiter "github.com/Yiming1997/agilelimiter/iplimiter"
	"github.com/Yiming1997/agilelimiter/iplimiter/memorystore"
)

// errStore is a store whose Take always fails with a chosen error, so the
// middleware's mapping of store failures onto status codes can be tested
// without provoking the real condition.
type errStore struct {
	err error
}

func (s *errStore) Take(context.Context, string) (uint64, uint64, uint64, bool, error) {
	return 0, 0, 0, false, s.err
}
func (s *errStore) Get(context.Context, string) (uint64, uint64, error) { return 0, 0, nil }
func (s *errStore) Set(context.Context, string, uint64, time.Duration) error {
	return nil
}
func (s *errStore) Burst(context.Context, string, uint64) error { return nil }
func (s *errStore) Close(context.Context) error                 { return nil }

func serve(t *testing.T, store iplimiter.Store, key string) *httptest.ResponseRecorder {
	t.Helper()
	m, err := NewMiddleware(store, func(*http.Request) (string, error) { return key, nil })
	if err != nil {
		t.Fatalf("NewMiddleware = %v, want no error", err)
	}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	rec := httptest.NewRecorder()
	m.Handle(next).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	return rec
}

func TestMiddlewareMapsStoreErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"key capacity reached", iplimiter.ErrCapacity, http.StatusServiceUnavailable},
		{"key too long", iplimiter.ErrKeyTooLong, http.StatusBadRequest},
		{"anything else", errors.New("boom"), http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := serve(t, &errStore{err: tc.err}, "k")
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}
		})
	}
}

// A key the store refuses for being too long reaches the client as a 400,
// not as a 500: the identifier is the caller's to fix.
func TestMiddlewareRejectsOverLongKey(t *testing.T) {
	store := memorystore.New(&memorystore.Config{
		Tokens:    5,
		Interval:  time.Minute,
		MaxKeyLen: 16,
	})
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	rec := serve(t, store, strings.Repeat("k", 17))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

// A request within its quota is admitted and carries the rate limit headers.
func TestMiddlewareAdmitsWithinQuota(t *testing.T) {
	store := memorystore.New(&memorystore.Config{Tokens: 2, Interval: time.Minute})
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	rec := serve(t, store, "203.0.113.45")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get(HeaderRateLimitLimit); got != "2" {
		t.Errorf("%s = %q, want %q", HeaderRateLimitLimit, got, "2")
	}
	if got := rec.Header().Get(HeaderRateLimitRemaining); got != "1" {
		t.Errorf("%s = %q, want %q", HeaderRateLimitRemaining, got, "1")
	}
	if rec.Header().Get(HeaderRateLimitReset) == "" {
		t.Errorf("%s is empty, want an HTTP-date", HeaderRateLimitReset)
	}
}

// The second request of a two-token quota is shed with a 429 and a
// Retry-After the client can act on.
func TestMiddlewareShedsPastQuota(t *testing.T) {
	store := memorystore.New(&memorystore.Config{Tokens: 1, Interval: time.Minute})
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	if rec := serve(t, store, "203.0.113.45"); rec.Code != http.StatusOK {
		t.Fatalf("first request status = %d, want %d", rec.Code, http.StatusOK)
	}

	rec := serve(t, store, "203.0.113.45")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("second request status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
	if rec.Header().Get(HeaderRetryAfter) == "" {
		t.Errorf("%s is empty, want a value", HeaderRetryAfter)
	}
}
