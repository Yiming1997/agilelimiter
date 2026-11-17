// Package httplimit adapts per-key quotas to net/http.
package httplimit

import (
	"errors"
	"net/http"
	"net/netip"
	"strconv"
	"time"

	iplimiter "github.com/Yiming1997/agilelimiter/iplimiter"
)

const (
	HeaderRateLimitLimit     = "X-RateLimit-Limit"
	HeaderRateLimitRemaining = "X-RateLimit-Remaining"
	HeaderRateLimitReset     = "X-RateLimit-Reset" // UTC HTTP-date.
	HeaderRetryAfter         = "Retry-After"
)

type KeyFunc func(*http.Request) (string, error)

// IPKeyFunc Retrieve key by IP address
func IPKeyFunc() KeyFunc {
	return func(r *http.Request) (string, error) {
		peer, err := netip.ParseAddrPort(r.RemoteAddr)
		if err != nil {
			return "", err
		}
		return peer.Addr().Unmap().String(), nil
	}
}

type Middleware struct {
	store   iplimiter.Store
	keyFunc KeyFunc
}

func NewMiddleware(s iplimiter.Store, key KeyFunc) (*Middleware, error) {
	if s == nil {
		return nil, errors.New("httplimit: store is required")
	}
	if key == nil {
		return nil, errors.New("httplimit: key function is required")
	}
	return &Middleware{store: s, keyFunc: key}, nil
}

// Handle counts attempts, including downstream rejections. Allowed requests
// retain the original ResponseWriter and its optional streaming interfaces.
func (m *Middleware) Handle(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, err := m.keyFunc(r)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		limit, remaining, reset, allowed, err := m.store.Take(r.Context(), key)
		if err != nil {
			status := http.StatusInternalServerError

			switch {
			case errors.Is(err, iplimiter.ErrCapacity):
				// The store is full: the server cannot serve this key right
				// now. That is the server's problem, not the caller's.
				status = http.StatusServiceUnavailable
			case errors.Is(err, iplimiter.ErrKeyTooLong):
				// The key came from the KeyFunc, so an over-long one means
				// the caller supplied an unbounded identifier. The client
				// cannot fix it, but 400 says "do not send this again"
				// where 500 says "try later" and would just repeat.
				status = http.StatusBadRequest
			}

			http.Error(w, http.StatusText(status), status)
			return
		}
		deadline := time.Unix(0, int64(reset))
		// HTTP-date has second precision: round up to avoid an early retry.
		if deadline.Nanosecond() != 0 {
			deadline = deadline.Truncate(time.Second).Add(time.Second)
		}
		resetHeader := deadline.UTC().Format(http.TimeFormat)
		h := w.Header()
		h.Set(HeaderRateLimitLimit, strconv.FormatUint(limit, 10))
		h.Set(HeaderRateLimitRemaining, strconv.FormatUint(remaining, 10))
		h.Set(HeaderRateLimitReset, resetHeader)
		if !allowed {
			h.Set(HeaderRetryAfter, resetHeader)
			http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}
