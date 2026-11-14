package inbound

import (
	"context"
	"errors"
	"net/http"

	"github.com/Yiming1997/agilelimiter"
	"github.com/Yiming1997/agilelimiter/httpserver"
	iplimiter "github.com/Yiming1997/agilelimiter/inbound_limiter"
	httplimit "github.com/Yiming1997/agilelimiter/inbound_limiter/httplimiter"
	"github.com/Yiming1997/agilelimiter/inbound_limiter/memorystore"
)

// Handler applies per-key quotas before the existing adaptive HTTP middleware.
type Handler struct {
	handler http.Handler
	store   iplimiter.Store
}

// NewHandler counts attempts, including requests later rejected by adaptive
// admission. The caller owns adaptive and must manage its Start/Stop lifecycle.
func NewHandler(next http.Handler, adaptive *agilelimiter.ConcurrencyLimiter, cfg Config) (*Handler, error) {
	if next == nil {
		return nil, errors.New("inbound: handler is required")
	}
	if adaptive == nil {
		return nil, errors.New("inbound: adaptive limiter is required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	h := &Handler{handler: httpserver.Wrap(next, adaptive, cfg.AdaptiveOptions...)}
	if !cfg.Enabled {
		return h, nil
	}

	key := cfg.KeyFunc
	if key == nil {
		key = httplimit.IPKeyFunc()
	}
	h.store = memorystore.New(&memorystore.Config{Tokens: cfg.Tokens, Interval: cfg.Interval})
	ip, err := httplimit.NewMiddleware(h.store, key)
	if err != nil {
		_ = h.store.Close(context.Background())
		return nil, err
	}
	h.handler = ip.Handle(h.handler)
	return h, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.handler.ServeHTTP(w, r)
}

// Close releases the owned quota store; it does not stop adaptive. Shut down the
// HTTP server first so business handlers finish before their resources close.
func (h *Handler) Close(ctx context.Context) error {
	if h.store == nil {
		return nil
	}
	return h.store.Close(ctx)
}
