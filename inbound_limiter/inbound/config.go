package inbound

import (
	"errors"
	"time"

	"github.com/Yiming1997/agilelimiter/httpserver"
	httplimit "github.com/Yiming1997/agilelimiter/inbound_limiter/httplimiter"
)

// Config controls process-local fixed-window quotas. Separate processes have
// separate quotas; fixed windows allow bursts around a window boundary.
type Config struct {
	Enabled  bool // Controls quotas only; adaptive admission remains active.
	Tokens   uint64
	Interval time.Duration
	// Defaults to the direct peer IP. Use a custom function for authenticated
	// identities or trusted proxies; untrusted forwarding headers are not IPs.
	KeyFunc         httplimit.KeyFunc
	AdaptiveOptions []httpserver.Option
}

func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.Tokens == 0 {
		return errors.New("inbound: tokens must be positive")
	}
	if c.Interval <= 0 {
		return errors.New("inbound: interval must be positive")
	}
	return nil
}
