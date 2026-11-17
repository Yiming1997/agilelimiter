package inbound

import (
	"errors"
	"time"

	"github.com/Yiming1997/agilelimiter/httpserver"
	httplimit "github.com/Yiming1997/agilelimiter/iplimiter/httplimit"
)

// Config controls process-local fixed-window quotas. Separate processes have
// separate quotas; fixed windows allow bursts around a window boundary.
type Config struct {
	Enabled  bool
	Tokens   uint64
	Interval time.Duration

	// Zero uses the memory store's defaults.
	MaxKeys       int64
	SweepInterval time.Duration
	SweepMinTTL   time.Duration
	// MaxKeyLen bounds the length of a key, in bytes. A longer key is
	// rejected rather than stored, so an unbounded identifier cannot turn
	// into unbounded memory. Zero uses the memory store's default.
	MaxKeyLen int

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
	if c.MaxKeys < 0 {
		return errors.New("inbound: max keys must not be negative")
	}
	if c.SweepInterval < 0 {
		return errors.New("inbound: sweep interval must not be negative")
	}
	if c.SweepMinTTL < 0 {
		return errors.New("inbound: sweep TTL must not be negative")
	}
	if c.MaxKeyLen < 0 {
		return errors.New("inbound: max key length must not be negative")
	}

	return nil
}
