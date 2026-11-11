package main

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"agilelimiter"
)

// response is one simulated round trip. Hit separates the two populations a
// CDN splits traffic into: a hit answers from the edge and consumes none of
// the origin's capacity, a miss pays whatever the origin costs (or fails when
// the origin is down). Outcome is what the integration should feed back to
// the controller; Good is scored against the SLO for goodput.
type response struct {
	Dur     time.Duration
	Hit     bool
	Outcome agilelimiter.Outcome
	Good    bool
}

// responder is the downstream model the load driver calls once per admitted
// request. offered is the rate the limiter is currently aiming at (so the
// M/M/1 utilisation reflects demand, not a lagging measurement) and elapsed is
// the time since the run started, which selects the capacity phase.
type responder interface {
	Respond(offered float64, elapsed time.Duration, seq uint64) response
	SLO() time.Duration
}

// capacityStep is one breakpoint of a capacity schedule: from startSec until
// the next breakpoint, the origin capacity is capacity (req/s). A capacity of
// zero means the origin is down for that span. The first step must start at
// second 0, so the schedule is continuous by construction: there are no gaps,
// no overlaps and no "before the first phase" case to get wrong.
type capacityStep struct {
	startSec int
	capacity float64
}

// downstreamParams is the parsed model, kept in Store for readers.
type downstreamParams struct {
	Base              time.Duration
	Capacity          float64
	Curve             string
	SLO               time.Duration
	CacheHit          float64
	CacheLatency      time.Duration
	OriginUp          bool
	OriginDownLatency time.Duration
}

// downstreamModel is the simulated dependency. It is deliberately small: the
// interesting behaviour (queueing below capacity, a cache in front of the
// origin, an origin outage) is expressed as one latency per request rather
// than as real goroutines, so the harness measures the limiter instead of
// itself.
type downstreamModel struct {
	Base              time.Duration
	Capacity          float64
	Steps             []capacityStep
	Curve             func(float64) float64
	CurveName         string
	SLOValue          time.Duration
	CacheHit          float64
	CacheLatency      time.Duration
	OriginUp          bool
	OriginDownLatency time.Duration
}

func (m *downstreamModel) SLO() time.Duration { return m.SLOValue }

// capacityAt returns the origin capacity at elapsed, honouring the optional
// breakpoint schedule. Each step holds from its second until the next step;
// the last step holds forever. A schedule with no steps is the constant
// Capacity. The first step is required to be at second 0, so sec always
// matches at least one step.
func (m *downstreamModel) capacityAt(elapsed time.Duration) float64 {
	if len(m.Steps) == 0 {
		return m.Capacity
	}
	sec := elapsed.Seconds()
	capacity := m.Steps[0].capacity
	for _, s := range m.Steps {
		if sec < float64(s.startSec) {
			break
		}
		capacity = s.capacity
	}
	return capacity
}

// Respond models one request. The cache decision comes first because it
// determines what, if anything, reaches the origin: a hit never does, so it
// neither pays the origin's queueing latency nor consumes its capacity. That
// distinction is the whole point of the cache scenarios.
func (m *downstreamModel) Respond(offered float64, elapsed time.Duration, seq uint64) response {
	hit := m.CacheHit > 0 && rand.Float64() < m.CacheHit
	if hit {
		return response{
			Dur:     m.CacheLatency,
			Hit:     true,
			Outcome: agilelimiter.Aborted, // cheap path: no evidence about capacity
			Good:    m.CacheLatency <= m.SLOValue,
		}
	}
	if !m.OriginUp {
		// The origin is down: the miss fails without doing work, so its round
		// trip is not a latency signal. Unreachable makes the controller back
		// off toward MinRate instead of reading a fast error as health.
		return response{
			Dur:     m.OriginDownLatency,
			Outcome: agilelimiter.Unreachable,
			Good:    false,
		}
	}
	capacity := m.capacityAt(elapsed)
	if capacity <= 0 {
		// A zero-capacity step means the origin is down: the miss cannot be
		// served, and like the origin-up=false case its latency is not a
		// signal about queueing, so it is reported as Unreachable.
		return response{
			Dur:     m.OriginDownLatency,
			Outcome: agilelimiter.Unreachable,
			Good:    false,
		}
	}
	missRate := offered * (1 - m.CacheHit)
	util := missRate / capacity
	d := time.Duration(float64(m.Base) * m.Curve(util))
	return response{
		Dur:     d,
		Outcome: agilelimiter.Reached,
		Good:    d <= m.SLOValue,
	}
}

// downstreamPlugin builds the model from options. It only provides the
// responder; any driving of load belongs to --load or a self-contained h*
// scenario.
type downstreamPlugin struct{}

func (downstreamPlugin) Name() string { return "downstream" }
func (downstreamPlugin) Desc() string {
	return "simulated downstream: capacity, queueing curve, cache and origin state"
}

func (downstreamPlugin) Options() []Option {
	return []Option{
		{Name: "base", Default: "20ms", Help: "idle latency when the origin is healthy"},
		{Name: "capacity", Default: "150", Help: "origin capacity (req/s) for misses"},
		{Name: "curve", Default: "mm1", Help: "queueing curve: mm1/flat/sharp"},
		{Name: "slo", Default: "100ms", Help: "SLO a request is scored against"},
		{Name: "cache", Default: "0", Help: "cache hit ratio in [0,1]; hits skip the origin"},
		{Name: "cache-latency", Default: "1ms", Help: "latency of a cache hit"},
		{Name: "origin-up", Default: "true", Help: "when false, misses fail as Unreachable"},
		{Name: "origin-down-latency", Default: "500ms", Help: "round trip of a miss while the origin is down"},
		{Name: "phases", Default: "", Help: "capacity steps: sec,capacity;... from 0; last holds (0 = origin down)"},
	}
}

func (downstreamPlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	opts := ParseOptions(args)

	base, err := GetDuration(opts, "base", 20*time.Millisecond)
	if err != nil {
		return err
	}
	capacity, err := GetFloat(opts, "capacity", 150)
	if err != nil {
		return err
	}
	if capacity < 0 || math.IsNaN(capacity) || math.IsInf(capacity, 0) {
		return fmt.Errorf("option %q: must be a finite non-negative number", "capacity")
	}
	curveName := strings.ToLower(GetString(opts, "curve", "mm1"))
	curve, err := curveFn(curveName)
	if err != nil {
		return err
	}
	slo, err := GetDuration(opts, "slo", 100*time.Millisecond)
	if err != nil {
		return err
	}
	cacheHit, err := GetFloat(opts, "cache", 0)
	if err != nil {
		return err
	}
	if cacheHit < 0 || cacheHit > 1 {
		return fmt.Errorf("option %q: must be within [0,1]", "cache")
	}
	cacheLatency, err := GetDuration(opts, "cache-latency", time.Millisecond)
	if err != nil {
		return err
	}
	originUp, err := GetBool(opts, "origin-up", true)
	if err != nil {
		return err
	}
	originDownLatency, err := GetDuration(opts, "origin-down-latency", 500*time.Millisecond)
	if err != nil {
		return err
	}
	steps, err := parseCapacitySteps(GetString(opts, "phases", ""))
	if err != nil {
		return err
	}

	model := &downstreamModel{
		Base: base, Capacity: capacity, Steps: steps,
		Curve: curve, CurveName: curveName, SLOValue: slo,
		CacheHit: cacheHit, CacheLatency: cacheLatency,
		OriginUp: originUp, OriginDownLatency: originDownLatency,
	}
	Provide(rt.Store, "downstream", "responder", responder(model))
	Provide(rt.Store, "downstream", "params", downstreamParams{
		Base: base, Capacity: capacity, Curve: curveName, SLO: slo,
		CacheHit: cacheHit, CacheLatency: cacheLatency,
		OriginUp: originUp, OriginDownLatency: originDownLatency,
	})
	fmt.Fprintf(rt.Out, "[downstream] base=%s capacity=%.0f/s curve=%s slo=%s cache=%.0f%% origin-up=%v\n",
		base, capacity, curveName, slo, cacheHit*100, originUp)
	return nil
}

// curveFn maps a utilisation in [0,1+] to a latency multiplier. mm1 is the
// classic 1/(1-u) queueing curve; sharp keeps latency flat until the origin is
// nearly saturated, which is the shape that exposed the knee in the existing
// sweep; flat has no queueing at all and is used by the pollution scenarios.
func curveFn(name string) (func(float64) float64, error) {
	switch name {
	case "mm1":
		return func(util float64) float64 {
			if util > 0.95 {
				util = 0.95
			}
			if util < 0 {
				util = 0
			}
			return 1 / (1 - util)
		}, nil
	case "sharp":
		return func(util float64) float64 {
			if util > 0.999 {
				util = 0.999
			}
			if util < 0 {
				util = 0
			}
			return 1 / (1 - pow8(util))
		}, nil
	case "flat":
		return func(float64) float64 { return 1 }, nil
	}
	return nil, fmt.Errorf("option %q: unsupported value %q", "curve", name)
}

func pow8(u float64) float64 {
	u *= u       // u^2
	u *= u       // u^4
	return u * u // u^8
}

// parseCapacitySteps parses "sec,capacity;sec,capacity;...". Each entry is a
// breakpoint: its capacity holds from that second until the next breakpoint,
// and the last one holds forever. The first breakpoint must be at second 0, so
// the schedule is always continuous. An empty string means a single constant
// capacity (the model's Capacity field).
//
// Capacity 0 is allowed: it means the origin is down for that span. Negative,
// NaN and infinite capacities are rejected.
func parseCapacitySteps(s string) ([]capacityStep, error) {
	if s == "" {
		return nil, nil
	}
	var steps []capacityStep
	for i, seg := range strings.Split(s, ";") {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		parts := strings.Split(seg, ",")
		if len(parts) != 2 {
			return nil, fmt.Errorf("phase %d: expected \"sec,capacity\", got %d fields", i, len(parts))
		}
		sec, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, fmt.Errorf("phase %d: invalid second %q", i, parts[0])
		}
		capacity, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		if err != nil {
			return nil, fmt.Errorf("phase %d: invalid capacity %q", i, parts[1])
		}
		if sec < 0 {
			return nil, fmt.Errorf("phase %d: second must not be negative", i)
		}
		if capacity < 0 || math.IsNaN(capacity) || math.IsInf(capacity, 0) {
			return nil, fmt.Errorf("phase %d: capacity must be a finite non-negative number", i)
		}
		if n := len(steps); n > 0 && sec <= steps[n-1].startSec {
			return nil, fmt.Errorf("phase %d: seconds must be strictly increasing (%d after %d)", i, sec, steps[n-1].startSec)
		}
		steps = append(steps, capacityStep{startSec: sec, capacity: capacity})
	}
	if len(steps) == 0 {
		return nil, nil
	}
	if steps[0].startSec != 0 {
		return nil, fmt.Errorf("phases must start at second 0, e.g. \"0,150;4,60\" (got %d,...)", steps[0].startSec)
	}
	return steps, nil
}
