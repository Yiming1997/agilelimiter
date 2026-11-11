package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"agilelimiter"
)

// limiterParams is the parsed limiter configuration, kept in Store for readers
// such as metrics and load. It is a plain value (not the live limiter) so a
// later reader can describe the run without touching the control loop.
type limiterParams struct {
	Kind    string
	Preset  string
	Min     int
	Max     int
	Initial int
	SLO     time.Duration
	Tick    time.Duration
}

// limiterHandle hides the difference between the outbound rate limiter and the
// inbound concurrency limiter. The load driver only needs five verbs, which is
// exactly what both types already offer under slightly different names.
//
// For the rate limiter Admit blocks until the bucket grants a slot (Take), so
// it always returns true. For the concurrency limiter Admit never blocks and
// returns false when the limit is reached (TryTake). Release is a no-op for
// the rate limiter because its slot is consumed by the grant, not held.
type limiterHandle interface {
	Kind() string
	Admit(ctx context.Context) bool
	Release()
	Observe(d time.Duration)
	Unavailable()
	Rate() float64
	Stats() agilelimiter.Stats
}

// --- outbound rate limiter ---

type rateHandle struct{ l *agilelimiter.Limiter }

func (h *rateHandle) Kind() string               { return "rate" }
func (h *rateHandle) Admit(context.Context) bool { h.l.Take(); return true }
func (h *rateHandle) Release()                   {}
func (h *rateHandle) Observe(d time.Duration)    { h.l.Observe(d) }
func (h *rateHandle) Unavailable()               { h.l.Unavailable() }
func (h *rateHandle) Rate() float64              { return h.l.Rate() }
func (h *rateHandle) Stats() agilelimiter.Stats  { return h.l.Stats() }

// --- inbound concurrency limiter ---

type concHandle struct {
	l *agilelimiter.ConcurrencyLimiter
}

func (h *concHandle) Kind() string               { return "concurrency" }
func (h *concHandle) Admit(context.Context) bool { return h.l.TryTake() }
func (h *concHandle) Release()                   { h.l.Release() }
func (h *concHandle) Observe(d time.Duration)    { h.l.Observe(d) }
func (h *concHandle) Unavailable()               { h.l.Unavailable() }
func (h *concHandle) Rate() float64              { return float64(h.l.Limit()) }
func (h *concHandle) Stats() agilelimiter.Stats  { return h.l.Stats() }

// limiterPlugin builds the limiter from its options and holds it until End
// stops the control loop. Construction happens in Start rather than Run: the
// load driver (and the metrics sampler) needs the handle to exist before any
// request is driven, and the Start phase precedes all Runs.
type limiterPlugin struct{}

func (limiterPlugin) Name() string { return "limiter" }
func (limiterPlugin) Desc() string { return "create the adaptive limiter, Stop on End" }

func (limiterPlugin) Options() []Option {
	return []Option{
		{Name: "kind", Default: "rate", Help: "limiter kind: rate (outbound) / concurrency (inbound)"},
		{Name: "preset", Default: "none", Help: "policy preset: none/balanced/latency-first/throughput-first"},
		{Name: "anchor", Default: "unset", Help: "congestion anchor: unset/baseline/slo"},
		{Name: "min", Default: "5", Help: "MinRate (or MinLimit): the outage probe rate"},
		{Name: "max", Default: "500", Help: "MaxRate (or MaxLimit): the promise ceiling"},
		{Name: "initial", Default: "0", Help: "InitialRate; 0 derives MaxRate/8"},
		{Name: "slo", Default: "100ms", Help: "SLOTarget; enables the tail guard"},
		{Name: "tick", Default: "250ms", Help: "control-loop period"},
		{Name: "fast-hl", Default: "0", Help: "FastHalfLife; 0 keeps the library default"},
		{Name: "slow-hl", Default: "0", Help: "SlowHalfLife; 0 keeps the library default"},
		{Name: "slow-down-hl", Default: "0", Help: "SlowDownHalfLife; 0 keeps the library default"},
		{Name: "max-baseline-rise", Default: "0", Help: "MaxBaselineRise; 0 keeps the library default"},
		{Name: "target-inflation", Default: "0", Help: "TargetInflation; 0 keeps the library default"},
		{Name: "gain", Default: "0", Help: "proportional backoff gain; 0 keeps the default"},
		{Name: "up-step", Default: "0", Help: "additive increase per tick; 0 keeps the default"},
		{Name: "down-step", Default: "0", Help: "max fractional decrease per tick; 0 keeps the default"},
		{Name: "slow-start-gain", Default: "0", Help: "multiplicative start gain; 0 keeps the default"},
		{Name: "deadband", Default: "0", Help: "hold band around the target; 0 keeps the default"},
		{Name: "tail-factor", Default: "0", Help: "p99 guard scales SLOTarget by this; 0 keeps the default"},
		{Name: "tail-tolerance", Default: "0", Help: "ratio-mode tail tolerance; 0 keeps quantile mode"},
		{Name: "backoff-ticks", Default: "0", Help: "consecutive slow ticks before a step; 0 keeps the default"},
		{Name: "recover-ticks", Default: "0", Help: "consecutive fast ticks before a step; 0 keeps the default"},
	}
}

func (limiterPlugin) Start(ctx context.Context, rt *Runtime, args []string) error {
	opts := ParseOptions(args)

	kind := strings.ToLower(GetString(opts, "kind", "rate"))
	if kind != "rate" && kind != "concurrency" {
		return fmt.Errorf("option %q: unsupported value %q", "kind", kind)
	}
	preset, err := parsePreset(GetString(opts, "preset", "none"))
	if err != nil {
		return err
	}
	anchor, err := parseAnchor(GetString(opts, "anchor", "unset"))
	if err != nil {
		return err
	}
	min, err := GetInt(opts, "min", 5)
	if err != nil {
		return err
	}
	max, err := GetInt(opts, "max", 500)
	if err != nil {
		return err
	}
	initial, err := GetInt(opts, "initial", 0)
	if err != nil {
		return err
	}
	slo, err := GetDuration(opts, "slo", 100*time.Millisecond)
	if err != nil {
		return err
	}
	tick, err := GetDuration(opts, "tick", 250*time.Millisecond)
	if err != nil {
		return err
	}
	fastHL, err := GetDuration(opts, "fast-hl", 0)
	if err != nil {
		return err
	}
	slowHL, err := GetDuration(opts, "slow-hl", 0)
	if err != nil {
		return err
	}
	slowDownHL, err := GetDuration(opts, "slow-down-hl", 0)
	if err != nil {
		return err
	}
	maxBaselineRise, err := GetFloat(opts, "max-baseline-rise", 0)
	if err != nil {
		return err
	}
	targetInflation, err := GetFloat(opts, "target-inflation", 0)
	if err != nil {
		return err
	}
	gain, err := GetFloat(opts, "gain", 0)
	if err != nil {
		return err
	}
	upStep, err := GetFloat(opts, "up-step", 0)
	if err != nil {
		return err
	}
	downStep, err := GetFloat(opts, "down-step", 0)
	if err != nil {
		return err
	}
	slowStartGain, err := GetFloat(opts, "slow-start-gain", 0)
	if err != nil {
		return err
	}
	deadband, err := GetFloat(opts, "deadband", 0)
	if err != nil {
		return err
	}
	tailFactor, err := GetFloat(opts, "tail-factor", 0)
	if err != nil {
		return err
	}
	tailTolerance, err := GetFloat(opts, "tail-tolerance", 0)
	if err != nil {
		return err
	}
	backoffTicks, err := GetInt(opts, "backoff-ticks", 0)
	if err != nil {
		return err
	}
	recoverTicks, err := GetInt(opts, "recover-ticks", 0)
	if err != nil {
		return err
	}

	var handle limiterHandle
	if kind == "rate" {
		cfg := agilelimiter.Config{
			Preset:           preset,
			Anchor:           anchor,
			MinRate:          min,
			MaxRate:          max,
			InitialRate:      initial,
			SLOTarget:        slo,
			Tick:             tick,
			FastHalfLife:     fastHL,
			SlowHalfLife:     slowHL,
			SlowDownHalfLife: slowDownHL,
			MaxBaselineRise:  maxBaselineRise,
			TargetInflation:  targetInflation,
			Gain:             gain,
			UpStep:           upStep,
			DownStep:         downStep,
			SlowStartGain:    slowStartGain,
			Deadband:         deadband,
			TailFactor:       tailFactor,
			TailTolerance:    tailTolerance,
			BackoffTicks:     backoffTicks,
			RecoverTicks:     recoverTicks,
		}
		if err := cfg.Validate(); err != nil {
			return err
		}
		l := agilelimiter.New(cfg)
		l.Start()
		handle = &rateHandle{l: l}
	} else {
		cc := agilelimiter.ConcurrencyConfig{
			Preset:           preset,
			Anchor:           anchor,
			MinLimit:         min,
			MaxLimit:         max,
			InitialLimit:     initial,
			SLOTarget:        slo,
			Tick:             tick,
			FastHalfLife:     fastHL,
			SlowHalfLife:     slowHL,
			SlowDownHalfLife: slowDownHL,
			MaxBaselineRise:  maxBaselineRise,
			TargetInflation:  targetInflation,
			Gain:             gain,
			UpStep:           upStep,
			DownStep:         downStep,
			SlowStartGain:    slowStartGain,
			Deadband:         deadband,
			TailFactor:       tailFactor,
			TailTolerance:    tailTolerance,
			BackoffTicks:     backoffTicks,
			RecoverTicks:     recoverTicks,
		}
		if err := cc.Validate(); err != nil {
			return err
		}
		l := agilelimiter.NewConcurrency(cc)
		l.Start()
		handle = &concHandle{l: l}
	}

	params := limiterParams{
		Kind: kind, Preset: preset.String(), Min: min, Max: max,
		Initial: initial, SLO: slo, Tick: tick,
	}
	Provide(rt.Store, "limiter", "handle", handle)
	Provide(rt.Store, "limiter", "params", params)
	fmt.Fprintf(rt.Out, "[limiter] %s min=%d max=%d initial=%d slo=%s tick=%s preset=%s anchor=%s\n",
		kind, min, max, initial, slo, tick, preset, anchorName(anchor))
	return nil
}

func (limiterPlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	return nil
}

func (limiterPlugin) End(ctx context.Context, rt *Runtime) error {
	h, ok := Get[limiterHandle](rt.Store, "limiter", "handle")
	if !ok {
		return nil
	}
	switch v := h.(type) {
	case *rateHandle:
		v.l.Stop()
	case *concHandle:
		v.l.Stop()
	}
	return nil
}

func parsePreset(s string) (agilelimiter.Preset, error) {
	switch strings.ToLower(s) {
	case "none", "":
		return agilelimiter.PresetNone, nil
	case "balanced":
		return agilelimiter.PresetBalanced, nil
	case "latency-first", "latency":
		return agilelimiter.PresetLatencyFirst, nil
	case "throughput-first", "throughput":
		return agilelimiter.PresetThroughputFirst, nil
	}
	return agilelimiter.PresetNone, fmt.Errorf("option %q: unsupported value %q", "preset", s)
}

func parseAnchor(s string) (agilelimiter.Anchor, error) {
	switch strings.ToLower(s) {
	case "unset", "":
		return agilelimiter.AnchorUnset, nil
	case "baseline":
		return agilelimiter.AnchorBaseline, nil
	case "slo":
		return agilelimiter.AnchorSLO, nil
	}
	return agilelimiter.AnchorUnset, fmt.Errorf("option %q: unsupported value %q", "anchor", s)
}

func anchorName(a agilelimiter.Anchor) string {
	switch a {
	case agilelimiter.AnchorBaseline:
		return "baseline"
	case agilelimiter.AnchorSLO:
		return "slo"
	default:
		return "unset"
	}
}
