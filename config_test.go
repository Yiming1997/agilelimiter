package agilelimiter

import (
	"strings"
	"testing"
	"time"
)

// A config with no Preset must resolve exactly as it did before presets existed,
// and PresetBalanced must be the same thing spelled out.
func TestPresetNoneChangesNothing(t *testing.T) {
	none := New(Config{MaxRate: 120}).Config()
	balanced := New(Config{MaxRate: 120, Preset: PresetBalanced}).Config()

	// Everything but the label must match: the preset records what was asked,
	// and what was asked is the same law either way.
	none.Preset, balanced.Preset = "", ""
	if none != balanced {
		t.Fatalf("PresetBalanced must equal PresetNone:\n none:     %+v\n balanced: %+v", none, balanced)
	}
	if none.Tick != time.Second || none.FastHalfLife != 3*time.Second ||
		none.SlowHalfLife != 45*time.Second || none.Anchor != "baseline" ||
		none.BackoffTicks != 1 || none.RecoverTicks != 1 || none.RecoverRamp != 0 {
		t.Fatalf("shipped defaults changed: %+v", none)
	}
	if none.Window != "2.00x baseline" {
		t.Fatalf("Window = %q, want the relative window", none.Window)
	}
}

// A preset fills policy fields; anything the caller set explicitly wins.
func TestPresetFillsOnlyUnset(t *testing.T) {
	r := New(Config{
		MaxRate:   120,
		SLOTarget: 100 * time.Millisecond,
		Preset:    PresetLatencyFirst,
	}).Config()
	if r.Anchor != "slo" || r.BackoffTicks != 2 || r.RecoverTicks != 2 {
		t.Fatalf("latency-first did not fill: %+v", r)
	}
	if r.Window != "80ms (0.8 x SLOTarget)" {
		t.Fatalf("Window = %q", r.Window)
	}

	r = New(Config{
		MaxRate:      120,
		SLOTarget:    100 * time.Millisecond,
		Preset:       PresetLatencyFirst,
		BackoffTicks: 5,
		Anchor:       AnchorBaseline,
	}).Config()
	if r.BackoffTicks != 5 || r.Anchor != "baseline" {
		t.Fatalf("explicit fields must win over the preset: %+v", r)
	}
}

func TestPresetThroughputFirst(t *testing.T) {
	r := New(Config{MaxRate: 120, Preset: PresetThroughputFirst}).Config()
	if r.Anchor != "baseline" || r.TargetInflation != 2.5 ||
		r.BackoffTicks != 2 || r.RecoverTicks != 1 {
		t.Fatalf("throughput-first did not fill: %+v", r)
	}
	if r.Window != "3.50x baseline" {
		t.Fatalf("Window = %q", r.Window)
	}
}

// The window description has to name the cap, because that cap is one-way and is
// the reason an SLO can be silently ignored.
func TestResolvedDescribesCappedWindow(t *testing.T) {
	r := New(Config{MaxRate: 120, SLOTarget: 100 * time.Millisecond}).Config()
	if r.Window != "2.00x baseline, capped at 80ms" {
		t.Fatalf("Window = %q", r.Window)
	}
	if r.Anchor != "baseline" {
		t.Fatalf("Anchor = %q", r.Anchor)
	}
}

// An SLO anchor without an SLO is kept in the config so Config() can say so,
// instead of being rewritten into the baseline behind the caller's back.
func TestResolvedReportsAnchorFallback(t *testing.T) {
	r := New(Config{MaxRate: 120, Anchor: AnchorSLO}).Config()
	if !strings.Contains(r.Anchor, "without SLOTarget") {
		t.Fatalf("Anchor = %q, want the fallback to be visible", r.Anchor)
	}
	if r.Window != "2.00x baseline" {
		t.Fatalf("Window = %q, want the law that actually runs", r.Window)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string // substring the error must contain; "" means valid
	}{
		{"minimal", Config{MaxRate: 120}, ""},
		{
			"preset",
			Config{MaxRate: 120, SLOTarget: 100 * time.Millisecond, Preset: PresetLatencyFirst},
			"",
		},
		{"missing ceiling", Config{}, "MaxRate is required"},
		{"floor above ceiling", Config{MaxRate: 10, MinRate: 20}, "exceeds MaxRate"},
		{"initial above ceiling", Config{MaxRate: 10, InitialRate: 20}, "InitialRate"},
		{"slo anchor without slo", Config{MaxRate: 120, Anchor: AnchorSLO}, "needs SLOTarget"},
		{"tiny slo", Config{MaxRate: 120, SLOTarget: time.Millisecond}, "unit mistake"},
		{"deadband of one", Config{MaxRate: 120, Deadband: 1}, "Deadband"},
		{"downstep over one", Config{MaxRate: 120, DownStep: 1.2}, "DownStep"},
		{"tick too long", Config{MaxRate: 120, Tick: 30 * time.Second}, "Tick"},
		{"half life under tick", Config{MaxRate: 120, Tick: time.Second, FastHalfLife: time.Second}, "FastHalfLife"},
		{"recover ramp out of range", Config{MaxRate: 120, RecoverRamp: 2}, "RecoverRamp"},
		{"negative slow start", Config{MaxRate: 120, SlowStartGain: 0.5}, "SlowStartGain"},
	}

	for _, tc := range tests {
		err := tc.cfg.Validate()
		switch {
		case tc.want == "" && err != nil:
			t.Errorf("%s: unexpected error: %v", tc.name, err)
		case tc.want != "" && err == nil:
			t.Errorf("%s: expected an error", tc.name)
		case tc.want != "" && !strings.Contains(err.Error(), tc.want):
			t.Errorf("%s: error %q does not mention %q", tc.name, err, tc.want)
		}
	}
}

// The concurrency limiter is the same brain, so it must expose the same surface.
func TestConcurrencyPresetAndValidate(t *testing.T) {
	cc := ConcurrencyConfig{
		MaxLimit:  200,
		SLOTarget: 100 * time.Millisecond,
		Preset:    PresetLatencyFirst,
	}
	if err := cc.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	r := NewConcurrency(cc).Config()
	if r.Kind != "concurrency" || r.Anchor != "slo" || r.MaxRate != 200 || r.BackoffTicks != 2 {
		t.Fatalf("unexpected resolved config: %+v", r)
	}

	if err := (ConcurrencyConfig{MaxLimit: 0}).Validate(); err == nil {
		t.Fatal("a zero MaxLimit must be rejected")
	}
}
