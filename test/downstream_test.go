package main

import (
	"strings"
	"testing"
	"time"

	"agilelimiter"
)

func TestParseCapacitySteps(t *testing.T) {
	valid := []struct {
		name string
		in   string
		want []capacityStep
	}{
		{"empty means constant", "", nil},
		{"single step", "0,150", []capacityStep{{0, 150}}},
		{"two steps", "0,150;4,60", []capacityStep{{0, 150}, {4, 60}}},
		{"spaces are trimmed", " 0 , 150 ; 4 , 60 ", []capacityStep{{0, 150}, {4, 60}}},
		{"zero capacity is allowed", "0,150;4,0", []capacityStep{{0, 150}, {4, 0}}},
		{"empty segments are skipped", ";;0,150;;4,60;;", []capacityStep{{0, 150}, {4, 60}}},
	}
	for _, tt := range valid {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseCapacitySteps(tt.in)
			if err != nil {
				t.Fatalf("parseCapacitySteps(%q) = %v", tt.in, err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("parseCapacitySteps(%q) returned %d steps, want %d: %v", tt.in, len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("parseCapacitySteps(%q)[%d] = %+v, want %+v", tt.in, i, got[i], tt.want[i])
				}
			}
		})
	}

	invalid := []struct {
		name string
		in   string
		want string
	}{
		{"must start at zero", "4,60", "must start at second 0"},
		{"old three-field syntax", "0,4,150", `expected "sec,capacity"`},
		{"missing capacity", "0,150;4", `expected "sec,capacity"`},
		{"duplicate second", "0,150;0,60", "strictly increasing"},
		{"out of order", "0,150;4,60;2,30", "strictly increasing"},
		{"negative second", "-1,150", "must not be negative"},
		{"negative capacity", "0,-1", "finite non-negative"},
		{"nan capacity", "0,NaN", "finite non-negative"},
		{"infinite capacity", "0,Inf", "finite non-negative"},
		{"bad second", "x,150", "invalid second"},
		{"bad capacity", "0,x", "invalid capacity"},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseCapacitySteps(tt.in)
			if err == nil {
				t.Fatalf("parseCapacitySteps(%q) succeeded, want an error", tt.in)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("parseCapacitySteps(%q) error = %q, want it to contain %q", tt.in, err, tt.want)
			}
		})
	}
}

func TestCapacityAt(t *testing.T) {
	steps := []capacityStep{{0, 150}, {4, 60}}
	withZero := []capacityStep{{0, 150}, {4, 0}, {10, 30}}

	tests := []struct {
		name  string
		model downstreamModel
		at    time.Duration
		want  float64
	}{
		{"no schedule uses the constant", downstreamModel{Capacity: 150}, 10 * time.Second, 150},
		{"before the change", downstreamModel{Capacity: 999, Steps: steps}, 3999 * time.Millisecond, 150},
		{"at the change", downstreamModel{Steps: steps}, 4 * time.Second, 60},
		{"after the last step", downstreamModel{Steps: steps}, 100 * time.Second, 60},
		{"zero-capacity span", downstreamModel{Steps: withZero}, 5 * time.Second, 0},
		{"recovered span", downstreamModel{Steps: withZero}, 10 * time.Second, 30},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.model.capacityAt(tt.at); got != tt.want {
				t.Fatalf("capacityAt(%v) = %v, want %v", tt.at, got, tt.want)
			}
		})
	}
}

func TestRespondZeroCapacityMeansDown(t *testing.T) {
	curve, err := curveFn("mm1")
	if err != nil {
		t.Fatal(err)
	}
	m := &downstreamModel{
		Base:              20 * time.Millisecond,
		Capacity:          150,
		Steps:             []capacityStep{{0, 150}, {4, 0}},
		Curve:             curve,
		CurveName:         "mm1",
		SLOValue:          100 * time.Millisecond,
		OriginUp:          true,
		OriginDownLatency: 500 * time.Millisecond,
	}

	if r := m.Respond(100, time.Second, 1); r.Outcome != agilelimiter.Reached {
		t.Fatalf("healthy span outcome = %v, want Reached", r.Outcome)
	}
	r := m.Respond(100, 5*time.Second, 2)
	if r.Outcome != agilelimiter.Unreachable || r.Good || r.Dur != m.OriginDownLatency {
		t.Fatalf("zero-capacity miss = %+v, want Unreachable with the origin-down latency", r)
	}

	// A cache hit is still served by the edge; only the origin is down.
	m.CacheHit = 1
	r = m.Respond(100, 5*time.Second, 3)
	if !r.Hit || r.Outcome != agilelimiter.Aborted {
		t.Fatalf("cache hit at zero origin capacity = %+v, want an Aborted hit", r)
	}
}
