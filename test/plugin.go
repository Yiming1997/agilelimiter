package main

import (
	"context"
	"io"
)

// Plugin interfaces follow the "minimal core + optional capabilities" design:
// a simple plugin only needs Name/Desc/Run. Help output, lifecycle management
// and dependency declarations are separate optional interfaces the host
// detects by type assertion (see execSession). A plugin file therefore makes
// its own role obvious and the host never needs to know concrete plugin types.

// Runtime is the only environment handle given to plugins. Out/Err are the
// global streams and Store carries session-level shared data. Everything is
// passed in (never package globals) so sessions stay isolated and testable.
type Runtime struct {
	Out   io.Writer
	Err   io.Writer
	Store *Store
}

// Plugin is the minimum contract. Name must be unique: the CLI uses it as the
// segment header ("--<name>") and the registry keys on it; a duplicate is
// rejected by NewRegistry.
type Plugin interface {
	Name() string
	Desc() string
	Run(ctx context.Context, rt *Runtime, args []string) error
}

// Option describes one key=value segment option for --help <name> output. It
// is metadata only and is not validated at runtime.
type Option struct {
	Name    string
	Default string
	Help    string
}

// Optioned declares the segment option table. The host probes it when
// printing help and omits the options block if it is not implemented.
type Optioned interface {
	Options() []Option
}

// Lifecycle is the session-scoped lifecycle. Start runs before every Run, in
// plan order, and End runs after every Run, in reverse order.
//
// Start takes args because envelope plugins (metrics, profile) must be in
// place before the session begins (sampler goroutine up, pprof started), and
// args only arrive with the segment. End takes no args: teardown never needs
// them since Start/Run already parsed what it requires.
type Lifecycle interface {
	Start(ctx context.Context, rt *Runtime, args []string) error
	End(ctx context.Context, rt *Runtime) error
}

// Depender declares which plugins must have run before this one. It is pure
// metadata consumed by the planner (auto-insertion, ordering checks, cycle
// detection); the Run code itself only reads dependency outputs from Store.
type Depender interface {
	Deps() []string
}
