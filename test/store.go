package main

import (
	"fmt"
	"sync"
)

// Store is the only data path between plugins: a two-level table of plugin
// name -> key -> value. Plugins do not know each other; a downstream plugin
// declares its Deps and reads what it needs here, and the plugin name in
// every call makes ownership explicit.
//
// Layering constraint (performance): Store only carries session-level objects
// (limiter handle, downstream model, configs) touched a handful of times per
// session, so the map/any boxing overhead is negligible. Per-request hot data
// (counters, samples) must stay in plugin-owned atomic state and never be
// written here — that would add a lock and a box to every increment.
type Store struct {
	mu   sync.RWMutex
	data map[string]map[string]any
}

// NewStore builds an empty Store; the host creates one per session.
func NewStore() *Store {
	return &Store{data: make(map[string]map[string]any)}
}

// Go does not allow type parameters on methods, so the generic accessors
// below are free functions.

// Provide stores v under plugin's namespace. Re-providing the same key
// overwrites: the latest parsed value wins.
func Provide[T any](s *Store, plugin, key string, v T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data[plugin] == nil {
		s.data[plugin] = make(map[string]any)
	}
	s.data[plugin][key] = v
}

// Get reads a value from an explicit namespace. A type mismatch (the stored
// type differs from T) reports ok=false instead of panicking: it is a broken
// plugin contract, better reported than crashing the process.
func Get[T any](s *Store, plugin, key string) (T, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var zero T
	if s.data[plugin] == nil {
		return zero, false
	}
	v, ok := s.data[plugin][key]
	if !ok {
		return zero, false
	}
	t, ok := v.(T)
	return t, ok
}

// Require reads a value from a dependency's namespace and errors when it is
// missing. Errors instead of a silent zero value matter for End phases: after
// an aborted Run an upstream may not have provided yet, and the "not
// provided" error is a readable signal of that timing issue.
func Require[T any](s *Store, dep, key string) (T, error) {
	v, ok := Get[T](s, dep, key)
	if !ok {
		var zero T
		return zero, fmt.Errorf("dependency %q: value %q not provided", dep, key)
	}
	return v, nil
}
