package main

import (
	"fmt"
	"sort"
)

// Registry indexes plugins by name. A registry is needed because two parts
// of the host resolve by name: the segment splitter has to know whether a
// "--xxx" token is a valid segment head, and the planner has to read the
// dependency metadata of plugins. Both then work with names only.
type Registry struct {
	byName map[string]Plugin
}

// NewRegistry builds the index and validates two registration-time
// invariants: plugin names must be unique (a duplicate would make segment
// headers ambiguous), and every declared dependency must be a registered
// plugin. Ordering and cycles are not checked here — they depend on the
// requested segment sequence and are left to Plan.
func NewRegistry(plugins []Plugin) (*Registry, error) {
	r := &Registry{byName: make(map[string]Plugin, len(plugins))}
	for _, p := range plugins {
		if _, dup := r.byName[p.Name()]; dup {
			return nil, fmt.Errorf("registry: duplicate plugin %q", p.Name())
		}
		r.byName[p.Name()] = p
	}
	for name, p := range r.byName {
		if d, ok := p.(Depender); ok {
			for _, dep := range d.Deps() {
				if _, ok := r.byName[dep]; !ok {
					return nil, fmt.Errorf("registry: plugin %q depends on unregistered plugin %q", name, dep)
				}
			}
		}
	}
	return r, nil
}

// Lookup finds a plugin by name; callers phrase their own errors.
func (r *Registry) Lookup(name string) (Plugin, bool) {
	p, ok := r.byName[name]
	return p, ok
}

// Names returns plugin names sorted for stable --list output.
func (r *Registry) Names() []string {
	names := make([]string, 0, len(r.byName))
	for n := range r.byName {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
