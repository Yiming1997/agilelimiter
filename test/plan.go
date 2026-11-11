package main

import "fmt"

// Segment is one split slice: Name is the plugin name, Args are the raw
// segment arguments (still unparsed; parsing happens inside the plugin's
// Run, since some plugins want positional words or custom syntax).
type Segment struct {
	Name string
	Args []string
}

// Plan expands the requested segments into the final execution order.
//
// Two deliberate asymmetries:
//
//  1. A missing dependency is auto-inserted (with empty args, i.e. defaults)
//     before its dependent, so "--load ..." alone runs limiter+downstream.
//     But an explicitly written dependency placed after its dependent is an
//     error, never silently reordered: the user's segment order is intent
//     (log ordering, profile range), and breaking it is worse than an error.
//  2. Planning is a DFS over the user order inserting deps in front, not a
//     global topological sort, which would reorder everything the user wrote.
//
// placed tracks settled segments; visiting is the recursion stack for cycle
// detection (a dependency cycle has no legal order whether the segments were
// explicit or auto-inserted). Duplicate segment names are rejected up front:
// one plugin has a single Store namespace and a single lifecycle, so
// duplicates have no defined semantics.
func Plan(segs []Segment, reg *Registry) ([]Segment, error) {
	requested := make(map[string]bool, len(segs))
	argsOf := make(map[string][]string, len(segs))
	for _, s := range segs {
		if requested[s.Name] {
			return nil, fmt.Errorf("plan: duplicate segment --%s", s.Name)
		}
		requested[s.Name] = true
		// Remember each segment's own args; auto-inserted segments are not
		// in the map and get the zero value (empty args = defaults).
		argsOf[s.Name] = s.Args
	}

	placed := make(map[string]bool, len(segs))
	visiting := make(map[string]bool)
	var order []Segment
	var visit func(name string) error
	visit = func(name string) error {
		if placed[name] {
			return nil
		}
		if visiting[name] {
			return fmt.Errorf("plan: dependency cycle detected at --%s", name)
		}
		visiting[name] = true

		p, ok := reg.Lookup(name)
		if !ok {
			return fmt.Errorf("plan: unregistered plugin %q", name)
		}
		if d, ok := p.(Depender); ok {
			for _, dep := range d.Deps() {
				if placed[dep] {
					continue
				}
				// Requested but not yet placed: the user wrote it after this
				// segment — a misorder, not a miss, so report it.
				if requested[dep] {
					return fmt.Errorf("plan: dependency --%s of --%s must be ordered before it", dep, name)
				}
				// Not requested: auto-insert the dependency (recursing so its
				// own deps are inserted first).
				if err := visit(dep); err != nil {
					return err
				}
			}
		}

		visiting[name] = false
		order = append(order, Segment{Name: name, Args: argsOf[name]})
		placed[name] = true
		return nil
	}
	for _, s := range segs {
		if err := visit(s.Name); err != nil {
			return nil, err
		}
	}
	return order, nil
}
