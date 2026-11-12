package main

import (
	"fmt"
	"io"
	"strings"
)

// ParseSegments splits argv into ordered Segments. The rule in one sentence:
// "--<registered plugin name>" opens a new segment wherever it appears; every
// other token belongs to the current segment and is passed through as-is.
//
// Recognizing heads by registered names (instead of position) is what makes
// composable long command lines possible. Everything else — including
// flag-style "-x"/"--<unregistered>" tokens — is kept verbatim and in order:
// flag parsing like "-T fixed --task-base 500" relies on adjacent flag/value
// pairs, so the framework must not reorder, merge or drop anything. The price
// is that a literal registered name cannot be expressed as a bare argument;
// embed it in a key=value if needed.
//
// Before any segment has opened, bare tokens and unregistered "--xxx" tokens
// are usage errors prompting for a segment head, so typos are not swallowed.
func ParseSegments(argv []string, reg *Registry) ([]Segment, error) {
	var segs []Segment
	var cur *Segment
	// flush is a closure shared by the "new segment head" path and the tail of
	// the loop to avoid duplicating the append logic.
	flush := func() {
		if cur != nil {
			segs = append(segs, *cur)
			cur = nil
		}
	}
	for _, tok := range argv {
		if strings.HasPrefix(tok, "--") {
			name := tok[2:]
			if _, ok := reg.Lookup(name); ok {
				flush()
				cur = &Segment{Name: name}
				continue
			}
			// Unregistered name: pass through inside a segment, but treat it
			// as an unknown head when no segment is open yet.
			if cur == nil {
				return nil, fmt.Errorf("usage: unknown segment --%s (see --list)", name)
			}
			cur.Args = append(cur.Args, tok)
			continue
		}
		// A bare token before any segment means the user expected arguments
		// to stand alone; point them at a segment head.
		if cur == nil {
			return nil, fmt.Errorf("usage: argument %q outside any segment (segment must start with --name)", tok)
		}
		cur.Args = append(cur.Args, tok)
	}
	flush()
	return segs, nil
}

// printUsage prints the global help (--help): the invocation forms followed
// by the plugin list, reusing printList so --help and --list cannot drift.
func printUsage(out io.Writer, reg *Registry) {
	fmt.Fprintln(out, "usage: agilelimiter_test [--list | --help [name]]")
	fmt.Fprintln(out, "       agilelimiter_test --pluginA key=value ... --pluginB key=value ...")
	fmt.Fprintln(out)
	printList(out, reg)
}

// printList prints one line per plugin. Deps and the lifecycle marker are
// shown because they are the information users need to compose command lines:
// auto-insertion visibility ("--load alone also runs limiter+downstream") and
// which segments take care of teardown.
func printList(out io.Writer, reg *Registry) {
	for _, name := range reg.Names() {
		p, _ := reg.Lookup(name)
		var deps string
		if d, ok := p.(Depender); ok && len(d.Deps()) > 0 {
			deps = "  (deps: --" + strings.Join(d.Deps(), ", --") + ")"
		}
		lc := ""
		if _, ok := p.(Lifecycle); ok {
			lc = "  [lifecycle]"
		}
		fmt.Fprintf(out, "  --%-12s %s%s%s\n", name, p.Desc(), deps, lc)
	}
}

// printPluginHelp prints one plugin's details (--help <name>). The name
// tolerates a leading "--" so output copied from --list pastes back cleanly.
// Errors are returned (not written) so run() decides the destination and exit
// code.
func printPluginHelp(out io.Writer, reg *Registry, name string) error {
	name = strings.TrimPrefix(name, "--")
	p, ok := reg.Lookup(name)
	if !ok {
		return fmt.Errorf("usage: unknown plugin %q (see --list)", name)
	}
	fmt.Fprintf(out, "--%s: %s\n", p.Name(), p.Desc())
	if o, ok := p.(Optioned); ok {
		fmt.Fprintln(out, "options:")
		for _, opt := range o.Options() {
			fmt.Fprintf(out, "  %-13s default: %-14s %s\n", opt.Name, opt.Default, opt.Help)
		}
	}
	return nil
}
