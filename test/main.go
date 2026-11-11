package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// main only translates run's return value into the process exit code. run
	// keeps the whole flow on return values so main stays a thin shell.
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run is the host entry point and returns the process exit code:
// 0 = success, 1 = runtime error (a plugin Start/Run/End failed),
// 2 = usage error (bad command line). The split lets scripts distinguish
// "I mistyped the command" from "the run actually failed".
func run(argv []string, out, errOut io.Writer) int {
	// The plugin set is fixed at compile time (plugins.go), so a registry
	// error is a development error: fail immediately.
	reg, err := NewRegistry(allPlugins)
	if err != nil {
		fmt.Fprintf(errOut, "%v\n", err)
		return 1
	}

	// --list and --help are host built-ins, not plugins, so they bypass
	// ParseSegments. They are only recognized as the first token; anywhere
	// else they hit the "unknown segment" path instead of hijacking the run.
	if len(argv) > 0 {
		switch argv[0] {
		case "--list":
			if len(argv) > 1 {
				fmt.Fprintln(errOut, "usage: --list does not accept arguments")
				return 2
			}
			printList(out, reg)
			return 0
		case "--help":
			switch len(argv) {
			case 1:
				printUsage(out, reg)
			case 2:
				if err := printPluginHelp(out, reg, argv[1]); err != nil {
					fmt.Fprintf(errOut, "%v\n", err)
					return 2
				}
			default:
				fmt.Fprintln(errOut, "usage: --help [name]")
				return 2
			}
			return 0
		}
	}

	if len(argv) == 0 {
		fmt.Fprintln(errOut, "nothing to run; see --help")
		return 2
	}

	// Segment splitting and dependency planning are pure computation with no
	// side effects; doing them first guarantees no plugin starts when the
	// command line is invalid.
	segs, err := ParseSegments(argv, reg)
	if err != nil {
		fmt.Fprintf(errOut, "%v\n", err)
		return 2
	}
	plan, err := Plan(segs, reg)
	if err != nil {
		fmt.Fprintf(errOut, "%v\n", err)
		return 2
	}

	// The signal context only cancels ctx; cleanup is handled by the End
	// phase of execSession. defer stop() releases the NotifyContext handler.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// A fresh Runtime/Store per session: no mutable state is shared between
	// successive runs.
	rt := &Runtime{Out: out, Err: errOut, Store: NewStore()}
	return execSession(ctx, rt, plan, reg)
}

// execSession runs a planned session: Start (in order), then Run (in order),
// then End (in reverse). Any failure aborts the remaining steps with exit code
// 1, but already-started Lifecycle plugins still get their End called.
//
// End must still run after a failure: Start may have created resources
// (sampler goroutines, pprof, the limiter) that would leak otherwise. A plugin
// whose Start itself failed is not in started and owes no End.
//
// On error we stop instead of running the rest: the error usually means a
// precondition is gone (e.g. limiter creation failed, so load could not read
// it from the Store), and continuing would only stack meaningless errors on
// top of the first one.
func execSession(ctx context.Context, rt *Runtime, plan []Segment, reg *Registry) int {
	code := 0
	var started []string
	for _, s := range plan {
		p, _ := reg.Lookup(s.Name)
		lc, ok := p.(Lifecycle)
		if !ok {
			continue
		}
		if err := lc.Start(ctx, rt, s.Args); err != nil {
			fmt.Fprintf(rt.Err, "[%s] %v\n", s.Name, err)
			code = 1
			break
		}
		started = append(started, s.Name)
	}

	// Skip all Runs when a Start failed: the session never opened properly, so
	// Runs would see an incomplete Store.
	if code == 0 {
		for _, s := range plan {
			p, _ := reg.Lookup(s.Name)
			if err := p.Run(ctx, rt, s.Args); err != nil {
				fmt.Fprintf(rt.Err, "[%s] %v\n", s.Name, err)
				code = 1
				break
			}
		}
	}

	// Tear down in reverse Start order so resources close in the opposite
	// order of their creation. End errors are reported but only promoted to
	// the exit code when nothing failed before, keeping the first error visible.
	for i := len(started) - 1; i >= 0; i-- {
		p, _ := reg.Lookup(started[i])
		if err := p.(Lifecycle).End(ctx, rt); err != nil {
			fmt.Fprintf(rt.Err, "[%s] %v\n", started[i], err)
			if code == 0 {
				code = 1
			}
		}
	}
	return code
}
