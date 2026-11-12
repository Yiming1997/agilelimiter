package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
)

// profile wraps the session in pprof. Start opens the CPU profile, End stops
// it and writes the heap profile. It must be a lifecycle plugin because
// profiling has to envelop the whole session (load driving and sampling
// included); writing the heap profile in End also captures the final memory
// state after the wait-exit window.
type profilePlugin struct {
	cpuFile *os.File
	mem     bool
}

func (profilePlugin) Name() string { return "profile" }
func (profilePlugin) Desc() string { return "pprof profiling enveloping the whole session" }

func (profilePlugin) Options() []Option {
	return []Option{
		{Name: "cpu", Default: "false", Help: "enable CPU profile (cpu_profile.prof)"},
		{Name: "mem", Default: "false", Help: "enable memory profile (mem_profile.prof)"},
	}
}

func (p *profilePlugin) Start(ctx context.Context, rt *Runtime, args []string) error {
	opts := ParseOptions(args)
	cpu, err := GetBool(opts, "cpu", false)
	if err != nil {
		return err
	}
	mem, err := GetBool(opts, "mem", false)
	if err != nil {
		return err
	}
	if !cpu && !mem {
		return fmt.Errorf("nothing to profile: specify cpu=true or mem=true")
	}
	p.mem = mem

	if cpu {
		f, err := os.Create("cpu_profile.prof")
		if err != nil {
			return fmt.Errorf("create cpu_profile.prof: %w", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			f.Close()
			return fmt.Errorf("start CPU profile: %w", err)
		}
		p.cpuFile = f
		fmt.Fprintln(rt.Out, "  CPU profile started -> cpu_profile.prof")
	}
	return nil
}

func (p *profilePlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	return nil
}

func (p *profilePlugin) End(ctx context.Context, rt *Runtime) error {
	if p.cpuFile != nil {
		pprof.StopCPUProfile()
		p.cpuFile.Close()
		fmt.Fprintln(rt.Out, "  CPU profile written to cpu_profile.prof")
	}
	if p.mem {
		runtime.GC()
		f, err := os.Create("mem_profile.prof")
		if err != nil {
			return fmt.Errorf("create mem_profile.prof: %w", err)
		}
		defer f.Close()
		if err := pprof.WriteHeapProfile(f); err != nil {
			return fmt.Errorf("write heap profile: %w", err)
		}
		fmt.Fprintln(rt.Out, "  Mem profile written to mem_profile.prof")
	}
	return nil
}
