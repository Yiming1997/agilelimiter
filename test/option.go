package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// These helpers form the shared syntax layer for segment arguments. The
// framework only splits key=value tokens and converts scalar types; it does
// not validate keys because only the plugin that implements an option knows
// its meaning. Accordingly every getter returns the default when the key is
// absent and errors only when a present value is malformed.

// ParseOptions converts key=value arguments into a map. Tokens without "="
// are positional words and are kept out of the map (see Positionals). A
// repeated key keeps the last value: segment options are order-free and
// duplicates usually come from script concatenation.
func ParseOptions(args []string) map[string]string {
	opts := make(map[string]string, len(args))
	for _, arg := range args {
		if k, v, ok := strings.Cut(arg, "="); ok {
			opts[k] = v
		}
	}
	return opts
}

// Positionals returns the bare tokens (no "=") of a segment, in command-line
// order, for plugins that support positional arguments.
func Positionals(args []string) []string {
	var pos []string
	for _, arg := range args {
		if _, _, ok := strings.Cut(arg, "="); !ok {
			pos = append(pos, arg)
		}
	}
	return pos
}

// GetString returns the option value or def when the key is absent.
func GetString(opts map[string]string, key, def string) string {
	if v, ok := opts[key]; ok {
		return v
	}
	return def
}

// Has reports whether the option was present at all. It matters for knobs
// whose zero value means "let the library choose": a caller must be able to
// say 0 explicitly and still have it behave as a default, while an absent key
// must not accidentally pin a zero where a preset would have filled one.
func Has(opts map[string]string, key string) bool {
	_, ok := opts[key]
	return ok
}

// GetInt parses the option as int, falling back to def when absent.
func GetInt(opts map[string]string, key string, def int) (int, error) {
	v, ok := opts[key]
	if !ok {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("option %q: invalid int %q", key, v)
	}
	return n, nil
}

// GetFloat parses the option as float64 (e.g. --load settle=0.5), falling
// back to def when absent.
func GetFloat(opts map[string]string, key string, def float64) (float64, error) {
	v, ok := opts[key]
	if !ok {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("option %q: invalid float %q", key, v)
	}
	return f, nil
}

// GetDuration parses the option as a time.Duration, falling back to def.
func GetDuration(opts map[string]string, key string, def time.Duration) (time.Duration, error) {
	v, ok := opts[key]
	if !ok {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("option %q: invalid duration %q", key, v)
	}
	return d, nil
}

// GetBool parses the option as bool, accepting true/1/on/yes and
// false/0/off/no, and falls back to def when absent.
func GetBool(opts map[string]string, key string, def bool) (bool, error) {
	v, ok := opts[key]
	if !ok {
		return def, nil
	}
	switch strings.ToLower(v) {
	case "true", "1", "on", "yes":
		return true, nil
	case "false", "0", "off", "no":
		return false, nil
	}
	return false, fmt.Errorf("option %q: invalid bool %q", key, v)
}
