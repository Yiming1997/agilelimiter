package main

// allPlugins is the central registration table, consumed by run() through
// NewRegistry. A single registration point is preferred over per-plugin init()
// self-registration: init ordering is compiler-determined and invisible, while
// one list makes the whole plugin set obvious. Adding a plugin means adding one
// line here.
//
// metrics/profile keep state across phases (Start parses and stores, End
// reads), so pointer instances are registered.
var allPlugins = []Plugin{
	limiterPlugin{},
	downstreamPlugin{},
	loadPlugin{},
	hcachePlugin{},
	&metricsPlugin{},
	&profilePlugin{},
}
