package main

// allPlugins is the central registration table, consumed by run() through
// NewRegistry. A single registration point is preferred over per-plugin init()
// self-registration: init ordering is compiler-determined and invisible, while
// one list makes the whole plugin set obvious. Adding a plugin means adding one
// line here.
//
// The bare framework has no plugins yet; the concrete plugin files and their
// registrations are added by the following change.
var allPlugins = []Plugin{}
