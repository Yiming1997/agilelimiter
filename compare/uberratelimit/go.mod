// This directory contains an unmodified copy of the go.uber.org/ratelimit
// source (MIT-licensed, © Uber Technologies), vendored here only so the
// comparison builds offline. The parent module replaces go.uber.org/ratelimit
// with this directory. See ../thirdparty/clock for the one dependency shim.
module go.uber.org/ratelimit

go 1.20

require github.com/benbjohnson/clock v1.3.0

replace github.com/benbjohnson/clock => ../thirdparty/clock
