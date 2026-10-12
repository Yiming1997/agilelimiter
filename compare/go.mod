module compare

go 1.22

replace agilelimiter => ../

replace go.uber.org/ratelimit => ./uberratelimit

replace github.com/benbjohnson/clock => ./thirdparty/clock

require (
	agilelimiter v0.0.0
	go.uber.org/ratelimit v0.3.1
)

require github.com/benbjohnson/clock v1.3.0 // indirect
