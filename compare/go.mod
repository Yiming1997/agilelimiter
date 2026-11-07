module compare

go 1.21

replace github.com/Yiming1997/agilelimiter => ../

replace go.uber.org/ratelimit => ./uberratelimit

replace github.com/benbjohnson/clock => ./thirdparty/clock

require (
	github.com/Yiming1997/agilelimiter v0.0.0
	go.uber.org/ratelimit v0.3.1
)

require github.com/benbjohnson/clock v1.3.0 // indirect
