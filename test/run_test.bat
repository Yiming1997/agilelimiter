@echo off
:: Regression scenarios for the agilelimiter performance harness.
:: Each line is one plugin composition; the last line is the CDN scenario,
:: which is expected to FAIL until cache hits stop consuming a rate slot.

:: steady downstream 
go run . --downstream base=20ms capacity=150 --load workers=256 settle=3s dur=6s

:: capacity step 150 -> 60 
go run . --downstream base=20ms phases=0,150;4,60 --load workers=256 settle=4s dur=6s

:: latency-first preset with an SLO anchor 
go run . --limiter preset=latency-first slo=100ms max=300 --downstream base=20ms capacity=150 --load workers=256 settle=3s dur=6s

:: metrics + profile 
go run . --downstream base=20ms capacity=150 --load workers=256 settle=2s dur=4s --metrics interval=1 format=csv file=metrics.csv --profile cpu=true mem=true

:: CDN: origin down, cache hits must flow 
go run . --hcache