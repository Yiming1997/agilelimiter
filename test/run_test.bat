@echo off
setlocal
rem Regression scenarios for the agilelimiter performance harness.
rem Each line is one plugin composition; the last line is the CDN scenario,
rem which is expected to FAIL until cache hits stop consuming a rate slot.

echo === steady downstream ===
go run . --downstream base=20ms capacity=150 --load workers=256 settle=3s dur=6s
if errorlevel 1 exit /b 1

echo === capacity step 150 -> 60 ===
go run . --downstream base=20ms phases=0,4,150;4,6,60 --load workers=256 settle=4s dur=6s
if errorlevel 1 exit /b 1

echo === latency-first preset with an SLO anchor ===
go run . --limiter preset=latency-first slo=100ms max=300 --downstream base=20ms capacity=150 --load workers=256 settle=3s dur=6s
if errorlevel 1 exit /b 1

echo === metrics + profile ===
go run . --downstream base=20ms capacity=150 --load workers=256 settle=2s dur=4s --metrics interval=1 format=csv file=metrics.csv --profile cpu=true mem=true
if errorlevel 1 exit /b 1

echo === CDN: origin down, cache hits must flow ===
go run . --hcache
if errorlevel 1 (
  echo [run_test] hcache FAILED: this is the known cache-slot issue in TODO.md
  exit /b 1
)

echo [run_test] all scenarios passed
