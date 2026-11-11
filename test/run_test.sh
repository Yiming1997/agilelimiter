#!/usr/bin/env sh
# Regression scenarios for the agilelimiter performance harness.
# The last scenario (hcache) is expected to FAIL until cache hits stop
# consuming a rate slot.
set -eu

echo "=== steady downstream ==="
go run . --downstream base=20ms capacity=150 --load workers=256 settle=3s dur=6s

echo "=== capacity step 150 -> 60 ==="
go run . --downstream base=20ms phases="0,150;4,60" --load workers=256 settle=4s dur=6s

echo "=== latency-first preset with an SLO anchor ==="
go run . --limiter preset=latency-first slo=100ms max=300 --downstream base=20ms capacity=150 --load workers=256 settle=3s dur=6s

echo "=== metrics + profile ==="
go run . --downstream base=20ms capacity=150 --load workers=256 settle=2s dur=4s --metrics interval=1 format=csv file=metrics.csv --profile cpu=true mem=true

echo "=== CDN: origin down, cache hits must flow ==="
if go run . --hcache; then
  echo "[run_test] hcache passed"
else
  echo "[run_test] hcache FAILED: this is the known cache-slot issue in TODO.md"
  exit 1
fi
