// Scratch diagnostic: watch the controller's baseline while the service is slow.
// This is the measurement that exposes the overshoot mechanism.
package main

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestDiagSlowBaseline(t *testing.T) {
	s := slowSteady()
	l, _ := newInboundAgile(s)
	defer l.Stop()

	var inflight atomic.Int64
	stop := make(chan struct{})
	for i := 0; i < s.clients; i++ {
		go func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if !l.TryTake() {
					time.Sleep(benchRefuseNs)
					continue
				}
				n := inflight.Add(1)
				begin := time.Now()
				time.Sleep(s.latencyAt(int(n)))
				d := time.Since(begin)
				inflight.Add(-1)
				l.Release()
				l.Observe(d)
			}
		}()
	}
	for i := 0; i < 40; i++ {
		time.Sleep(250 * time.Millisecond)
		st := l.Stats()
		high := st.Baseline * 2
		verdict := "hold"
		if st.Fast <= time.Duration(float64(high)*0.95) {
			verdict = "UP"
		} else if st.Fast >= high {
			verdict = "DOWN"
		}
		// The service's true latency at the current in-flight count, for contrast
		// with the quantised value the controller actually sees.
		trueLat := s.latencyAt(int(l.InFlight()))
		fmt.Printf("t=%5dms limit=%3d inflight=%3d baseline=%6s fast=%6s high=%6s true=%6s -> %s\n",
			(i+1)*250, l.Limit(), l.InFlight(),
			st.Baseline.Round(time.Millisecond), st.Fast.Round(time.Millisecond),
			high.Round(time.Millisecond), trueLat.Round(time.Millisecond), verdict)
	}
	close(stop)
}
