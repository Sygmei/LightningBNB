package app

import (
	"context"
	"fmt"
	"time"

	"github.com/Sygmei/LightningBNB/internal/traffic"
)

type trafficReportFunc func(current, previous traffic.Snapshot, elapsed time.Duration, final bool)

func startTrafficReporter(parent context.Context, interval time.Duration, counter *traffic.Counter, report trafficReportFunc) func() {
	if interval <= 0 || counter == nil || report == nil {
		return func() {}
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runTrafficReporter(ctx, interval, counter, report)
	}()
	return func() {
		cancel()
		<-done
	}
}

func runTrafficReporter(ctx context.Context, interval time.Duration, counter *traffic.Counter, report trafficReportFunc) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	previous := counter.Snapshot()
	previousAt := time.Now()
	for {
		select {
		case now := <-ticker.C:
			current := counter.Snapshot()
			report(current, previous, now.Sub(previousAt), false)
			previous = current
			previousAt = now
		case <-ctx.Done():
			now := time.Now()
			current := counter.Snapshot()
			report(current, previous, now.Sub(previousAt), true)
			return
		}
	}
}

func formatTraffic(current, previous traffic.Snapshot, elapsed time.Duration) string {
	txDelta := delta(current.TX, previous.TX)
	rxDelta := delta(current.RX, previous.RX)
	seconds := elapsed.Seconds()
	if seconds <= 0 {
		seconds = 1
	}
	return fmt.Sprintf(
		"tx=%s rx=%s tx-rate=%s/s rx-rate=%s/s",
		formatBytes(float64(current.TX)),
		formatBytes(float64(current.RX)),
		formatBytes(float64(txDelta)/seconds),
		formatBytes(float64(rxDelta)/seconds),
	)
}

func delta(current, previous uint64) uint64 {
	if current < previous {
		return 0
	}
	return current - previous
}

func formatBytes(bytes float64) string {
	units := [...]string{"B", "KiB", "MiB", "GiB", "TiB"}
	unit := 0
	for bytes >= 1024 && unit < len(units)-1 {
		bytes /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%.0f %s", bytes, units[unit])
	}
	return fmt.Sprintf("%.1f %s", bytes, units[unit])
}
