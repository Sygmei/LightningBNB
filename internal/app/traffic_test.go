package app

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Sygmei/LightningBNB/internal/traffic"
)

func TestFormatTraffic(t *testing.T) {
	got := formatTraffic(
		traffic.Snapshot{TX: 1536, RX: 4096},
		traffic.Snapshot{TX: 512, RX: 2048},
		2*time.Second,
	)
	want := "tx=1.5 KiB rx=4.0 KiB tx-rate=512 B/s rx-rate=1.0 KiB/s"
	if got != want {
		t.Fatalf("formatTraffic() = %q, want %q", got, want)
	}
}

func TestTrafficReporterPrintsFinalTotals(t *testing.T) {
	var counter traffic.Counter
	counter.AddTX(2048)
	counter.AddRX(512)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var message string
	runTrafficReporter(ctx, time.Hour, &counter, func(current, previous traffic.Snapshot, elapsed time.Duration, final bool) {
		if !final {
			t.Fatal("canceled reporter did not produce a final report")
		}
		message = "traffic final " + formatTraffic(current, previous, elapsed)
	})
	if !strings.Contains(message, "traffic final tx=2.0 KiB rx=512 B") {
		t.Fatalf("final stats = %q", message)
	}
}
