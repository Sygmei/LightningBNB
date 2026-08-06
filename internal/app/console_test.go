package app

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/Sygmei/LightningBNB/internal/link"
	"github.com/Sygmei/LightningBNB/internal/traffic"
)

func TestRuntimeConsoleUsesLineLogsWhenTUICannotBeUsed(t *testing.T) {
	var output bytes.Buffer
	console := newRuntimeConsole(&output, false)
	console.ReportTraffic(
		traffic.Snapshot{
			TX:                 2048,
			RX:                 512,
			CompressionEnabled: true,
			TXUncompressed:     2048,
			TXCompressed:       1024,
		},
		traffic.Snapshot{TX: 1024, RX: 0},
		time.Second,
		false,
	)

	got := output.String()
	if !strings.Contains(got, "traffic tx=2.0 KiB rx=512 B tx-rate=1.0 KiB/s rx-rate=512 B/s\n") {
		t.Fatalf("plain traffic output = %q", got)
	}
	if strings.Contains(got, "\x1b[") {
		t.Fatalf("plain traffic output contains ANSI control sequences: %q", got)
	}
	if strings.Contains(got, "uncompressed") {
		t.Fatalf("plain traffic output unexpectedly changed for compression stats: %q", got)
	}
}

func TestRuntimeConsoleUpdatesDashboardInPlace(t *testing.T) {
	var output bytes.Buffer
	console := newRuntimeConsole(&output, true)
	console.color = false

	console.ReportTraffic(
		traffic.Snapshot{TX: 2048, RX: 512},
		traffic.Snapshot{},
		time.Second,
		false,
	)
	console.ReportTraffic(
		traffic.Snapshot{TX: 4096, RX: 1536},
		traffic.Snapshot{TX: 2048, RX: 512},
		time.Second,
		false,
	)
	console.Logger().Printf("BLE client connected")
	console.ReportTraffic(
		traffic.Snapshot{TX: 4096, RX: 1536},
		traffic.Snapshot{TX: 4096, RX: 1536},
		time.Second,
		true,
	)
	if err := console.Close(); err != nil {
		t.Fatal(err)
	}

	got := output.String()
	for _, want := range []string{
		"LightningBNB traffic · LIVE",
		"LightningBNB traffic · FINAL",
		"TX  total 4.0 KiB",
		"RX  total 1.5 KiB",
		"lightningbnb:",
		"BLE client connected",
		"\r\x1b[2K",
		"\x1b[1A",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("dashboard output does not contain %q: %q", want, got)
		}
	}
	if strings.Contains(got, "traffic tx=") {
		t.Fatalf("dashboard also emitted line-oriented traffic logs: %q", got)
	}
	if strings.Contains(got, "uncompressed") {
		t.Fatalf("dashboard showed compression rows before compression was enabled: %q", got)
	}
	if !strings.HasSuffix(got, "\n") {
		t.Fatalf("closed dashboard did not leave the cursor on a fresh line: %q", got)
	}
}

func TestTrafficDashboardHasAlignedBorders(t *testing.T) {
	console := newRuntimeConsole(&bytes.Buffer{}, true)
	console.color = false
	console.peakRate = 2048
	console.peakCombinedRate = 3072

	lines := console.trafficLines(traffic.Snapshot{
		TX:                 4096,
		RX:                 1536,
		CompressionEnabled: true,
		TXUncompressed:     4096,
		TXCompressed:       1024,
		RXUncompressed:     1536,
		RXCompressed:       768,
	}, 2048, 1024, false)
	wantWidth := len([]rune(lines[0]))
	for index, line := range lines[1:] {
		if got := len([]rune(line)); got != wantWidth {
			t.Errorf("line %d has width %d, want %d: %q", index+2, got, wantWidth, line)
		}
	}
}

func TestTrafficDashboardShowsCompressionTotals(t *testing.T) {
	console := newRuntimeConsole(&bytes.Buffer{}, true)
	console.color = false
	lines := strings.Join(console.trafficLines(traffic.Snapshot{
		CompressionEnabled: true,
		TXUncompressed:     4096,
		TXCompressed:       1024,
		RXUncompressed:     2048,
		RXCompressed:       1024,
	}, 0, 0, false), "\n")

	for _, want := range []string{
		"TX  uncompressed 4.0 KiB    compressed 1.0 KiB    saved   75.0%",
		"RX  uncompressed 2.0 KiB    compressed 1.0 KiB    saved   50.0%",
	} {
		if !strings.Contains(lines, want) {
			t.Errorf("compression dashboard does not contain %q: %q", want, lines)
		}
	}
}

func TestTrafficDashboardShowsLinkHealthDot(t *testing.T) {
	console := newRuntimeConsole(&bytes.Buffer{}, true)
	console.color = false
	console.ReportTraffic(traffic.Snapshot{}, traffic.Snapshot{}, time.Second, false)
	console.ReportLinkHealth(link.TransportSnapshot{Bound: true})
	lines := strings.Join(console.lines, "\n")
	if !strings.Contains(lines, "● link HEALTHY") {
		t.Fatalf("healthy link indicator = %q", lines)
	}
	console.ReportLinkHealth(link.TransportSnapshot{Bound: true, HeartbeatPending: true})
	if !strings.Contains(strings.Join(console.lines, "\n"), "● link CHECKING") {
		t.Fatalf("checking link indicator = %q", strings.Join(console.lines, "\n"))
	}
	console.ReportLinkHealth(link.TransportSnapshot{})
	if !strings.Contains(strings.Join(console.lines, "\n"), "● link OFFLINE") {
		t.Fatalf("offline link indicator = %q", strings.Join(console.lines, "\n"))
	}
	console.ReportLinkAndBufferFor(nil, link.TransportSnapshot{Bound: true}, 2, 1, 5, 4096)
	if got := strings.Join(console.lines, "\n"); !strings.Contains(got, "BUF queued 2    opening 1    active 5    data 4.0 KiB") {
		t.Fatalf("buffer indicator = %q", got)
	}
	if got := strings.Join(console.lines, "\n"); !strings.Contains(got, "● link DEGRADED") {
		t.Fatalf("degraded link indicator = %q", got)
	}
}

func TestRuntimeConsoleDisablesDashboardColorWithNoColor(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	console := newRuntimeConsole(&bytes.Buffer{}, true)
	if console.color {
		t.Fatal("dashboard color enabled despite NO_COLOR")
	}
}
