package app

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Sygmei/LightningBNB/internal/traffic"
)

func TestBenchmarkUploadHandshakeAndCounters(t *testing.T) {
	server, client := net.Pipe()
	var counter traffic.Counter
	done := make(chan error, 1)
	go func() {
		done <- handleBenchmarkConnection(context.Background(), server, &counter)
	}()

	if err := writeFull(client, benchmarkHeader(benchmarkUpload)); err != nil {
		t.Fatal(err)
	}
	ready := []byte{0}
	if _, err := io.ReadFull(client, ready); err != nil {
		t.Fatal(err)
	}
	if ready[0] != benchmarkReady {
		t.Fatalf("ready marker = %d", ready[0])
	}
	if err := writeFull(client, []byte{benchmarkStart}); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("benchmark"), 127)
	if err := writeFull(client, payload); err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := counter.Snapshot(); got != (traffic.Snapshot{RX: uint64(len(payload))}) {
		t.Fatalf("traffic = %+v", got)
	}
}

func TestBenchmarkRejectsInvalidHandshake(t *testing.T) {
	server, client := net.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- handleBenchmarkConnection(context.Background(), server, &traffic.Counter{})
	}()
	if err := writeFull(client, []byte("NOTBENCH!")); err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	if err := <-done; err == nil || !strings.Contains(err.Error(), "invalid benchmark handshake") {
		t.Fatalf("handshake error = %v", err)
	}
}

func TestBenchmarkClientAndServerAllDirections(t *testing.T) {
	serverCtx, cancelServer := context.WithCancel(context.Background())
	defer cancelServer()
	addresses := make(chan string, 1)
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- RunBenchmarkServer(serverCtx, BenchmarkServerConfig{
			ListenHost:     "127.0.0.1",
			ListenPort:     0,
			MaxConnections: 4,
			StatsInterval:  0,
			Output:         &addressWriter{addresses: addresses},
		})
	}()
	address := <-addresses

	for _, direction := range []string{"upload", "download", "both"} {
		t.Run(direction, func(t *testing.T) {
			var diagnostics bytes.Buffer
			if err := RunBenchmarkClient(context.Background(), BenchmarkClientConfig{
				Address:       address,
				Direction:     direction,
				Duration:      50 * time.Millisecond,
				DialTimeout:   time.Second,
				Connections:   1,
				StatsInterval: time.Hour,
				ErrorOutput:   &diagnostics,
			}); err != nil {
				t.Fatal(err)
			}
			output := diagnostics.String()
			if !strings.Contains(output, "benchmark result") || !strings.Contains(output, "traffic final") {
				t.Fatalf("diagnostics = %q", output)
			}
			if direction != "download" && strings.Contains(output, "traffic final tx=0 B") {
				t.Fatalf("upload did not transfer data: %q", output)
			}
			if direction != "upload" && strings.Contains(output, "rx=0 B") {
				t.Fatalf("download did not transfer data: %q", output)
			}
		})
	}

	cancelServer()
	if err := <-serverErrors; err != nil {
		t.Fatal(err)
	}
}

type addressWriter struct {
	addresses chan<- string
	once      sync.Once
}

func (w *addressWriter) Write(data []byte) (int, error) {
	line := strings.TrimSpace(string(data))
	if strings.HasPrefix(line, "LISTEN_ADDR=") {
		w.once.Do(func() { w.addresses <- strings.TrimPrefix(line, "LISTEN_ADDR=") })
	}
	return len(data), nil
}

func TestParseBenchmarkDirection(t *testing.T) {
	for input, want := range map[string]benchmarkDirection{
		"upload":        benchmarkUpload,
		"tx":            benchmarkUpload,
		"download":      benchmarkDownload,
		"rx":            benchmarkDownload,
		"both":          benchmarkBoth,
		"bidirectional": benchmarkBoth,
	} {
		got, err := parseBenchmarkDirection(input)
		if err != nil || got != want {
			t.Fatalf("parseBenchmarkDirection(%q) = %d, %v", input, got, err)
		}
	}
	if _, err := parseBenchmarkDirection("sideways"); err == nil {
		t.Fatal("invalid direction was accepted")
	}
}
