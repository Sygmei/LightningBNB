package app

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Sygmei/LightningBNB/internal/mux"
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

func TestBenchmarkResponderRunsInsideMuxSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clientWire, serverWire := net.Pipe()
	clientMux := mux.NewClient(clientWire, 4)
	serverMux := mux.NewServer(serverWire, 4)
	defer clientMux.Close()
	defer serverMux.Close()
	var counter traffic.Counter
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- ServeBenchmarkStreams(ctx, serverMux, &counter, nil) }()

	stream, err := clientMux.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFull(stream, benchmarkHeader(benchmarkUpload)); err != nil {
		t.Fatal(err)
	}
	ready := []byte{0}
	if _, err := io.ReadFull(stream, ready); err != nil {
		t.Fatal(err)
	}
	if ready[0] != benchmarkReady {
		t.Fatalf("ready marker = %d", ready[0])
	}
	if err := writeFull(stream, []byte{benchmarkStart}); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("mux-benchmark"), 100)
	if err := writeFull(stream, payload); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseWrite(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for counter.Snapshot().RX != uint64(len(payload)) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := counter.Snapshot(); got.RX != uint64(len(payload)) {
		t.Fatalf("server traffic = %+v", got)
	}
	cancel()
	if err := <-serverErrors; err != nil {
		t.Fatal(err)
	}
}

func TestBenchmarkClientAndServerAllDirections(t *testing.T) {
	serverCtx, cancelServer := context.WithCancel(context.Background())
	defer cancelServer()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverErrors := make(chan error, 1)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if serverCtx.Err() != nil {
					serverErrors <- nil
				} else {
					serverErrors <- err
				}
				return
			}
			go func() { _ = handleBenchmarkConnection(serverCtx, conn, &traffic.Counter{}) }()
		}
	}()
	address := listener.Addr().String()

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
	_ = listener.Close()
	if err := <-serverErrors; err != nil {
		t.Fatal(err)
	}
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
