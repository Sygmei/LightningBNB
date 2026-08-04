package mux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Sygmei/LightningBNB/internal/protocol"
	"github.com/Sygmei/LightningBNB/internal/traffic"
)

func TestMultiplexedStreamsAreIsolated(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	client := NewClient(clientConn, 8)
	server := NewServer(serverConn, 8)
	defer client.Close()
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	serverErr := make(chan error, 1)
	go func() {
		for range 3 {
			stream, err := server.Accept(ctx)
			if err != nil {
				serverErr <- err
				return
			}
			if err := stream.Approve(); err != nil {
				serverErr <- err
				return
			}
			go func() { _, _ = io.Copy(stream, stream) }()
		}
		serverErr <- nil
	}()

	var wg sync.WaitGroup
	for _, value := range []string{"alpha", "beta", "gamma"} {
		value := value
		wg.Add(1)
		go func() {
			defer wg.Done()
			stream, err := client.Open(ctx)
			if err != nil {
				t.Errorf("open: %v", err)
				return
			}
			defer stream.Close()
			if _, err := stream.Write([]byte(value)); err != nil {
				t.Errorf("write: %v", err)
				return
			}
			got := make([]byte, len(value))
			if _, err := io.ReadFull(stream, got); err != nil {
				t.Errorf("read: %v", err)
				return
			}
			if string(got) != value {
				t.Errorf("got %q, want %q", got, value)
			}
		}()
	}
	wg.Wait()
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestOpenRejectionPropagates(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	client := NewClient(clientConn, 1)
	server := NewServer(serverConn, 1)
	defer client.Close()
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		stream, err := server.Accept(ctx)
		if err == nil {
			_ = stream.Reject(errors.New("target refused"))
		}
	}()
	_, err := client.Open(ctx)
	if !errors.Is(err, ErrStreamRejected) {
		t.Fatalf("Open error = %v", err)
	}
}

func TestHalfClosePreservesReverseTraffic(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	client := NewClient(clientConn, 1)
	server := NewServer(serverConn, 1)
	defer client.Close()
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	serverErr := make(chan error, 1)
	go func() {
		stream, err := server.Accept(ctx)
		if err != nil {
			serverErr <- err
			return
		}
		if err := stream.Approve(); err != nil {
			serverErr <- err
			return
		}
		request, err := io.ReadAll(stream)
		if err != nil {
			serverErr <- err
			return
		}
		_, err = stream.Write(append([]byte("reply:"), request...))
		_ = stream.CloseWrite()
		serverErr <- err
	}()
	stream, err := client.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte("request")); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	response, err := io.ReadAll(stream)
	if err != nil {
		t.Fatal(err)
	}
	if string(response) != "reply:request" {
		t.Fatalf("response = %q", response)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestStreamLimit(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	client := NewClient(clientConn, 1)
	server := NewServer(serverConn, 1)
	defer client.Close()
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		stream, err := server.Accept(ctx)
		if err == nil {
			_ = stream.Approve()
		}
	}()
	first, err := client.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := client.Open(ctx); !errors.Is(err, ErrTooManyStreams) {
		t.Fatalf("second Open error = %v", err)
	}
}

func TestCompressedMultiplexedStream(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	clientTraffic := &traffic.Counter{}
	serverTraffic := &traffic.Counter{}
	client := NewClientWithCompressionAndTraffic(clientConn, 1, true, clientTraffic)
	server := NewServerWithCompressionAndTraffic(serverConn, 1, true, serverTraffic)
	defer client.Close()
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	payload := bytes.Repeat([]byte("data: {\"token\":\"LightningBNB compression\"}\n\n"), 4096)
	serverErr := make(chan error, 1)
	go func() {
		stream, err := server.Accept(ctx)
		if err == nil {
			err = stream.Approve()
		}
		if err == nil {
			_, err = io.Copy(stream, stream)
		}
		serverErr <- err
	}()

	stream, err := client.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := stream.Write(payload)
		writeDone <- writeErr
	}()
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(stream, got); err != nil {
		t.Fatal(err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("compressed round trip changed payload")
	}
	for name, snapshot := range map[string]traffic.Snapshot{
		"client": clientTraffic.Snapshot(),
		"server": serverTraffic.Snapshot(),
	} {
		if !snapshot.CompressionEnabled {
			t.Fatalf("%s compression counters are disabled", name)
		}
		if snapshot.TXUncompressed != uint64(len(payload)) || snapshot.RXUncompressed != uint64(len(payload)) {
			t.Fatalf("%s uncompressed totals = tx %d rx %d, want %d", name, snapshot.TXUncompressed, snapshot.RXUncompressed, len(payload))
		}
		if snapshot.TXCompressed >= snapshot.TXUncompressed || snapshot.RXCompressed >= snapshot.RXUncompressed {
			t.Fatalf("%s compressed totals did not shrink: %+v", name, snapshot)
		}
	}
	_ = stream.Close()
	select {
	case err := <-serverErr:
		if err != nil && !errors.Is(err, io.ErrClosedPipe) && err.Error() != "stream closed" {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("compressed echo handler did not stop")
	}
}

func TestCompressedPayloadRejectsMalformedInput(t *testing.T) {
	if _, err := decodeCompressedPayload([]byte{compressionDeflate, 0xff}); err == nil {
		t.Fatal("malformed compressed payload was accepted")
	}
	encoded, err := encodeCompressedPayload(bytes.Repeat([]byte("repeat"), 1000))
	if err != nil {
		t.Fatal(err)
	}
	if encoded[0] != compressionDeflate {
		t.Fatal("compressible payload was stored raw")
	}
	decoded, err := decodeCompressedPayload(encoded)
	if err != nil || !bytes.Equal(decoded, bytes.Repeat([]byte("repeat"), 1000)) {
		t.Fatalf("decoded payload differs: %v", err)
	}
}

func TestStreamBatchesWindowUpdates(t *testing.T) {
	session := &Session{
		control: make(chan protocol.Frame, 1),
		done:    make(chan struct{}),
	}
	stream := newStream(session, 1)
	stream.opened = true
	stream.inbound = make([]byte, windowUpdateThreshold)
	stream.receiveWindow -= windowUpdateThreshold

	buffer := make([]byte, windowUpdateThreshold/2)
	if _, err := stream.Read(buffer); err != nil {
		t.Fatal(err)
	}
	select {
	case frame := <-session.control:
		t.Fatalf("early window update = %+v", frame)
	default:
	}
	if _, err := stream.Read(buffer); err != nil {
		t.Fatal(err)
	}
	select {
	case frame := <-session.control:
		if got := protocol.WindowAmount(frame); got != windowUpdateThreshold {
			t.Fatalf("window update = %d", got)
		}
	default:
		t.Fatal("batched window update was not emitted")
	}
}
