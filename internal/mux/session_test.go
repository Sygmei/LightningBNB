package mux

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
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
