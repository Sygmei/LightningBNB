package mux

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestServiceListAndSelectorRoundTrip(t *testing.T) {
	clientWire, serverWire := net.Pipe()
	services := []Service{{Name: "http", Port: 1180}, {Name: "google", Host: "google.com", Port: 443}, {Name: "https", Port: 11443}}
	client := NewClient(clientWire, 4)
	server := NewServerWithServicesAndCompressionAndTraffic(serverWire, 4, false, nil, services)
	defer client.Close()
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	gotServices, err := client.Services(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(gotServices) != len(services) || gotServices[0] != services[0] || gotServices[1] != services[1] {
		t.Fatalf("services = %+v, want %+v", gotServices, services)
	}

	accepted := make(chan *Stream, 1)
	acceptErr := make(chan error, 1)
	go func() {
		serverStream, err := server.Accept(ctx)
		if err != nil {
			acceptErr <- err
			return
		}
		if err := serverStream.Approve(); err != nil {
			acceptErr <- err
			return
		}
		accepted <- serverStream
	}()
	stream, err := client.OpenService(ctx, "1180")
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	var serverStream *Stream
	select {
	case serverStream = <-accepted:
	case err := <-acceptErr:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if serverStream.Service() != "1180" {
		t.Fatalf("server stream service = %q", serverStream.Service())
	}
}
