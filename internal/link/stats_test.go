package link

import (
	"io"
	"testing"
	"time"
)

func TestTransportSnapshotTracksPacketsACKsAndOutstandingBytes(t *testing.T) {
	client, server, _, _ := newSessionPair(t, Config{ReplayWindow: 4096})
	defer client.Close()
	defer server.Close()

	payload := make([]byte, 2048)
	writeDone := make(chan error, 1)
	go func() {
		_, err := client.Write(payload)
		writeDone <- err
	}()
	if _, err := io.ReadFull(server, make([]byte, len(payload))); err != nil {
		t.Fatal(err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for client.TransportSnapshot().OutstandingBytes != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	clientStats := client.TransportSnapshot()
	serverStats := server.TransportSnapshot()
	if clientStats.DataTXPackets == 0 || clientStats.DataTXBytes < uint64(len(payload)) || clientStats.ACKRXPackets == 0 {
		t.Fatalf("client transport stats = %+v", clientStats)
	}
	if serverStats.DataRXPackets == 0 || serverStats.DataRXBytes != uint64(len(payload)) || serverStats.ACKTXPackets == 0 {
		t.Fatalf("server transport stats = %+v", serverStats)
	}
	if clientStats.OutstandingBytes != 0 {
		t.Fatalf("client still has %d outstanding bytes", clientStats.OutstandingBytes)
	}
	if clientStats.SendCalls < clientStats.DataTXPackets || clientStats.MaxSendDuration <= 0 {
		t.Fatalf("client send timing stats = %+v", clientStats)
	}
}
