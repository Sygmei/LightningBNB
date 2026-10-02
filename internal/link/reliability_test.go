package link

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/Sygmei/LightningBNB/internal/mux"
)

func TestRetransmissionWithChangedPacketBoundaries(t *testing.T) {
	for _, start := range []uint64{0, 2} {
		t.Run(fmt.Sprint(start), func(t *testing.T) {
			s := NewSessionWithID(SessionID{}, Config{ReplayWindow: 12})
			defer s.Close()
			// The first short write was lost; a later fragment arrived first.
			_, _, _, _ = s.acceptDataLocked(4, []byte("efghijkl"))
			if start > 0 {
				_, _, _, _ = s.acceptDataLocked(0, []byte("ab"))
			}
			// Replay coalesces writes into a larger packet and can overlap both
			// delivered bytes and the retained fragment. It must still advance.
			if _, _, rejected, err := s.acceptDataLocked(0, []byte("abcdefgh")); err != nil || rejected {
				t.Fatalf("resegmented replay rejected: %v, %v", rejected, err)
			}
			if s.rxNext != 12 || !bytes.Equal(s.rxBuf, []byte("abcdefghijkl")) || s.rxPendingBytes != 0 {
				t.Fatalf("replay stalled or corrupted bytes: next=%d data=%q pending=%d", s.rxNext, s.rxBuf, s.rxPendingBytes)
			}
			if s.stats.dataRXBytes != 12 {
				t.Fatalf("unique received bytes = %d", s.stats.dataRXBytes)
			}
		})
	}
}

func TestCompressedUploadOverLossyLinkAndResume(t *testing.T) {
	cfg := Config{ResumeTimeout: 5 * time.Second, Compression: true}
	client, server, clientPacket, serverPacket := newSessionPair(t, cfg)
	defer client.Close()
	defer server.Close()
	cm := mux.NewClientWithCompression(client, 2, true)
	sm := mux.NewServerWithCompression(server, 2, true)
	defer cm.Close()
	defer sm.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	accepted := make(chan *mux.Stream, 1)
	go func() {
		stream, err := sm.Accept(ctx)
		if err == nil {
			err = stream.Approve()
		}
		if err != nil {
			accepted <- nil
			return
		}
		accepted <- stream
	}()
	out, err := cm.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	in := <-accepted
	if in == nil {
		t.Fatal("stream accept failed")
	}
	deadline, _ := ctx.Deadline()
	_ = out.SetDeadline(deadline)
	_ = in.SetDeadline(deadline)
	noise := make([]byte, 64*1024)
	_, _ = rand.New(rand.NewSource(9)).Read(noise)
	payload := append(bytes.Repeat([]byte("data: {\"message\":\"中文 🌩️ \\u0000\"}\n\n"), 16384), noise...)
	clientPacket.dropNext(packetData)
	clientPacket.reorderNextPair(packetData)
	serverPacket.dropNext(packetAck)
	writeDone := make(chan error, 1)
	go func() {
		_, err := out.Write(payload)
		if err == nil {
			err = out.CloseWrite()
		}
		writeDone <- err
	}()
	// Drop the physical connection while the large compressed request is in
	// flight, then resume the same stream at a different negotiated MTU.
	prefix := make([]byte, 32*1024)
	if _, err := io.ReadFull(in, prefix); err != nil {
		t.Fatal(err)
	}
	_ = clientPacket.Close()
	until := time.Now().Add(time.Second)
	for client.IsBound() || server.IsBound() {
		if time.Now().After(until) {
			t.Fatal("transport did not detach")
		}
		time.Sleep(time.Millisecond)
	}
	nextClient, nextServer := fakePacketPair(244, 244)
	nextClient.duplicateNext(packetData)
	bindPair(t, client, server, nextClient, nextServer)
	suffix, err := io.ReadAll(in)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(append(prefix, suffix...), payload) {
		t.Fatal("resumed compressed request changed bytes")
	}
	response := []byte("data: {\"token\":\"ok 🌩️\"}\n\ndata: [DONE]\n\n")
	if _, err := in.Write(response); err != nil {
		t.Fatal(err)
	}
	_ = in.CloseWrite()
	got, err := io.ReadAll(out)
	if err != nil || !bytes.Equal(got, response) {
		t.Fatalf("SSE response failed: %v", err)
	}
	if !client.IsBound() || !server.IsBound() {
		t.Fatal("healthy session disconnected")
	}
	if snapshot := client.TransportSnapshot(); snapshot.DetachCount != 1 || snapshot.LastDetachReason == "" {
		t.Fatalf("missing disconnect diagnostics: %+v", snapshot)
	}
}

func TestHeartbeatSendTimeoutWhileReceivingDoesNotDetach(t *testing.T) {
	s := NewSessionWithID(SessionID{}, Config{})
	started := time.Now()
	b := &binding{heartbeatPending: true, lastRX: started.Add(time.Second)}
	s.current = b
	finished := started.Add(heartbeatResponseTimeout)
	if s.heartbeatSendResultLocked(b, started, finished, context.DeadlineExceeded) {
		t.Fatal("probe queue timeout detached a link still receiving ACKs")
	}
	if b.heartbeatPending || b.heartbeatFailures != 0 {
		t.Fatal("productive link retained a failed probe")
	}
	b.lastRX = started.Add(-time.Second)
	if !s.heartbeatSendResultLocked(b, started, finished, context.DeadlineExceeded) {
		t.Fatal("stalled send without incoming packets did not detach")
	}
	b.lastRX = finished
	if !s.heartbeatSendResultLocked(b, started, finished, errors.New("device disconnected")) {
		t.Fatal("actual transport error was suppressed")
	}
	b.heartbeatPending = true
	if s.heartbeatSendResultLocked(b, started, finished, nil) || b.heartbeatSentAt != finished {
		t.Fatal("response deadline did not start after the probe was sent")
	}
}

func FuzzResegmentedReplay(f *testing.F) {
	f.Add([]byte("a\x00\xff中文🌩️\r\n"), uint8(5))
	f.Add(bytes.Repeat([]byte("data: {}\n\n"), 100), uint8(61))
	f.Fuzz(func(t *testing.T, input []byte, width uint8) {
		if len(input) < 2 {
			return
		}
		if len(input) > 4096 {
			input = input[:4096]
		}
		s := NewSessionWithID(SessionID{}, Config{ReplayWindow: len(input)})
		chunk := int(width) + 1
		// Retain a subset of fragments out of order, then replay using
		// different boundaries. Final delivery must be exact and bounded.
		for end := len(input); end > chunk; end -= chunk {
			_, _, _, _ = s.acceptDataLocked(uint64(end-chunk), input[end-chunk:end])
		}
		for start := 0; start < len(input); start += chunk + 3 {
			end := min(len(input), start+chunk+3)
			if _, _, _, err := s.acceptDataLocked(uint64(start), input[start:end]); err != nil {
				t.Fatal(err)
			}
			if len(s.rxBuf)+s.rxPendingBytes > len(input) {
				t.Fatal("receive window exceeded")
			}
		}
		if s.rxNext != uint64(len(input)) || !bytes.Equal(s.rxBuf, input) {
			t.Fatalf("replay lost or duplicated bytes: received %d of %d", s.rxNext, len(input))
		}
	})
}
