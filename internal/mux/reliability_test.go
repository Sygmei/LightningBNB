package mux

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/Sygmei/LightningBNB/internal/protocol"
)

func TestStreamErrorsDoNotCloseSession(t *testing.T) {
	for _, kind := range []string{"data-after-fin", "bad-compression", "window-overflow", "close-race"} {
		t.Run(kind, func(t *testing.T) {
			a, b := net.Pipe()
			defer b.Close()
			s := NewServerWithCompression(a, 2, kind == "bad-compression")
			defer s.Close()
			stream := newStream(s, 1)
			stream.opened = true
			frame := protocol.Frame{Type: protocol.FrameData, StreamID: 1, Payload: []byte{0xff, 1}}
			switch kind {
			case "data-after-fin":
				stream.remoteWriteClosed = true
			case "window-overflow":
				frame = protocol.WindowUpdate(1, 1)
			case "close-race":
				// readLoop can fetch a stream just before another goroutine closes it.
				stream.closed = true
			}
			s.mu.Lock()
			s.streams[1] = stream
			s.mu.Unlock()
			if err := s.handleFrame(frame); err != nil {
				t.Fatalf("stream error would terminate entire link: %v", err)
			}
			select {
			case <-s.Done():
				t.Fatalf("session closed: %v", s.Err())
			default:
			}
		})
	}
}

func TestArbitraryPayloadsAcrossStreamWindows(t *testing.T) {
	for _, compression := range []bool{false, true} {
		name := "raw"
		if compression {
			name = "compressed"
		}
		t.Run(name, func(t *testing.T) {
			a, b := net.Pipe()
			client := NewClientWithCompression(a, 4, compression)
			server := NewServerWithCompression(b, 4, compression)
			defer client.Close()
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			random := make([]byte, 256*1024)
			_, _ = rand.New(rand.NewSource(42)).Read(random)
			payload := append(bytes.Repeat([]byte("data: {\"text\":\"中文 🌩️ \\u0000 \\n\"}\r\n\r\n"), 8192), random...)
			accepted := make(chan *Stream, 1)
			go func() {
				stream, err := server.Accept(ctx)
				if err == nil {
					err = stream.Approve()
				}
				if err != nil {
					accepted <- nil
					return
				}
				accepted <- stream
			}()
			out, err := client.Open(ctx)
			if err != nil {
				t.Fatal(err)
			}
			in := <-accepted
			if in == nil {
				t.Fatal("accept failed")
			}
			deadline, _ := ctx.Deadline()
			_ = out.SetDeadline(deadline)
			_ = in.SetDeadline(deadline)
			writeDone := make(chan error, 1)
			go func() {
				_, err := out.Write(payload)
				if err == nil {
					err = out.CloseWrite()
				}
				writeDone <- err
			}()
			got, err := io.ReadAll(in)
			if err != nil {
				t.Fatal(err)
			}
			if err := <-writeDone; err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatal("large payload changed")
			}
			// A failed request's ordinary HTTP response is just bytes too.
			response := []byte("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
			if _, err := in.Write(response); err != nil {
				t.Fatal(err)
			}
			_ = in.CloseWrite()
			got, err = io.ReadAll(out)
			if err != nil || !bytes.Equal(got, response) {
				t.Fatalf("response changed: %v", err)
			}
			select {
			case <-client.Done():
				t.Fatalf("session closed: %v", client.Err())
			default:
			}
		})
	}
}

func FuzzCompressedPayloadRoundTrip(f *testing.F) {
	f.Add([]byte("\x00\xff\r\n中文🌩️"))
	f.Add(bytes.Repeat([]byte("tool_call data: {}\n\n"), 1000))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			return
		}
		if len(data) >= protocol.MaxDataPayload {
			data = data[:protocol.MaxDataPayload-1]
		}
		encoded, err := encodeCompressedPayload(data)
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) > protocol.MaxDataPayload {
			t.Fatal("encoded frame too large")
		}
		decoded, err := decodeCompressedPayload(encoded)
		if err != nil || !bytes.Equal(data, decoded) {
			t.Fatalf("round trip failed: %v", err)
		}
	})
}

func TestClosedStreamsDoNotExhaustScheduler(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	// Deliberately omit the writer to model a blocked/offline BLE transport.
	s := &Session{conn: a, maxStreams: 1, streams: make(map[uint32]*Stream),
		ready: make(chan uint32, 1), control: make(chan protocol.Frame, 8), done: make(chan struct{})}
	defer s.Close()
	for _, id := range []uint32{1, 3, 5} {
		stream := newStream(s, id)
		s.streams[id] = stream
		if err := s.schedule(stream); err != nil {
			t.Fatalf("closed streams exhausted scheduler: %v", err)
		}
		s.removeStream(id)
	}
}

func TestLateDataAfterResetDoesNotFloodControlQueue(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	s := &Session{conn: a, maxStreams: 1, streams: make(map[uint32]*Stream),
		ready: make(chan uint32, 1), control: make(chan protocol.Frame, 8),
		accept: make(chan *Stream, 1), done: make(chan struct{})}
	defer s.Close()
	if err := s.handleFrame(protocol.Frame{Type: protocol.FrameOpen, StreamID: 1}); err != nil {
		t.Fatal(err)
	}
	s.removeStream(1)
	for range 100 {
		if err := s.handleFrame(protocol.Frame{Type: protocol.FrameData, StreamID: 1, Payload: []byte("late")}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-s.Done():
		t.Fatalf("late data closed session: %v", s.Err())
	default:
	}
	if len(s.control) != 0 {
		t.Fatal("late frames generated redundant resets")
	}
}

func TestStreamIDsCannotWrapAndReuseOldStreams(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	s := NewClient(a, 1)
	defer s.Close()
	s.nextID = ^uint32(0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = s.Open(ctx)
	if s.nextID != 0 {
		t.Fatalf("next ID wrapped: %d", s.nextID)
	}
	for range 2 {
		if _, err := s.Open(context.Background()); err == nil || err.Error() != "stream id space exhausted" {
			t.Fatalf("exhausted IDs reused: %v", err)
		}
	}
}
