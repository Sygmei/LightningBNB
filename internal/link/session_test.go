package link

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

type fakeWire struct {
	done chan struct{}
	once sync.Once
}

type fakePacketConn struct {
	wire *fakeWire
	in   chan []byte
	out  chan []byte
	recv chan []byte
	mtu  int

	mu            sync.Mutex
	dropType      map[byte]int
	duplicateType map[byte]int
}

func fakePacketPair(mtuA, mtuB int) (*fakePacketConn, *fakePacketConn) {
	wire := &fakeWire{done: make(chan struct{})}
	aIn := make(chan []byte, 64)
	bIn := make(chan []byte, 64)
	a := &fakePacketConn{wire: wire, in: aIn, out: bIn, recv: make(chan []byte, 64), mtu: mtuA, dropType: make(map[byte]int), duplicateType: make(map[byte]int)}
	b := &fakePacketConn{wire: wire, in: bIn, out: aIn, recv: make(chan []byte, 64), mtu: mtuB, dropType: make(map[byte]int), duplicateType: make(map[byte]int)}
	go a.forward()
	go b.forward()
	return a, b
}

func (c *fakePacketConn) forward() {
	defer close(c.recv)
	for {
		select {
		case <-c.wire.done:
			return
		case packet := <-c.in:
			select {
			case c.recv <- packet:
			case <-c.wire.done:
				return
			}
		}
	}
}

func (c *fakePacketConn) Send(ctx context.Context, packet []byte) error {
	copyOfPacket := append([]byte(nil), packet...)
	c.mu.Lock()
	if len(packet) > 0 && c.dropType[packet[0]] > 0 {
		c.dropType[packet[0]]--
		c.mu.Unlock()
		return nil
	}
	duplicate := len(packet) > 0 && c.duplicateType[packet[0]] > 0
	if duplicate {
		c.duplicateType[packet[0]]--
	}
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.wire.done:
		return io.ErrClosedPipe
	case c.out <- copyOfPacket:
	}
	if duplicate {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.wire.done:
			return io.ErrClosedPipe
		case c.out <- append([]byte(nil), copyOfPacket...):
		}
	}
	return nil
}

func (c *fakePacketConn) Receive() <-chan []byte { return c.recv }
func (c *fakePacketConn) MTU() int               { return c.mtu }
func (c *fakePacketConn) Close() error {
	c.wire.once.Do(func() { close(c.wire.done) })
	return nil
}

func (c *fakePacketConn) dropNext(packetType byte) {
	c.mu.Lock()
	c.dropType[packetType]++
	c.mu.Unlock()
}

func (c *fakePacketConn) duplicateNext(packetType byte) {
	c.mu.Lock()
	c.duplicateType[packetType]++
	c.mu.Unlock()
}

func bindPair(t *testing.T, clientSession, serverSession *Session, clientConn, serverConn PacketConn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	serverErr := make(chan error, 1)
	go func() {
		hello, err := ReadHello(ctx, serverConn)
		if err == nil {
			err = serverSession.BindServer(ctx, serverConn, hello)
		}
		serverErr <- err
	}()
	if err := clientSession.BindClient(ctx, clientConn); err != nil {
		t.Fatalf("client bind: %v", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("server bind: %v", err)
	}
}

func newSessionPair(t *testing.T, cfg Config) (*Session, *Session, *fakePacketConn, *fakePacketConn) {
	t.Helper()
	client, err := NewSession(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server := NewSessionWithID(client.ID(), cfg)
	clientConn, serverConn := fakePacketPair(80, 64)
	bindPair(t, client, server, clientConn, serverConn)
	return client, server, clientConn, serverConn
}

func TestSessionTransfersAndRetransmits(t *testing.T) {
	cfg := Config{ResumeTimeout: 2 * time.Second, ReplayWindow: 1024, MaxConnections: 8}
	client, server, clientConn, _ := newSessionPair(t, cfg)
	defer client.Close()
	defer server.Close()

	clientConn.dropNext(packetData)
	want := []byte("retransmitted payload")
	if _, err := client.Write(want); err != nil {
		t.Fatal(err)
	}
	if err := server.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(server, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestSessionPipelinesPacketsBeforeAcknowledgement(t *testing.T) {
	session, err := NewSession(Config{ReplayWindow: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	session.mu.Lock()
	session.txBuf = make([]byte, 100)
	session.txNext = uint64(len(session.txBuf))
	b := &binding{mtu: 20, lastTX: time.Now(), sendNext: session.txBase}
	first := session.nextPacketLocked(b, time.Now())
	second := session.nextPacketLocked(b, time.Now())
	third := session.nextPacketLocked(b, time.Now())
	session.mu.Unlock()

	for i, packet := range [][]byte{first, second, third} {
		if len(packet) != 20 || packet[0] != packetData {
			t.Fatalf("packet %d = %x, want a 20-byte data packet", i, packet)
		}
		if got, want := binary.BigEndian.Uint64(packet[1:9]), uint64(i*11); got != want {
			t.Fatalf("packet %d sequence = %d, want %d", i, got, want)
		}
	}
}

func TestSessionBoundsUnacknowledgedTransmissionWindow(t *testing.T) {
	session, err := NewSession(Config{ReplayWindow: 2 * transmitWindow})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	session.mu.Lock()
	session.txBuf = make([]byte, 2*transmitWindow)
	session.txNext = uint64(len(session.txBuf))
	b := &binding{mtu: 244, lastTX: time.Now(), sendNext: session.txBase}
	now := time.Now()
	sent := 0
	for {
		packet := session.nextPacketLocked(b, now)
		if packet == nil {
			break
		}
		if packet[0] != packetData {
			t.Fatalf("unexpected packet type %d", packet[0])
		}
		sent += len(packet) - 9
	}
	session.mu.Unlock()

	if sent != transmitWindow {
		t.Fatalf("sent %d unacknowledged bytes, want %d", sent, transmitWindow)
	}
}

func TestSessionBatchesCumulativeAcknowledgements(t *testing.T) {
	session, err := NewSession(Config{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	b := &binding{mtu: 20, lastRX: now, lastTX: now}
	session.mu.Lock()
	session.current = b
	session.mu.Unlock()

	for sequence := range uint64(ackBatchPackets) {
		packet := make([]byte, 10)
		packet[0] = packetData
		binary.BigEndian.PutUint64(packet[1:9], sequence)
		packet[9] = byte(sequence)
		if err := session.handlePacket(b, packet); err != nil {
			t.Fatal(err)
		}
		if sequence+1 < ackBatchPackets {
			session.mu.Lock()
			ack := session.nextPacketLocked(b, now)
			session.mu.Unlock()
			if ack != nil {
				t.Fatalf("ACK emitted after only %d packets: %x", sequence+1, ack)
			}
		}
	}

	session.mu.Lock()
	ack := session.nextPacketLocked(b, now)
	session.current = nil
	session.mu.Unlock()
	_ = session.Close()
	if len(ack) != 9 || ack[0] != packetAck || binary.BigEndian.Uint64(ack[1:]) != ackBatchPackets {
		t.Fatalf("batched ACK = %x", ack)
	}
}

func TestSessionAcknowledgementDeadline(t *testing.T) {
	session, err := NewSession(Config{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	b := &binding{mtu: 20, lastRX: now, lastTX: now}
	session.mu.Lock()
	session.current = b
	session.mu.Unlock()
	packet := make([]byte, 10)
	packet[0] = packetData
	if err := session.handlePacket(b, packet); err != nil {
		t.Fatal(err)
	}

	session.mu.Lock()
	ack := session.nextPacketLocked(b, now.Add(2*ackMaxDelay))
	session.current = nil
	session.mu.Unlock()
	_ = session.Close()
	if len(ack) != 9 || ack[0] != packetAck || binary.BigEndian.Uint64(ack[1:]) != 1 {
		t.Fatalf("deadline ACK = %x", ack)
	}
}

func TestCompressionCapabilityUsesOptionalBootstrapByte(t *testing.T) {
	var id SessionID
	_, legacyHello := encodeHello(id, 0, Config{}.normalized(), 244)
	legacyAck := encodeHelloAck(0, Config{}.normalized(), 244)
	if len(legacyHello) != 17 || len(legacyAck) != 18 {
		t.Fatalf("legacy bootstrap lengths = (%d, %d)", len(legacyHello), len(legacyAck))
	}

	compressed := Config{Compression: true}.normalized()
	_, compressedHello := encodeHello(id, 0, compressed, 244)
	compressedAck := encodeHelloAck(0, compressed, 244)
	if len(compressedHello) != 18 || compressedHello[17] != capabilityCompression {
		t.Fatalf("compressed HELLO = %x", compressedHello)
	}
	_, decoded, _, err := decodeHelloAck(compressedAck)
	if err != nil || !decoded.Compression {
		t.Fatalf("decode compressed HELLO_ACK = %+v, %v", decoded, err)
	}
}

func TestSessionNegotiatesCompression(t *testing.T) {
	cfg := Config{ResumeTimeout: time.Second, ReplayWindow: 1024, MaxConnections: 1, Compression: true}
	client, server, _, _ := newSessionPair(t, cfg)
	defer client.Close()
	defer server.Close()
	if !client.Config().Compression || !server.Config().Compression {
		t.Fatalf("compression not negotiated: client=%t server=%t", client.Config().Compression, server.Config().Compression)
	}
}

func TestSessionRejectsCompressionMismatch(t *testing.T) {
	client, err := NewSession(Config{Compression: true})
	if err != nil {
		t.Fatal(err)
	}
	server := NewSessionWithID(client.ID(), Config{Compression: false})
	clientConn, serverConn := fakePacketPair(80, 80)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	clientErr := make(chan error, 1)
	go func() { clientErr <- client.BindClient(ctx, clientConn) }()
	hello, err := ReadHello(ctx, serverConn)
	if err != nil {
		t.Fatal(err)
	}
	if !hello.Compression {
		t.Fatal("compressed client did not advertise compression")
	}
	if err := server.BindServer(ctx, serverConn, hello); !errors.Is(err, ErrCompression) {
		t.Fatalf("BindServer error = %v", err)
	}
	_ = serverConn.Close()
	if err := <-clientErr; err == nil {
		t.Fatal("compressed client unexpectedly bound")
	}
	_ = client.Close()
	_ = server.Close()
}

func TestSessionSlidingWindowRecoversMissingPacket(t *testing.T) {
	cfg := Config{ResumeTimeout: 3 * time.Second, ReplayWindow: 128 << 10, MaxConnections: 2}
	client, server, clientConn, _ := newSessionPair(t, cfg)
	defer client.Close()
	defer server.Close()

	clientConn.dropNext(packetData)
	want := make([]byte, 4096)
	for i := range want {
		want[i] = byte(i)
	}
	if _, err := client.Write(want); err != nil {
		t.Fatal(err)
	}
	if err := server.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(server, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("recovered payload differs from sent payload")
	}
}

func TestHandshakeRetransmitsLostResponse(t *testing.T) {
	cfg := Config{ResumeTimeout: 3 * time.Second, ReplayWindow: 1024, MaxConnections: 2}
	client, err := NewSession(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server := NewSessionWithID(client.ID(), cfg)
	clientConn, serverConn := fakePacketPair(40, 40)
	serverConn.dropNext(packetHelloAck)
	bindPair(t, client, server, clientConn, serverConn)
	defer client.Close()
	defer server.Close()
	if !client.IsBound() || !server.IsBound() {
		t.Fatal("sessions did not bind after retransmitted handshake")
	}
}

func TestSessionRetransmitsAfterLostAcknowledgement(t *testing.T) {
	cfg := Config{ResumeTimeout: 3 * time.Second, ReplayWindow: 1024, MaxConnections: 2}
	client, server, _, serverConn := newSessionPair(t, cfg)
	defer client.Close()
	defer server.Close()
	serverConn.dropNext(packetAck)
	if _, err := client.Write([]byte("ack me")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len("ack me"))
	if _, err := io.ReadFull(server, got); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return len(client.txBuf) == 0
	})
}

func TestSessionResumesBufferedBytes(t *testing.T) {
	cfg := Config{ResumeTimeout: 2 * time.Second, ReplayWindow: 1024, MaxConnections: 8, Compression: true}
	client, server, clientConn, _ := newSessionPair(t, cfg)
	defer client.Close()
	defer server.Close()

	_ = clientConn.Close()
	waitUntil(t, time.Second, func() bool { return !client.IsBound() && !server.IsBound() })

	want := []byte("written while bluetooth is offline")
	if _, err := client.Write(want); err != nil {
		t.Fatal(err)
	}
	newClientConn, newServerConn := fakePacketPair(40, 100)
	bindPair(t, client, server, newClientConn, newServerConn)
	if err := server.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(server, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestSessionSuppressesDuplicateData(t *testing.T) {
	cfg := Config{ResumeTimeout: time.Second, ReplayWindow: 1024, MaxConnections: 1}
	client, server, clientConn, _ := newSessionPair(t, cfg)
	defer client.Close()
	defer server.Close()
	clientConn.duplicateNext(packetData)
	if _, err := client.Write([]byte("once")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4)
	if _, err := io.ReadFull(server, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "once" {
		t.Fatalf("got %q", got)
	}
	_ = server.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if n, err := server.Read(make([]byte, 1)); n != 0 || err == nil {
		t.Fatalf("duplicate was delivered: Read = (%d, %v)", n, err)
	}
}

func TestSessionExpiresAfterResumeDeadline(t *testing.T) {
	cfg := Config{ResumeTimeout: 100 * time.Millisecond, ReplayWindow: 1024, MaxConnections: 2}
	client, server, clientConn, _ := newSessionPair(t, cfg)
	defer client.Close()
	defer server.Close()
	_ = clientConn.Close()

	select {
	case <-client.Done():
	case <-time.After(time.Second):
		t.Fatal("client session did not expire")
	}
	if !errors.Is(client.Err(), ErrResumeTimeout) {
		t.Fatalf("client error = %v", client.Err())
	}
}

func TestSessionReplayWindowBackpressure(t *testing.T) {
	cfg := Config{ResumeTimeout: time.Second, ReplayWindow: 8, MaxConnections: 1}
	client, err := NewSession(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Write([]byte("12345678")); err != nil {
		t.Fatal(err)
	}
	_ = client.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
	n, err := client.Write([]byte("x"))
	if n != 0 || err == nil {
		t.Fatalf("Write = (%d, %v), want timeout", n, err)
	}
}

func TestSessionLimitsLiveWritesWithoutShrinkingOfflineReplay(t *testing.T) {
	session, err := NewSession(Config{ReplayWindow: 2 * liveWriteWindow})
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	session.current = &binding{}
	session.mu.Unlock()
	if n, err := session.Write(make([]byte, liveWriteWindow)); n != liveWriteWindow || err != nil {
		t.Fatalf("initial live Write = (%d, %v)", n, err)
	}
	_ = session.SetWriteDeadline(time.Now().Add(20 * time.Millisecond))
	if n, err := session.Write([]byte{1}); n != 0 || err == nil {
		t.Fatalf("overflow live Write = (%d, %v), want timeout", n, err)
	}

	session.mu.Lock()
	session.current = nil
	session.mu.Unlock()
	_ = session.SetWriteDeadline(time.Time{})
	if n, err := session.Write(make([]byte, liveWriteWindow)); n != liveWriteWindow || err != nil {
		t.Fatalf("offline replay Write = (%d, %v)", n, err)
	}
	_ = session.Close()
}

func TestSessionRejectsSequenceRollover(t *testing.T) {
	session, err := NewSession(Config{ReplayWindow: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	session.mu.Lock()
	session.txBase = ^uint64(0) - 1
	session.txNext = ^uint64(0) - 1
	session.mu.Unlock()
	if n, err := session.Write([]byte("xx")); n != 0 || !errors.Is(err, ErrSequenceExhausted) {
		t.Fatalf("Write = (%d, %v)", n, err)
	}
}

func TestBusyServerRejectsDifferentSession(t *testing.T) {
	client, err := NewSession(Config{})
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewSession(Config{})
	if err != nil {
		t.Fatal(err)
	}
	clientConn, serverConn := fakePacketPair(40, 40)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		hello, readErr := ReadHello(ctx, serverConn)
		if readErr == nil && !other.Matches(hello.ID) {
			_ = SendReject(ctx, serverConn, "server busy")
		}
	}()
	if err := client.BindClient(ctx, clientConn); !errors.Is(err, ErrRejected) || RejectionReason(err) != "server busy" {
		t.Fatalf("BindClient error = %v", err)
	}
}

func waitUntil(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
