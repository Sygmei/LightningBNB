package link

import (
	"context"
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
	cfg := Config{ResumeTimeout: 2 * time.Second, ReplayWindow: 1024, MaxConnections: 8}
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
	if err := client.BindClient(ctx, clientConn); !errors.Is(err, ErrRejected) {
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
