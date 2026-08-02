package bridge

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Sygmei/LightningBNB/internal/link"
	"github.com/Sygmei/LightningBNB/internal/mux"
)

type testAvailability struct {
	mu    sync.Mutex
	bound bool
	done  chan struct{}
}

func (a *testAvailability) IsBound() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.bound
}

func (a *testAvailability) Done() <-chan struct{} { return a.done }

func (a *testAvailability) setBound(bound bool) {
	a.mu.Lock()
	a.bound = bound
	a.mu.Unlock()
}

func TestTCPBridgeQueuesUntilAvailableAndForwards(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	target, targetAddr := startEchoServer(t, ctx)
	defer target.Close()

	clientWire, serverWire := net.Pipe()
	clientMux := mux.NewClient(clientWire, 4)
	serverMux := mux.NewServer(serverWire, 4)
	defer clientMux.Close()
	defer serverMux.Close()
	go func() { _ = ServeServer(ctx, serverMux, targetAddr, time.Second, nil) }()

	availability := &testAvailability{done: make(chan struct{})}
	bridge := NewClient(time.Second, 4, nil)
	bridge.SetEndpoint(&Endpoint{Link: availability, Mux: clientMux})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = bridge.Serve(ctx, listener) }()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("queued")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	availability.setBound(true)

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	got := make([]byte, len("queued"))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "queued" {
		t.Fatalf("got %q", got)
	}
}

func TestTCPBridgeEnforcesConnectionLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bridge := NewClient(time.Second, 1, nil)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = bridge.Serve(ctx, listener) }()

	first, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	_ = second.SetReadDeadline(time.Now().Add(time.Second))
	buffer := make([]byte, 1)
	if _, err := second.Read(buffer); err == nil {
		t.Fatal("second connection was not rejected")
	}
}

func TestActiveTCPConnectionResumesAcrossPacketConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	target, targetAddr := startEchoServer(t, ctx)
	defer target.Close()

	cfg := link.Config{ResumeTimeout: 2 * time.Second, ReplayWindow: 4096, MaxConnections: 2}
	clientLink, err := link.NewSession(cfg)
	if err != nil {
		t.Fatal(err)
	}
	serverLink := link.NewSessionWithID(clientLink.ID(), cfg)
	firstClientPacket, firstServerPacket := bridgePacketPair(64)
	bindBridgeLink(t, clientLink, serverLink, firstClientPacket, firstServerPacket)
	defer clientLink.Close()
	defer serverLink.Close()

	clientMux := mux.NewClient(clientLink, 2)
	serverMux := mux.NewServer(serverLink, 2)
	defer clientMux.Close()
	defer serverMux.Close()
	go func() { _ = ServeServer(ctx, serverMux, targetAddr, time.Second, nil) }()
	clientBridge := NewClient(2*time.Second, 2, nil)
	clientBridge.SetEndpoint(&Endpoint{Link: clientLink, Mux: clientMux})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = clientBridge.Serve(ctx, listener) }()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	assertEcho(t, conn, "before")
	_ = firstClientPacket.Close()
	waitBridgeCondition(t, time.Second, func() bool { return !clientLink.IsBound() && !serverLink.IsBound() })
	if _, err := conn.Write([]byte("during")); err != nil {
		t.Fatal(err)
	}

	secondClientPacket, secondServerPacket := bridgePacketPair(40)
	bindBridgeLink(t, clientLink, serverLink, secondClientPacket, secondServerPacket)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	got := make([]byte, len("during"))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "during" {
		t.Fatalf("resumed echo = %q", got)
	}
}

func TestServerReportsUnavailableTarget(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	target := probe.Addr().String()
	_ = probe.Close()
	clientWire, serverWire := net.Pipe()
	clientMux := mux.NewClient(clientWire, 1)
	serverMux := mux.NewServer(serverWire, 1)
	defer clientMux.Close()
	defer serverMux.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = ServeServer(ctx, serverMux, target, 100*time.Millisecond, nil) }()
	if _, err := clientMux.Open(ctx); err == nil {
		t.Fatal("opening a stream to an unavailable target succeeded")
	}
}

func TestServerForwardsToIPv6Target(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	target, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback unavailable: %v", err)
	}
	defer target.Close()
	go serveEcho(ctx, target)
	clientWire, serverWire := net.Pipe()
	clientMux := mux.NewClient(clientWire, 1)
	serverMux := mux.NewServer(serverWire, 1)
	defer clientMux.Close()
	defer serverMux.Close()
	go func() { _ = ServeServer(ctx, serverMux, target.Addr().String(), time.Second, nil) }()
	openCtx, openCancel := context.WithTimeout(ctx, 2*time.Second)
	defer openCancel()
	stream, err := clientMux.Open(openCtx)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if _, err := stream.Write([]byte("ipv6")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4)
	if _, err := io.ReadFull(stream, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "ipv6" {
		t.Fatalf("IPv6 echo = %q", got)
	}
}

func assertEcho(t *testing.T, conn net.Conn, value string) {
	t.Helper()
	if _, err := conn.Write([]byte(value)); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	got := make([]byte, len(value))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != value {
		t.Fatalf("echo = %q, want %q", got, value)
	}
}

type bridgeWire struct {
	done chan struct{}
	once sync.Once
}

type bridgePacketConn struct {
	wire    *bridgeWire
	in      chan []byte
	out     chan []byte
	receive chan []byte
	mtu     int
}

func bridgePacketPair(mtu int) (*bridgePacketConn, *bridgePacketConn) {
	wire := &bridgeWire{done: make(chan struct{})}
	aIn := make(chan []byte, 64)
	bIn := make(chan []byte, 64)
	a := &bridgePacketConn{wire: wire, in: aIn, out: bIn, receive: make(chan []byte, 64), mtu: mtu}
	b := &bridgePacketConn{wire: wire, in: bIn, out: aIn, receive: make(chan []byte, 64), mtu: mtu}
	go a.forward()
	go b.forward()
	return a, b
}

func (c *bridgePacketConn) forward() {
	defer close(c.receive)
	for {
		select {
		case <-c.wire.done:
			return
		case packet := <-c.in:
			select {
			case c.receive <- packet:
			case <-c.wire.done:
				return
			}
		}
	}
}

func (c *bridgePacketConn) Send(ctx context.Context, packet []byte) error {
	copyOfPacket := append([]byte(nil), packet...)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.wire.done:
		return io.ErrClosedPipe
	case c.out <- copyOfPacket:
		return nil
	}
}

func (c *bridgePacketConn) Receive() <-chan []byte { return c.receive }
func (c *bridgePacketConn) MTU() int               { return c.mtu }
func (c *bridgePacketConn) Close() error {
	c.wire.once.Do(func() { close(c.wire.done) })
	return nil
}

func bindBridgeLink(t *testing.T, client, server *link.Session, clientPacket, serverPacket link.PacketConn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	serverErr := make(chan error, 1)
	go func() {
		hello, err := link.ReadHello(ctx, serverPacket)
		if err == nil {
			err = server.BindServer(ctx, serverPacket, hello)
		}
		serverErr <- err
	}()
	if err := client.BindClient(ctx, clientPacket); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func waitBridgeCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func startEchoServer(t *testing.T, ctx context.Context) (net.Listener, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go serveEcho(ctx, listener)
	return listener, listener.Addr().String()
}

func serveEcho(ctx context.Context, listener net.Listener) {
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go func() {
			defer conn.Close()
			_, _ = io.Copy(conn, conn)
		}()
	}
}
