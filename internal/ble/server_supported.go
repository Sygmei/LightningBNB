//go:build linux || windows

package ble

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/Sygmei/LightningBNB/internal/link"
	"tinygo.org/x/bluetooth"
)

type peripheralServer struct {
	adapter       *bluetooth.Adapter
	service       bluetooth.Service
	advertisement *bluetooth.Advertisement
	rx            bluetooth.Characteristic
	tx            bluetooth.Characteristic
	identity      bluetooth.Characteristic

	connections                 chan link.PacketConn
	done                        chan struct{}
	once                        sync.Once
	mu                          sync.Mutex
	current                     *serverPacketConn
	advertisementMu             sync.Mutex
	restartingAdvertisement     bool
	restartServiceAdvertisement func() error
	logf                        func(string, ...any)
}

func StartServer(ctx context.Context, name string, serverID ServerID) (PeripheralListener, error) {
	return StartServerWithOptions(ctx, name, serverID, nil, ServerStartOptions{})
}

// StartServerWithLogger starts the local GATT server and reports native
// advertising lifecycle events through logf. The callback is optional.
func StartServerWithLogger(ctx context.Context, name string, serverID ServerID, logf func(string, ...any)) (PeripheralListener, error) {
	return StartServerWithOptions(ctx, name, serverID, logf, ServerStartOptions{})
}

// StartServerWithOptions starts the local GATT server with optional native
// adapter checks.
func StartServerWithOptions(ctx context.Context, name string, serverID ServerID, logf func(string, ...any), options ServerStartOptions) (PeripheralListener, error) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	adapter := bluetooth.DefaultAdapter
	logf("BLE: enabling adapter")
	if err := adapter.Enable(); err != nil {
		return nil, err
	}
	if options.SkipAdapterChecks {
		logf("BLE: adapter initialized; skipping BLE peripheral-role capability check")
	} else {
		if err := checkServerAdapter(adapter); err != nil {
			return nil, err
		}
		logf("BLE: adapter initialized; BLE peripheral-role capability check passed")
	}
	server := &peripheralServer{
		adapter:     adapter,
		connections: make(chan link.PacketConn, 1),
		done:        make(chan struct{}),
		logf:        logf,
	}
	adapter.SetConnectHandler(func(device bluetooth.Device, connected bool) {
		if connected {
			logf("BLE: central connected (%s)", device.Address.String())
			return
		}
		logf("BLE: central disconnected (%s)", device.Address.String())
		server.closeCurrent()
		if runtime.GOOS == "windows" {
			server.scheduleAdvertisementRestart()
		}
	})
	logf("BLE: registering GATT service %s with 3 characteristics", ServiceUUIDString)
	server.service = transportService(&server.rx, &server.tx, &server.identity, serverID, func(_ bluetooth.Connection, _ int, packet []byte) {
		server.ensureConnection().push(packet)
	})
	if err := adapter.AddService(&server.service); err != nil {
		logf("BLE: GATT service registration/advertising failed: %v", err)
		return nil, err
	}
	server.restartServiceAdvertisement = func() error {
		return adapter.RestartServiceAdvertisement(&server.service)
	}
	logf("BLE: GATT service registered; native advertising setup completed")

	if options, start := genericAdvertisementOptions(runtime.GOOS, name); start {
		advertisement := adapter.DefaultAdvertisement()
		if err := advertisement.Configure(options); err != nil {
			if cleanupErr := advertisement.Stop(); cleanupErr != nil {
				logf("BLE: generic advertisement cleanup after configure failure: %v", cleanupErr)
			}
			if cleanupErr := adapter.RemoveService(&server.service); cleanupErr != nil {
				logf("BLE: GATT service cleanup after advertisement configure failure: %v", cleanupErr)
			}
			return nil, err
		}
		if err := advertisement.Start(); err != nil {
			// WinRT publisher startup is asynchronous. Stop the partially
			// created publisher before removing the service so a retry does not
			// inherit an operation that is still being torn down.
			if cleanupErr := advertisement.Stop(); cleanupErr != nil {
				logf("BLE: generic advertisement cleanup after start failure: %v", cleanupErr)
			}
			if cleanupErr := adapter.RemoveService(&server.service); cleanupErr != nil {
				logf("BLE: GATT service cleanup after advertisement start failure: %v", cleanupErr)
			}
			return nil, err
		}
		server.advertisement = advertisement
		logf("BLE: generic advertisement started as %q", name)
	} else {
		logf("BLE: using WinRT connectable GATT advertisement; local name may be omitted")
	}
	go func() {
		<-ctx.Done()
		_ = server.Close()
	}()
	return server, nil
}

// scheduleAdvertisementRestart handles Windows/WinRT GATT providers that stop
// advertising after a link disconnect without reporting a fatal process error.
// The backend waits for the provider's stopped/started status around the
// restart rather than relying only on a fixed delay.
func (s *peripheralServer) scheduleAdvertisementRestart() {
	s.advertisementMu.Lock()
	restart := s.restartServiceAdvertisement
	if restart == nil || s.restartingAdvertisement {
		s.advertisementMu.Unlock()
		return
	}
	s.restartingAdvertisement = true
	s.advertisementMu.Unlock()
	go func() {
		defer func() {
			s.advertisementMu.Lock()
			s.restartingAdvertisement = false
			s.advertisementMu.Unlock()
		}()
		timer := time.NewTimer(750 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-s.done:
			return
		case <-timer.C:
		}
		for attempt := 1; attempt <= 3; attempt++ {
			select {
			case <-s.done:
				return
			default:
			}
			if err := restart(); err == nil {
				s.logf("BLE: native advertisement restart succeeded on attempt %d", attempt)
				return
			} else {
				s.logf("BLE: native advertisement restart attempt %d/3 failed: %v", attempt, err)
			}
			if attempt < 3 {
				timer.Reset(time.Duration(attempt) * time.Second)
				select {
				case <-s.done:
					return
				case <-timer.C:
				}
			}
		}
	}()
}

func (s *peripheralServer) Accept(ctx context.Context) (link.PacketConn, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.done:
		return nil, io.ErrClosedPipe
	case conn := <-s.connections:
		return conn, nil
	}
}

func (s *peripheralServer) Close() error {
	var firstErr error
	s.once.Do(func() {
		close(s.done)
		s.closeCurrent()
		if s.advertisement != nil {
			if err := s.advertisement.Stop(); err != nil {
				firstErr = err
			}
		}
		if err := s.adapter.RemoveService(&s.service); err != nil && firstErr == nil {
			firstErr = err
		}
	})
	return firstErr
}

func (s *peripheralServer) ensureConnection() *serverPacketConn {
	s.mu.Lock()
	if s.current != nil && !s.current.isClosed() {
		conn := s.current
		s.mu.Unlock()
		return conn
	}
	conn := newServerPacketConn(&s.tx)
	conn.onClose = func() {
		s.mu.Lock()
		if s.current == conn {
			s.current = nil
		}
		s.mu.Unlock()
	}
	s.current = conn
	s.mu.Unlock()
	select {
	case s.connections <- conn:
	case <-s.done:
		conn.close()
	default:
		conn.close()
	}
	return conn
}

func (s *peripheralServer) closeCurrent() {
	s.mu.Lock()
	conn := s.current
	s.current = nil
	s.mu.Unlock()
	if conn != nil {
		conn.close()
	}
}

type serverPacketConn struct {
	tx       *bluetooth.Characteristic
	incoming chan []byte
	receive  chan []byte
	done     chan struct{}
	once     sync.Once
	onClose  func()
	sendMu   sync.Mutex
}

func newServerPacketConn(tx *bluetooth.Characteristic) *serverPacketConn {
	conn := &serverPacketConn{
		tx:       tx,
		incoming: make(chan []byte, 128),
		receive:  make(chan []byte, 128),
		done:     make(chan struct{}),
	}
	go conn.forward()
	return conn
}

func (c *serverPacketConn) push(packet []byte) {
	copyOfPacket := append([]byte(nil), packet...)
	select {
	case c.incoming <- copyOfPacket:
	case <-c.done:
	default:
		// The reliable layer will retransmit when no ACK is returned.
	}
}

func (c *serverPacketConn) forward() {
	defer close(c.receive)
	for {
		select {
		case <-c.done:
			return
		case packet := <-c.incoming:
			select {
			case c.receive <- packet:
			case <-c.done:
				return
			}
		}
	}
}

func (c *serverPacketConn) Send(ctx context.Context, packet []byte) error {
	if len(packet) > maxPacketMTU {
		return errors.New("BLE packet exceeds maximum MTU")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return io.ErrClosedPipe
	default:
	}
	if err := lockPacketSend(ctx, c.done, &c.sendMu); err != nil {
		return err
	}
	defer c.sendMu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return io.ErrClosedPipe
	default:
	}
	_, err := c.tx.Write(packet)
	return err
}

func (c *serverPacketConn) Receive() <-chan []byte { return c.receive }
func (c *serverPacketConn) MTU() int               { return maxPacketMTU }
func (c *serverPacketConn) Close() error {
	c.close()
	return nil
}

func (c *serverPacketConn) close() {
	c.once.Do(func() {
		close(c.done)
		if c.onClose != nil {
			c.onClose()
		}
	})
}

func (c *serverPacketConn) isClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}
