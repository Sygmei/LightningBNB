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

	connections             chan link.PacketConn
	done                    chan struct{}
	once                    sync.Once
	mu                      sync.Mutex
	current                 *serverPacketConn
	advertisementMu         sync.Mutex
	restartingAdvertisement bool
}

func StartServer(ctx context.Context, name string, serverID ServerID) (PeripheralListener, error) {
	adapter := bluetooth.DefaultAdapter
	if err := adapter.Enable(); err != nil {
		return nil, err
	}
	server := &peripheralServer{
		adapter:     adapter,
		connections: make(chan link.PacketConn, 1),
		done:        make(chan struct{}),
	}
	adapter.SetConnectHandler(func(_ bluetooth.Device, connected bool) {
		if !connected {
			server.closeCurrent()
			if runtime.GOOS == "windows" {
				server.scheduleAdvertisementRestart()
			}
		}
	})
	server.service = transportService(&server.rx, &server.tx, &server.identity, serverID, func(_ bluetooth.Connection, _ int, packet []byte) {
		server.ensureConnection().push(packet)
	})
	if err := adapter.AddService(&server.service); err != nil {
		return nil, err
	}

	if options, start := genericAdvertisementOptions(runtime.GOOS, name); start {
		advertisement := adapter.DefaultAdvertisement()
		if err := advertisement.Configure(options); err != nil {
			_ = advertisement.Stop()
			_ = adapter.RemoveService(&server.service)
			return nil, err
		}
		if err := advertisement.Start(); err != nil {
			// WinRT publisher startup is asynchronous. Stop the partially
			// created publisher before removing the service so a retry does not
			// inherit an operation that is still being torn down.
			_ = advertisement.Stop()
			_ = adapter.RemoveService(&server.service)
			return nil, err
		}
		server.advertisement = advertisement
	}
	go func() {
		<-ctx.Done()
		_ = server.Close()
	}()
	return server, nil
}

// scheduleAdvertisementRestart handles Windows/WinRT publishers that stop
// advertising after a link disconnect without reporting a fatal process error.
// The short stop/start gap also gives the OS time to finish the prior async
// publisher operation before accepting a new connection.
func (s *peripheralServer) scheduleAdvertisementRestart() {
	s.advertisementMu.Lock()
	advertisement := s.advertisement
	if advertisement == nil || s.restartingAdvertisement {
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
		_ = advertisement.Stop()
		timer.Reset(750 * time.Millisecond)
		select {
		case <-s.done:
			return
		case <-timer.C:
		}
		_ = advertisement.Start()
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
