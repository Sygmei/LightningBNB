//go:build linux || windows

package ble

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync"

	"github.com/Sygmei/LightningBNB/internal/link"
	"tinygo.org/x/bluetooth"
)

type peripheralServer struct {
	adapter       *bluetooth.Adapter
	service       bluetooth.Service
	advertisement *bluetooth.Advertisement
	tx            bluetooth.Characteristic

	connections chan link.PacketConn
	done        chan struct{}
	once        sync.Once
	mu          sync.Mutex
	current     *serverPacketConn
}

func StartServer(ctx context.Context, name string) (PeripheralListener, error) {
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
		if connected {
			server.ensureConnection()
			return
		}
		server.closeCurrent()
	})
	server.service = bluetooth.Service{
		UUID: ServiceUUID,
		Characteristics: []bluetooth.CharacteristicConfig{
			{
				UUID:  RXUUID,
				Flags: bluetooth.CharacteristicWritePermission,
				WriteEvent: func(_ bluetooth.Connection, _ int, packet []byte) {
					server.ensureConnection().push(packet)
				},
			},
			{
				Handle: &server.tx,
				UUID:   TXUUID,
				Flags:  bluetooth.CharacteristicNotifyPermission,
			},
		},
	}
	if err := adapter.AddService(&server.service); err != nil {
		return nil, err
	}

	advertisement := adapter.DefaultAdvertisement()
	options := bluetooth.AdvertisementOptions{}
	if runtime.GOOS == "windows" {
		advertisedName := []byte(name)
		if len(advertisedName) > 16 {
			advertisedName = advertisedName[:16]
		}
		options.ManufacturerData = []bluetooth.ManufacturerDataElement{{
			CompanyID: TestCompanyID,
			Data:      append(append([]byte(nil), marker...), advertisedName...),
		}}
	} else {
		options.LocalName = name
		options.ServiceUUIDs = []bluetooth.UUID{ServiceUUID}
	}
	if err := advertisement.Configure(options); err != nil {
		_ = adapter.RemoveService(&server.service)
		return nil, err
	}
	if err := advertisement.Start(); err != nil {
		_ = adapter.RemoveService(&server.service)
		return nil, err
	}
	server.advertisement = advertisement
	go func() {
		<-ctx.Done()
		_ = server.Close()
	}()
	return server, nil
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
