package ble

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"tinygo.org/x/bluetooth"
)

type clientPacketConn struct {
	id       string
	serverID string
	device   bluetooth.Device
	rx       bluetooth.DeviceCharacteristic
	tx       bluetooth.DeviceCharacteristic
	mtu      int

	incoming chan []byte
	receive  chan []byte
	done     chan struct{}
	once     sync.Once
	sendMu   sync.Mutex
	withACK  bool
	onClose  func()
}

func newClientPacketConn(id, serverID string, device bluetooth.Device, rx, tx bluetooth.DeviceCharacteristic) *clientPacketConn {
	return &clientPacketConn{
		id:       id,
		serverID: serverID,
		device:   device,
		rx:       rx,
		tx:       tx,
		mtu:      min(packetMTU(rx), packetMTU(tx)),
		incoming: make(chan []byte, 128),
		receive:  make(chan []byte, 128),
		done:     make(chan struct{}),
	}
}

func (c *clientPacketConn) start() error {
	if err := c.tx.EnableNotifications(func(packet []byte) {
		copyOfPacket := append([]byte(nil), packet...)
		select {
		case c.incoming <- copyOfPacket:
		case <-c.done:
		default:
			// The reliable link will request retransmission when its ACK does not advance.
		}
	}); err != nil {
		return err
	}
	go c.forward()
	go c.monitor()
	return nil
}

func (c *clientPacketConn) forward() {
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

func (c *clientPacketConn) monitor() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			connected, err := c.device.Connected()
			if err != nil || !connected {
				c.close(false)
				return
			}
		}
	}
}

func (c *clientPacketConn) Send(ctx context.Context, packet []byte) error {
	if len(packet) > c.mtu {
		return errors.New("BLE packet exceeds negotiated MTU")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return io.ErrClosedPipe
	default:
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	var err error
	if c.withACK {
		_, err = c.rx.Write(packet)
	} else {
		_, err = c.rx.WriteWithoutResponse(packet)
		if err != nil {
			// Older LightningBNB servers exposed only ATT writes with response.
			// Preserve compatibility and keep using that mode for this binding.
			if _, fallbackErr := c.rx.Write(packet); fallbackErr == nil {
				c.withACK = true
				err = nil
			} else {
				err = fallbackErr
			}
		}
	}
	if err != nil {
		c.close(false)
	}
	return err
}

func (c *clientPacketConn) Receive() <-chan []byte { return c.receive }
func (c *clientPacketConn) MTU() int               { return c.mtu }
func (c *clientPacketConn) Close() error {
	c.close(true)
	return nil
}

func (c *clientPacketConn) close(disconnect bool) {
	c.once.Do(func() {
		close(c.done)
		_ = c.tx.EnableNotifications(nil)
		if disconnect {
			_ = c.device.Disconnect()
		}
		if c.onClose != nil {
			c.onClose()
		}
	})
}
