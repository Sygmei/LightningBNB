package link

import "context"

// PacketConn is the small message-oriented transport supplied by the BLE GATT
// adapter. Send calls are serialized by Session.
type PacketConn interface {
	Send(context.Context, []byte) error
	Receive() <-chan []byte
	MTU() int
	Close() error
}
