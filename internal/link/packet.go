package link

import "context"

// PacketConn is the small message-oriented transport supplied by the BLE GATT
// adapter. Implementations serialize Send calls and must honor their context
// while waiting for the write path.
type PacketConn interface {
	Send(context.Context, []byte) error
	Receive() <-chan []byte
	MTU() int
	Close() error
}
