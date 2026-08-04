package traffic

import "sync/atomic"

// Snapshot contains process-relative forwarded TCP payload totals. TX is data
// sent across the Bluetooth bridge and RX is data received from it.
type Snapshot struct {
	TX                 uint64
	RX                 uint64
	CompressionEnabled bool
	TXUncompressed     uint64
	TXCompressed       uint64
	RXUncompressed     uint64
	RXCompressed       uint64
}

// Counter tracks forwarded TCP payload without including multiplexing, link,
// GATT, or Bluetooth protocol overhead. When enabled, the compression fields
// compare original DATA payload bytes with their encoded payload bytes.
type Counter struct {
	tx                 atomic.Uint64
	rx                 atomic.Uint64
	compressionEnabled atomic.Bool
	txUncompressed     atomic.Uint64
	txCompressed       atomic.Uint64
	rxUncompressed     atomic.Uint64
	rxCompressed       atomic.Uint64
}

func (c *Counter) AddTX(bytes uint64) {
	c.tx.Add(bytes)
}

func (c *Counter) AddRX(bytes uint64) {
	c.rx.Add(bytes)
}

func (c *Counter) EnableCompression() {
	c.compressionEnabled.Store(true)
}

func (c *Counter) AddCompressionTX(uncompressed, compressed uint64) {
	c.txUncompressed.Add(uncompressed)
	c.txCompressed.Add(compressed)
}

func (c *Counter) AddCompressionRX(uncompressed, compressed uint64) {
	c.rxUncompressed.Add(uncompressed)
	c.rxCompressed.Add(compressed)
}

func (c *Counter) Snapshot() Snapshot {
	return Snapshot{
		TX:                 c.tx.Load(),
		RX:                 c.rx.Load(),
		CompressionEnabled: c.compressionEnabled.Load(),
		TXUncompressed:     c.txUncompressed.Load(),
		TXCompressed:       c.txCompressed.Load(),
		RXUncompressed:     c.rxUncompressed.Load(),
		RXCompressed:       c.rxCompressed.Load(),
	}
}
