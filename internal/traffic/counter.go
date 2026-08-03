package traffic

import "sync/atomic"

// Snapshot contains process-relative forwarded TCP payload totals. TX is data
// sent across the Bluetooth bridge and RX is data received from it.
type Snapshot struct {
	TX uint64
	RX uint64
}

// Counter tracks forwarded TCP payload without including multiplexing, link,
// GATT, or Bluetooth protocol overhead.
type Counter struct {
	tx atomic.Uint64
	rx atomic.Uint64
}

func (c *Counter) AddTX(bytes uint64) {
	c.tx.Add(bytes)
}

func (c *Counter) AddRX(bytes uint64) {
	c.rx.Add(bytes)
}

func (c *Counter) Snapshot() Snapshot {
	return Snapshot{
		TX: c.tx.Load(),
		RX: c.rx.Load(),
	}
}
