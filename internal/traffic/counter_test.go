package traffic

import "testing"

func TestCounterSnapshot(t *testing.T) {
	var counter Counter
	counter.AddTX(12)
	counter.AddTX(3)
	counter.AddRX(7)
	counter.EnableCompression()
	counter.AddCompressionTX(12, 5)
	counter.AddCompressionTX(3, 2)
	counter.AddCompressionRX(7, 4)

	if got := counter.Snapshot(); got != (Snapshot{
		TX:                 15,
		RX:                 7,
		CompressionEnabled: true,
		TXUncompressed:     15,
		TXCompressed:       7,
		RXUncompressed:     7,
		RXCompressed:       4,
	}) {
		t.Fatalf("snapshot = %+v", got)
	}
}
