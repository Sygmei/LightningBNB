package traffic

import "testing"

func TestCounterSnapshot(t *testing.T) {
	var counter Counter
	counter.AddTX(12)
	counter.AddTX(3)
	counter.AddRX(7)

	if got := counter.Snapshot(); got != (Snapshot{TX: 15, RX: 7}) {
		t.Fatalf("snapshot = %+v", got)
	}
}
