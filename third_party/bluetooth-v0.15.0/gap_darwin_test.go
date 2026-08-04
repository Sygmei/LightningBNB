package bluetooth

import (
	"testing"

	"github.com/tinygo-org/cbgo"
)

func TestClearConnectAttemptDoesNotDeleteNewerAttempt(t *testing.T) {
	adapter := &Adapter{}
	first := make(chan cbgo.Peripheral, 1)
	second := make(chan cbgo.Peripheral, 1)
	const id = "test-peripheral"

	adapter.connectMap.Store(id, first)
	adapter.connectMap.Store(id, second)
	clearConnectAttempt(adapter, id, first)

	got, ok := adapter.connectMap.Load(id)
	if !ok || got != second {
		t.Fatalf("newer connection attempt was removed: %v, %t", got, ok)
	}
	clearConnectAttempt(adapter, id, second)
	if _, ok := adapter.connectMap.Load(id); ok {
		t.Fatal("owned connection attempt was not removed")
	}
}
