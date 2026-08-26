package ble

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestLockPacketSendHonorsContextWhileBusy(t *testing.T) {
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := lockPacketSend(ctx, make(chan struct{}), &mu); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lockPacketSend error = %v, want context deadline", err)
	}
}
