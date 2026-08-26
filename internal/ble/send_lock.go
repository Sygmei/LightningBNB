package ble

import (
	"context"
	"errors"
	"sync"
	"time"
)

var errPacketSendClosed = errors.New("BLE packet connection closed while waiting to send")

func lockPacketSend(ctx context.Context, done <-chan struct{}, mu *sync.Mutex) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if mu.TryLock() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return errPacketSendClosed
		case <-ticker.C:
		}
	}
}
