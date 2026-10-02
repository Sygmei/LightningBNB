package ble

import (
	"context"
	"errors"
	"sync"
)

var errPacketSendClosed = errors.New("BLE packet connection closed while waiting to send")

// A channel parks contenders in the send queue. Polling Mutex.TryLock lets
// the bulk sender reacquire between every poll and starve heartbeat probes.
type packetSendLock struct {
	once  sync.Once
	token chan struct{}
}

func (mu *packetSendLock) init() {
	mu.once.Do(func() {
		mu.token = make(chan struct{}, 1)
		mu.token <- struct{}{}
	})
}

func (mu *packetSendLock) Unlock() { mu.token <- struct{}{} }

func lockPacketSend(ctx context.Context, done <-chan struct{}, mu *packetSendLock) error {
	mu.init()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return errPacketSendClosed
	case <-mu.token:
	}
	// Cancellation and the token may become ready together.
	select {
	case <-ctx.Done():
		mu.Unlock()
		return ctx.Err()
	case <-done:
		mu.Unlock()
		return errPacketSendClosed
	default:
		return nil
	}
}
