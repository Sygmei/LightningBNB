package ble

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLockPacketSendHonorsContextWhileBusy(t *testing.T) {
	var mu packetSendLock
	if err := lockPacketSend(context.Background(), make(chan struct{}), &mu); err != nil {
		t.Fatal(err)
	}
	defer mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := lockPacketSend(ctx, make(chan struct{}), &mu); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lockPacketSend error = %v, want context deadline", err)
	}
}

func TestPacketSendLockDoesNotStarveProbe(t *testing.T) {
	var mu packetSendLock
	done := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		first := true
		for ctx.Err() == nil && lockPacketSend(ctx, done, &mu) == nil {
			if first {
				close(started)
				first = false
			}
			// Match a continuous BLE writer holding the lock for most of each
			// packet, with immediate reacquisition between writes.
			time.Sleep(time.Millisecond)
			mu.Unlock()
		}
	}()
	<-started
	for range 20 {
		probeCtx, stop := context.WithTimeout(ctx, 100*time.Millisecond)
		err := lockPacketSend(probeCtx, done, &mu)
		stop()
		if err != nil {
			cancel()
			<-stopped
			t.Fatalf("probe starved by bulk sender: %v", err)
		}
		mu.Unlock()
	}
	cancel()
	<-stopped
}

func TestPacketSendLockRejectsCanceledOrClosed(t *testing.T) {
	var mu packetSendLock
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := lockPacketSend(ctx, make(chan struct{}), &mu); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled send = %v", err)
	}
	done := make(chan struct{})
	close(done)
	if err := lockPacketSend(context.Background(), done, &mu); !errors.Is(err, errPacketSendClosed) {
		t.Fatalf("closed send = %v", err)
	}
}
