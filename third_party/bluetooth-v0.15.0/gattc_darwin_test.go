package bluetooth

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestWaitForWriteWithoutResponseUsesReadySignal(t *testing.T) {
	ready := make(chan struct{}, 1)
	var canSend atomic.Bool
	done := make(chan error, 1)
	go func() {
		done <- waitForWriteWithoutResponse(canSend.Load, ready, time.Second)
	}()

	canSend.Store(true)
	ready <- struct{}{}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWaitForWriteWithoutResponseRecoversMissedReadySignal(t *testing.T) {
	var canSend atomic.Bool
	go func() {
		time.Sleep(5 * time.Millisecond)
		canSend.Store(true)
	}()

	if err := waitForWriteWithoutResponse(canSend.Load, make(chan struct{}), time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestWaitForWriteWithoutResponseTimesOut(t *testing.T) {
	err := waitForWriteWithoutResponse(func() bool { return false }, make(chan struct{}), 5*time.Millisecond)
	if !errors.Is(err, errWriteWithoutResponseTimeout) {
		t.Fatalf("timeout error = %v", err)
	}
}
