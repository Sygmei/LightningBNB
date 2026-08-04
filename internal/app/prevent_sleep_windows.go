//go:build windows

package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"

	"golang.org/x/sys/windows"
)

const (
	executionStateSystemRequired = uintptr(0x00000001)
	executionStateContinuous     = uintptr(0x80000000)
)

var setThreadExecutionState = windows.NewLazySystemDLL("kernel32.dll").NewProc("SetThreadExecutionState")

type windowsSleepInhibitor struct {
	done      chan struct{}
	closed    chan error
	closeOnce sync.Once
	closeErr  error
}

func acquireSleepInhibitor(ctx context.Context) (io.Closer, error) {
	inhibitor := &windowsSleepInhibitor{
		done:   make(chan struct{}),
		closed: make(chan error, 1),
	}
	ready := make(chan error, 1)
	go inhibitor.run(ready)

	select {
	case err := <-ready:
		if err != nil {
			return nil, err
		}
		return inhibitor, nil
	case <-ctx.Done():
		close(inhibitor.done)
		<-inhibitor.closed
		return nil, ctx.Err()
	}
}

func (inhibitor *windowsSleepInhibitor) run(ready chan<- error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if err := callSetThreadExecutionState(executionStateContinuous | executionStateSystemRequired); err != nil {
		ready <- fmt.Errorf("set Windows execution state: %w", err)
		inhibitor.closed <- nil
		return
	}
	ready <- nil
	<-inhibitor.done
	inhibitor.closed <- callSetThreadExecutionState(executionStateContinuous)
}

func (inhibitor *windowsSleepInhibitor) Close() error {
	inhibitor.closeOnce.Do(func() {
		close(inhibitor.done)
		inhibitor.closeErr = <-inhibitor.closed
	})
	return inhibitor.closeErr
}

func callSetThreadExecutionState(state uintptr) error {
	result, _, callErr := setThreadExecutionState.Call(state)
	if result != 0 {
		return nil
	}
	if callErr != nil && !errors.Is(callErr, windows.ERROR_SUCCESS) {
		return callErr
	}
	return errors.New("SetThreadExecutionState returned zero")
}
