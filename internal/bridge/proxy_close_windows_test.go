//go:build windows

package bridge

import (
	"fmt"
	"syscall"
	"testing"
)

func TestCleanProxyErrorSuppressesWindowsConnectionShutdown(t *testing.T) {
	for _, err := range []error{syscall.WSAECONNABORTED, syscall.WSAECONNRESET, syscall.ERROR_BROKEN_PIPE} {
		if got := cleanProxyError(fmt.Errorf("wrapped network error: %w", err)); got != nil {
			t.Fatalf("cleanProxyError(%v) = %v", err, got)
		}
	}
}
