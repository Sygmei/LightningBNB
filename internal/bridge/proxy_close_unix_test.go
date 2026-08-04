//go:build !windows

package bridge

import (
	"fmt"
	"syscall"
	"testing"
)

func TestCleanProxyErrorSuppressesUnixConnectionShutdown(t *testing.T) {
	for _, err := range []error{syscall.ECONNABORTED, syscall.ECONNRESET, syscall.EPIPE} {
		if got := cleanProxyError(fmt.Errorf("wrapped network error: %w", err)); got != nil {
			t.Fatalf("cleanProxyError(%v) = %v", err, got)
		}
	}
}
