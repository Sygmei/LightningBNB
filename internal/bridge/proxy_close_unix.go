//go:build !windows

package bridge

import (
	"errors"
	"syscall"
)

func isExpectedNetworkClose(err error) bool {
	return errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE)
}
