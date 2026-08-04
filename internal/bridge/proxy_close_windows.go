//go:build windows

package bridge

import (
	"errors"
	"syscall"
)

func isExpectedNetworkClose(err error) bool {
	return errors.Is(err, syscall.WSAECONNABORTED) ||
		errors.Is(err, syscall.WSAECONNRESET) ||
		errors.Is(err, syscall.ERROR_BROKEN_PIPE)
}
