//go:build !linux && !windows

package ble

import (
	"context"
	"errors"
)

var ErrServerUnsupported = errors.New("Bluetooth server mode is supported only on Windows and Linux")

func StartServer(context.Context, string, ServerID) (PeripheralListener, error) {
	return nil, ErrServerUnsupported
}
