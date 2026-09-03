//go:build !windows

package app

import (
	"context"
	"fmt"
)

func recoverBluetoothServicesElevated(context.Context) error {
	return fmt.Errorf("Windows Bluetooth service recovery is unavailable on this platform")
}
