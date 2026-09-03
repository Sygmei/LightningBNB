//go:build !windows

package ble

import (
	"errors"

	"tinygo.org/x/bluetooth"
)

func checkServerAdapter(*bluetooth.Adapter) error {
	return nil
}

func RecoverServerAdapter() error {
	return errors.New("automatic Bluetooth radio recovery is supported only on Windows")
}
