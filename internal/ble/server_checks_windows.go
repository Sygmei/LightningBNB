//go:build windows

package ble

import "tinygo.org/x/bluetooth"

func checkServerAdapter(adapter *bluetooth.Adapter) error {
	return adapter.CheckPeripheralRole()
}

// RecoverServerAdapter resets the per-user Windows Bluetooth radio state. It
// does not require elevation, but briefly disconnects other Bluetooth links.
func RecoverServerAdapter() error {
	return bluetooth.DefaultAdapter.ResetRadio()
}
