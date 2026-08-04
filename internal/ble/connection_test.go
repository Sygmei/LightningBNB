package ble

import (
	"runtime"
	"testing"

	"tinygo.org/x/bluetooth"
)

func TestPendingConnectionCanOnlyBeTakenOnce(t *testing.T) {
	native := bluetooth.Device{}
	pending := newPendingConnection(&native)
	if _, ok := pending.take(); !ok {
		t.Fatal("first take did not return the pending connection")
	}
	if _, ok := pending.take(); ok {
		t.Fatal("pending connection was returned more than once")
	}
}

func TestValidateConnectedDeviceRejectsEmptyBackendResult(t *testing.T) {
	var address bluetooth.Address
	if runtime.GOOS == "darwin" {
		address.Set("8ae2e0e5-a073-5403-6327-327ff30c885b")
	} else {
		address.Set("01:02:03:04:05:06")
	}
	if err := validateConnectedDevice(bluetooth.Device{}, address); err == nil {
		t.Fatal("empty backend device was accepted")
	}
	if err := validateConnectedDevice(bluetooth.Device{Address: address}, address); err != nil {
		t.Fatalf("connected device was rejected: %v", err)
	}
}
