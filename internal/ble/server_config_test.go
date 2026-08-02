package ble

import (
	"testing"

	"tinygo.org/x/bluetooth"
)

func TestTransportServiceRegistersBothCharacteristicHandles(t *testing.T) {
	var rx bluetooth.Characteristic
	var tx bluetooth.Characteristic
	onWrite := func(bluetooth.Connection, int, []byte) {}

	service := transportService(&rx, &tx, onWrite)
	if service.UUID != ServiceUUID {
		t.Fatalf("service UUID = %s", service.UUID)
	}
	if len(service.Characteristics) != 2 {
		t.Fatalf("characteristic count = %d", len(service.Characteristics))
	}
	rxConfig := service.Characteristics[0]
	if rxConfig.UUID != RXUUID || rxConfig.Handle != &rx {
		t.Fatal("RX characteristic is not registered with its handle")
	}
	if rxConfig.Flags != bluetooth.CharacteristicWritePermission || rxConfig.WriteEvent == nil {
		t.Fatal("RX characteristic does not support handled writes with response")
	}
	txConfig := service.Characteristics[1]
	if txConfig.UUID != TXUUID || txConfig.Handle != &tx {
		t.Fatal("TX characteristic is not registered with its handle")
	}
	if txConfig.Flags != bluetooth.CharacteristicNotifyPermission {
		t.Fatal("TX characteristic does not support notifications")
	}
}

func TestWindowsUsesOnlyGattServiceAdvertisement(t *testing.T) {
	if _, start := genericAdvertisementOptions("windows", "LightningBNB"); start {
		t.Fatal("Windows must not start a separate non-connectable advertisement")
	}
}

func TestLinuxGenericAdvertisementIncludesServiceAndName(t *testing.T) {
	options, start := genericAdvertisementOptions("linux", "Test bridge")
	if !start {
		t.Fatal("Linux generic advertisement was disabled")
	}
	if options.LocalName != "Test bridge" {
		t.Fatalf("local name = %q", options.LocalName)
	}
	if len(options.ServiceUUIDs) != 1 || options.ServiceUUIDs[0] != ServiceUUID {
		t.Fatalf("service UUIDs = %v", options.ServiceUUIDs)
	}
}
