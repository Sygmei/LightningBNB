package ble

import (
	"bytes"
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
	if !bytes.Equal(service.ServiceData, marker) {
		t.Fatalf("service data = %q", service.ServiceData)
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

func TestServiceDataAdvertisementIsDiscoverable(t *testing.T) {
	payload := testAdvertisementPayload{
		serviceData: []bluetooth.ServiceDataElement{{UUID: ServiceUUID, Data: append([]byte(nil), marker...)}},
	}
	if !isLightningBNB(payload) {
		t.Fatal("LightningBNB service-data advertisement was not recognized")
	}
	payload.serviceData[0].Data = []byte("other")
	if isLightningBNB(payload) {
		t.Fatal("foreign service data was recognized as LightningBNB")
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

type testAdvertisementPayload struct {
	serviceUUIDs     []bluetooth.UUID
	serviceData      []bluetooth.ServiceDataElement
	manufacturerData []bluetooth.ManufacturerDataElement
}

func (p testAdvertisementPayload) LocalName() string { return "" }

func (p testAdvertisementPayload) HasServiceUUID(want bluetooth.UUID) bool {
	for _, uuid := range p.serviceUUIDs {
		if uuid == want {
			return true
		}
	}
	return false
}

func (p testAdvertisementPayload) ServiceUUIDs() []bluetooth.UUID { return p.serviceUUIDs }
func (p testAdvertisementPayload) Bytes() []byte                  { return nil }

func (p testAdvertisementPayload) ManufacturerData() []bluetooth.ManufacturerDataElement {
	return p.manufacturerData
}

func (p testAdvertisementPayload) ServiceData() []bluetooth.ServiceDataElement {
	return p.serviceData
}
