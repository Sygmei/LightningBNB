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
	writes := bluetooth.CharacteristicWritePermission | bluetooth.CharacteristicWriteWithoutResponsePermission
	if rxConfig.Flags != writes || rxConfig.WriteEvent == nil {
		t.Fatal("RX characteristic does not support handled writes with and without response")
	}
	txConfig := service.Characteristics[1]
	if txConfig.UUID != TXUUID || txConfig.Handle != &tx {
		t.Fatal("TX characteristic is not registered with its handle")
	}
	if txConfig.Flags != bluetooth.CharacteristicNotifyPermission {
		t.Fatal("TX characteristic does not support notifications")
	}
}

func TestAdvertisementDetailsAndDeviceMerge(t *testing.T) {
	payload := testAdvertisementPayload{
		serviceUUIDs: []bluetooth.UUID{ServiceUUID},
		serviceData:  []bluetooth.ServiceDataElement{{UUID: ServiceUUID, Data: append([]byte(nil), marker...)}},
		manufacturerData: []bluetooth.ManufacturerDataElement{{
			CompanyID: TestCompanyID,
			Data:      []byte("legacy"),
		}},
	}
	services, serviceData, manufacturerData := advertisementDetails(payload)
	if len(services) != 1 || services[0] != ServiceUUIDString {
		t.Fatalf("services = %v", services)
	}
	if len(serviceData) != 1 || serviceData[0] != ServiceUUIDString+"=4c424e4231" {
		t.Fatalf("service data = %v", serviceData)
	}
	if len(manufacturerData) != 1 || manufacturerData[0] != "ffff=6c6567616379" {
		t.Fatalf("manufacturer data = %v", manufacturerData)
	}

	merged := mergeDevice(
		Device{Name: "named", LightningBNB: true, ServiceUUIDs: services},
		Device{RSSI: -20, ServiceData: serviceData},
	)
	if merged.Name != "named" || !merged.LightningBNB || merged.RSSI != -20 {
		t.Fatalf("merged device = %+v", merged)
	}
	if len(merged.ServiceUUIDs) != 1 || len(merged.ServiceData) != 1 {
		t.Fatalf("merged payload = %+v", merged)
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
