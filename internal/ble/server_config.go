package ble

import "tinygo.org/x/bluetooth"

func transportService(rx, tx *bluetooth.Characteristic, onWrite bluetooth.WriteEvent) bluetooth.Service {
	return bluetooth.Service{
		UUID: ServiceUUID,
		Characteristics: []bluetooth.CharacteristicConfig{
			{
				Handle:     rx,
				UUID:       RXUUID,
				Flags:      bluetooth.CharacteristicWritePermission | bluetooth.CharacteristicWriteWithoutResponsePermission,
				WriteEvent: onWrite,
			},
			{
				Handle: tx,
				UUID:   TXUUID,
				Flags:  bluetooth.CharacteristicNotifyPermission,
			},
		},
	}
}

func genericAdvertisementOptions(goos, name string) (bluetooth.AdvertisementOptions, bool) {
	if goos == "windows" {
		// AddService uses WinRT's GattServiceProvider, which starts its own
		// connectable service advertisement. A separate
		// BluetoothLEAdvertisementPublisher is not connectable and CoreBluetooth
		// may expose it as a different peripheral, causing macOS clients to time
		// out while connecting to the marker advertisement.
		return bluetooth.AdvertisementOptions{}, false
	}
	return bluetooth.AdvertisementOptions{
		LocalName:    name,
		ServiceUUIDs: []bluetooth.UUID{ServiceUUID},
	}, true
}
