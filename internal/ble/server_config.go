package ble

import (
	"errors"

	"tinygo.org/x/bluetooth"
)

type ServerStartOptions struct {
	SkipAdapterChecks bool
}

func IsAdvertisementAborted(err error) bool {
	return errors.Is(err, bluetooth.ErrAdvertisementAborted)
}

func transportService(rx, tx, identity *bluetooth.Characteristic, serverID ServerID, onWrite bluetooth.WriteEvent) bluetooth.Service {
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
			{
				Handle: identity,
				UUID:   IdentityUUID,
				Value:  append([]byte(nil), serverID[:]...),
				Flags:  bluetooth.CharacteristicReadPermission,
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
		AdvertisementType: bluetooth.AdvertisingTypeInd,
		LocalName:         name,
		ServiceUUIDs:      []bluetooth.UUID{ServiceUUID},
	}, true
}
