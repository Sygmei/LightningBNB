//go:build windows

package bluetooth

import (
	"fmt"
	"syscall"
	"unsafe"

	"github.com/go-ole/go-ole"
	"github.com/saltosystems/winrt-go/windows/foundation"
)

// The winrt-go version vendored by this project does not yet generate
// Windows.Devices.Bluetooth.BluetoothAdapter. Keep this small projection here
// so the GATT server can check the role supported by the selected radio before
// attempting to advertise.
const (
	bluetoothAdapterSignature     = "rc(Windows.Devices.Bluetooth.BluetoothAdapter;{7974f04c-5f7a-4a34-9225-a855f84b1a8b})"
	bluetoothAdapterInterfaceGUID = "7974f04c-5f7a-4a34-9225-a855f84b1a8b"
	bluetoothAdapterStaticsGUID   = "8b02fb6a-ac4c-4741-8661-8eab7d17ea9f"
	bluetoothAdapterRuntimeClass  = "Windows.Devices.Bluetooth.BluetoothAdapter"
)

type windowsBluetoothAdapter struct {
	ole.IUnknown
}

type iWindowsBluetoothAdapter struct {
	ole.IInspectable
}

type iWindowsBluetoothAdapterVtbl struct {
	ole.IInspectableVtbl

	GetDeviceId                        uintptr
	GetBluetoothAddress                uintptr
	GetIsClassicSupported              uintptr
	GetIsLowEnergySupported            uintptr
	GetIsPeripheralRoleSupported       uintptr
	GetIsCentralRoleSupported          uintptr
	GetIsAdvertisementOffloadSupported uintptr
	GetRadioAsync                      uintptr
}

func (v *iWindowsBluetoothAdapter) VTable() *iWindowsBluetoothAdapterVtbl {
	return (*iWindowsBluetoothAdapterVtbl)(unsafe.Pointer(v.RawVTable))
}

func (a *windowsBluetoothAdapter) IsLowEnergySupported() (bool, error) {
	return a.getBool(func(v *iWindowsBluetoothAdapterVtbl) uintptr {
		return v.GetIsLowEnergySupported
	})
}

func (a *windowsBluetoothAdapter) IsPeripheralRoleSupported() (bool, error) {
	return a.getBool(func(v *iWindowsBluetoothAdapterVtbl) uintptr {
		return v.GetIsPeripheralRoleSupported
	})
}

func (a *windowsBluetoothAdapter) getBool(method func(*iWindowsBluetoothAdapterVtbl) uintptr) (bool, error) {
	itf := a.MustQueryInterface(ole.NewGUID(bluetoothAdapterInterfaceGUID))
	defer itf.Release()
	v := (*iWindowsBluetoothAdapter)(unsafe.Pointer(itf))

	var out bool
	hr, _, _ := syscall.SyscallN(
		method(v.VTable()),
		uintptr(unsafe.Pointer(v)),
		uintptr(unsafe.Pointer(&out)),
	)
	if hr != 0 {
		return false, ole.NewError(hr)
	}
	return out, nil
}

type iWindowsBluetoothAdapterStatics struct {
	ole.IInspectable
}

type iWindowsBluetoothAdapterStaticsVtbl struct {
	ole.IInspectableVtbl

	GetDeviceSelector uintptr
	FromIdAsync       uintptr
	GetDefaultAsync   uintptr
}

func (v *iWindowsBluetoothAdapterStatics) VTable() *iWindowsBluetoothAdapterStaticsVtbl {
	return (*iWindowsBluetoothAdapterStaticsVtbl)(unsafe.Pointer(v.RawVTable))
}

func windowsBluetoothAdapterGetDefaultAsync() (*foundation.IAsyncOperation, error) {
	inspectable, err := ole.RoGetActivationFactory(
		bluetoothAdapterRuntimeClass,
		ole.NewGUID(bluetoothAdapterStaticsGUID),
	)
	if err != nil {
		return nil, err
	}
	defer inspectable.Release()

	v := (*iWindowsBluetoothAdapterStatics)(unsafe.Pointer(inspectable))
	var out *foundation.IAsyncOperation
	hr, _, _ := syscall.SyscallN(
		v.VTable().GetDefaultAsync,
		uintptr(unsafe.Pointer(v)),
		uintptr(unsafe.Pointer(&out)),
	)
	if hr != 0 {
		return nil, ole.NewError(hr)
	}
	return out, nil
}

func checkWindowsBluetoothPeripheralRole() error {
	operation, err := windowsBluetoothAdapterGetDefaultAsync()
	if err != nil {
		return fmt.Errorf("get default Windows Bluetooth adapter: %w", err)
	}
	defer operation.Release()

	if err := awaitAsyncOperation(operation, bluetoothAdapterSignature); err != nil {
		return fmt.Errorf("query default Windows Bluetooth adapter: %w", err)
	}
	result, err := operation.GetResults()
	if err != nil {
		return fmt.Errorf("get default Windows Bluetooth adapter: %w", err)
	}
	if result == nil {
		return fmt.Errorf("get default Windows Bluetooth adapter returned no adapter")
	}
	adapter := (*windowsBluetoothAdapter)(result)
	defer adapter.Release()

	lowEnergy, err := adapter.IsLowEnergySupported()
	if err != nil {
		return fmt.Errorf("query Windows Bluetooth LE support: %w", err)
	}
	peripheral, err := adapter.IsPeripheralRoleSupported()
	if err != nil {
		return fmt.Errorf("query Windows Bluetooth peripheral-role support: %w", err)
	}
	if !lowEnergy {
		return fmt.Errorf("default Windows Bluetooth adapter does not support Bluetooth LE")
	}
	if !peripheral {
		return fmt.Errorf("default Windows Bluetooth adapter does not support the BLE peripheral role required by the GATT server")
	}
	return nil
}
