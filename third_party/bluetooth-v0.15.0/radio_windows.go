package bluetooth

import (
	"fmt"
	"runtime"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/go-ole/go-ole"
	"github.com/saltosystems/winrt-go/windows/foundation"
	"github.com/saltosystems/winrt-go/windows/foundation/collections"
)

const (
	windowsRadioRuntimeClass = "Windows.Devices.Radios.Radio"
	windowsRadioInterfaceID  = "252118df-b33e-416a-875f-1cf38ae2d83e"
	windowsRadioStaticsID    = "5fb6a12e-67cb-46ae-aae9-65919f86eff4"
	windowsRadioSignature    = "rc(Windows.Devices.Radios.Radio;{252118df-b33e-416a-875f-1cf38ae2d83e})"
	radioAccessSignature     = "enum(Windows.Devices.Radios.RadioAccessStatus;i4)"

	radioAccessAllowed radioAccessStatus = 1
	radioKindBluetooth radioKind         = 3
	radioStateOn       radioState        = 1
	radioStateOff      radioState        = 2
)

type radioAccessStatus int32
type radioKind int32
type radioState int32

type windowsRadio struct{ ole.IUnknown }
type iWindowsRadio struct{ ole.IInspectable }

type iWindowsRadioVtbl struct {
	ole.IInspectableVtbl
	SetStateAsync      uintptr
	AddStateChanged    uintptr
	RemoveStateChanged uintptr
	GetState           uintptr
	GetName            uintptr
	GetKind            uintptr
}

func (v *iWindowsRadio) VTable() *iWindowsRadioVtbl {
	return (*iWindowsRadioVtbl)(unsafe.Pointer(v.RawVTable))
}

func (r *windowsRadio) interfaceValue() (*iWindowsRadio, func(), error) {
	itf := r.MustQueryInterface(ole.NewGUID(windowsRadioInterfaceID))
	if itf == nil {
		return nil, nil, fmt.Errorf("query Windows radio interface")
	}
	return (*iWindowsRadio)(unsafe.Pointer(itf)), func() { itf.Release() }, nil
}

func (r *windowsRadio) Kind() (radioKind, error) {
	v, release, err := r.interfaceValue()
	if err != nil {
		return 0, err
	}
	defer release()
	var out radioKind
	hr, _, _ := syscall.SyscallN(v.VTable().GetKind, uintptr(unsafe.Pointer(v)), uintptr(unsafe.Pointer(&out)))
	if hr != 0 {
		return 0, ole.NewError(hr)
	}
	return out, nil
}

func (r *windowsRadio) State() (radioState, error) {
	v, release, err := r.interfaceValue()
	if err != nil {
		return 0, err
	}
	defer release()
	var out radioState
	hr, _, _ := syscall.SyscallN(v.VTable().GetState, uintptr(unsafe.Pointer(v)), uintptr(unsafe.Pointer(&out)))
	if hr != 0 {
		return 0, ole.NewError(hr)
	}
	return out, nil
}

func (r *windowsRadio) SetStateAsync(state radioState) (*foundation.IAsyncOperation, error) {
	v, release, err := r.interfaceValue()
	if err != nil {
		return nil, err
	}
	defer release()
	var out *foundation.IAsyncOperation
	hr, _, _ := syscall.SyscallN(
		v.VTable().SetStateAsync,
		uintptr(unsafe.Pointer(v)),
		uintptr(state),
		uintptr(unsafe.Pointer(&out)),
	)
	if hr != 0 {
		return nil, ole.NewError(hr)
	}
	return out, nil
}

type iWindowsRadioStatics struct{ ole.IInspectable }

type iWindowsRadioStaticsVtbl struct {
	ole.IInspectableVtbl
	GetRadiosAsync     uintptr
	GetDeviceSelector  uintptr
	FromIDAsync        uintptr
	RequestAccessAsync uintptr
}

func (v *iWindowsRadioStatics) VTable() *iWindowsRadioStaticsVtbl {
	return (*iWindowsRadioStaticsVtbl)(unsafe.Pointer(v.RawVTable))
}

func windowsRadioStatics() (*iWindowsRadioStatics, func(), error) {
	inspectable, err := ole.RoGetActivationFactory(windowsRadioRuntimeClass, ole.NewGUID(windowsRadioStaticsID))
	if err != nil {
		return nil, nil, err
	}
	return (*iWindowsRadioStatics)(unsafe.Pointer(inspectable)), func() { inspectable.Release() }, nil
}

func radioRequestAccessAsync() (*foundation.IAsyncOperation, error) {
	statics, release, err := windowsRadioStatics()
	if err != nil {
		return nil, err
	}
	defer release()
	var out *foundation.IAsyncOperation
	hr, _, _ := syscall.SyscallN(
		statics.VTable().RequestAccessAsync,
		uintptr(unsafe.Pointer(statics)),
		uintptr(unsafe.Pointer(&out)),
	)
	if hr != 0 {
		return nil, ole.NewError(hr)
	}
	return out, nil
}

func radioGetRadiosAsync() (*foundation.IAsyncOperation, error) {
	statics, release, err := windowsRadioStatics()
	if err != nil {
		return nil, err
	}
	defer release()
	var out *foundation.IAsyncOperation
	hr, _, _ := syscall.SyscallN(
		statics.VTable().GetRadiosAsync,
		uintptr(unsafe.Pointer(statics)),
		uintptr(unsafe.Pointer(&out)),
	)
	if hr != 0 {
		return nil, ole.NewError(hr)
	}
	return out, nil
}

func radioAccessResult(operation *foundation.IAsyncOperation) (radioAccessStatus, error) {
	var out radioAccessStatus
	hr, _, _ := syscall.SyscallN(
		operation.VTable().GetResults,
		uintptr(unsafe.Pointer(operation)),
		uintptr(unsafe.Pointer(&out)),
	)
	if hr != 0 {
		return 0, ole.NewError(hr)
	}
	return out, nil
}

func setRadioState(radio *windowsRadio, state radioState) error {
	operation, err := radio.SetStateAsync(state)
	if err != nil {
		return err
	}
	defer operation.Release()
	if err := awaitAsyncOperation(operation, radioAccessSignature); err != nil {
		return err
	}
	access, err := radioAccessResult(operation)
	if err != nil {
		return err
	}
	if access != radioAccessAllowed {
		return fmt.Errorf("Windows denied changing Bluetooth radio state (access=%d)", access)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		current, err := radio.State()
		if err != nil {
			return err
		}
		if current == state {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for Bluetooth radio state %d (current=%d)", state, current)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

var windowsRadioResetMu sync.Mutex

// ResetRadio toggles the Windows Bluetooth radio off and on through the
// supported per-user Radio API. This does not require administrator rights,
// but it briefly disconnects other Bluetooth devices.
func (a *Adapter) ResetRadio() error {
	windowsRadioResetMu.Lock()
	defer windowsRadioResetMu.Unlock()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := a.Enable(); err != nil {
		return err
	}

	accessOperation, err := radioRequestAccessAsync()
	if err != nil {
		return fmt.Errorf("request Windows radio access: %w", err)
	}
	defer accessOperation.Release()
	if err := awaitAsyncOperation(accessOperation, radioAccessSignature); err != nil {
		return fmt.Errorf("request Windows radio access: %w", err)
	}
	access, err := radioAccessResult(accessOperation)
	if err != nil {
		return fmt.Errorf("read Windows radio access result: %w", err)
	}
	if access != radioAccessAllowed {
		return fmt.Errorf("Windows denied Bluetooth radio access (access=%d)", access)
	}

	operation, err := radioGetRadiosAsync()
	if err != nil {
		return fmt.Errorf("enumerate Windows radios: %w", err)
	}
	defer operation.Release()
	vectorSignature := fmt.Sprintf("pinterface({%s};%s)", collections.GUIDIVectorView, windowsRadioSignature)
	if err := awaitAsyncOperation(operation, vectorSignature); err != nil {
		return fmt.Errorf("enumerate Windows radios: %w", err)
	}
	result, err := operation.GetResults()
	if err != nil {
		return fmt.Errorf("read Windows radio list: %w", err)
	}
	if result == nil {
		return fmt.Errorf("Windows returned no radio list")
	}
	radios := (*collections.IVectorView)(result)
	defer radios.Release()
	size, err := radios.GetSize()
	if err != nil {
		return fmt.Errorf("read Windows radio count: %w", err)
	}
	for i := uint32(0); i < size; i++ {
		item, err := radios.GetAt(i)
		if err != nil {
			return fmt.Errorf("read Windows radio %d: %w", i, err)
		}
		if item == nil {
			continue
		}
		radio := (*windowsRadio)(item)
		kind, kindErr := radio.Kind()
		if kindErr != nil {
			radio.Release()
			return fmt.Errorf("read Windows radio %d kind: %w", i, kindErr)
		}
		if kind != radioKindBluetooth {
			radio.Release()
			continue
		}
		if err := setRadioState(radio, radioStateOff); err != nil {
			radio.Release()
			return fmt.Errorf("turn Bluetooth radio off: %w", err)
		}
		// Give bthserv and the vendor stack time to observe the radio transition.
		time.Sleep(1500 * time.Millisecond)
		if err := setRadioState(radio, radioStateOn); err != nil {
			radio.Release()
			return fmt.Errorf("turn Bluetooth radio on: %w", err)
		}
		radio.Release()
		// Give bthserv and the vendor driver time to recreate their GATT state.
		time.Sleep(2500 * time.Millisecond)
		return nil
	}
	return fmt.Errorf("Windows returned no Bluetooth radio")
}
