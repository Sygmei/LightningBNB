package bluetooth

import (
	"fmt"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/go-ole/go-ole"
	"github.com/saltosystems/winrt-go"
	winrtbluetooth "github.com/saltosystems/winrt-go/windows/devices/bluetooth"
	"github.com/saltosystems/winrt-go/windows/devices/bluetooth/genericattributeprofile"
	"github.com/saltosystems/winrt-go/windows/foundation"
	"github.com/saltosystems/winrt-go/windows/foundation/collections"
	"github.com/saltosystems/winrt-go/windows/storage/streams"
)

// Characteristic is a single characteristic in a service. It has an UUID and a
// value.
type Characteristic struct {
	wintCharacteristic *genericattributeprofile.GattLocalCharacteristic
	writeEvent         WriteEvent
	flags              CharacteristicPermissions

	valueMtx *sync.Mutex
	value    []byte
}

// AddService creates a new service with the characteristics listed in the
// Service struct.
func (a *Adapter) AddService(s *Service) error {
	gattServiceOp, err := genericattributeprofile.GattServiceProviderCreateAsync(syscallUUIDFromUUID(s.UUID))

	if err != nil {
		return err
	}

	if err = awaitAsyncOperation(gattServiceOp, genericattributeprofile.SignatureGattServiceProviderResult); err != nil {
		return err
	}

	res, err := gattServiceOp.GetResults()
	if err != nil {
		return err
	}
	if res == nil {
		return fmt.Errorf("create GATT service provider returned no result")
	}

	serviceProviderResult := (*genericattributeprofile.GattServiceProviderResult)(res)
	resultError, err := serviceProviderResult.GetError()
	if err != nil {
		return fmt.Errorf("get GATT service provider result: %w", err)
	}
	if resultError != winrtbluetooth.BluetoothErrorSuccess {
		return fmt.Errorf("create GATT service provider failed with Bluetooth error %d", resultError)
	}

	serviceProvider, err := serviceProviderResult.GetServiceProvider()
	if err != nil {
		return err
	}
	if serviceProvider == nil {
		return fmt.Errorf("create GATT service provider returned no provider")
	}
	advertisingStarted := false
	providerStored := false
	defer func() {
		if !providerStored {
			if advertisingStarted {
				_ = serviceProvider.StopAdvertising()
				_ = waitForAdvertisementStatus(serviceProvider, false)
			}
			serviceProvider.Release()
		}
	}()

	localService, err := serviceProvider.GetService()
	if err != nil {
		return err
	}

	// TODO: "ParameterizedInstanceGUID" + "foundation.NewTypedEventHandler"
	// seems to always return the same instance, need to figure out how to get different instances each time...
	// was following c# source for this flow: https://github.com/microsoft/Windows-universal-samples/blob/main/Samples/BluetoothLE/cs/Scenario3_ServerForeground.xaml.cs
	// which relies on instanced event handlers. for now we'll manually setup our handlers with a map of golang characteristics
	//
	// TypedEventHandler<GattLocalCharacteristic,GattWriteRequestedEventArgs>
	guid := winrt.ParameterizedInstanceGUID(
		foundation.GUIDTypedEventHandler,
		genericattributeprofile.SignatureGattLocalCharacteristic,
		genericattributeprofile.SignatureGattWriteRequestedEventArgs)

	goChars := map[syscall.GUID]*Characteristic{}

	writeRequestedHandler := foundation.NewTypedEventHandler(ole.NewGUID(guid), func(instance *foundation.TypedEventHandler, sender, args unsafe.Pointer) {
		writeReqArgs := (*genericattributeprofile.GattWriteRequestedEventArgs)(args)
		reqAsyncOp, err := writeReqArgs.GetRequestAsync()
		if err != nil {
			return
		}

		if err = awaitAsyncOperation(reqAsyncOp, genericattributeprofile.SignatureGattWriteRequest); err != nil {
			return
		}

		res, err := reqAsyncOp.GetResults()
		if err != nil {
			return
		}

		gattWriteRequest := (*genericattributeprofile.GattWriteRequest)(res)

		buf, err := gattWriteRequest.GetValue()
		if err != nil {
			return
		}

		offset, err := gattWriteRequest.GetOffset()
		if err != nil {
			return
		}

		characteristic := (*genericattributeprofile.GattLocalCharacteristic)(sender)
		uuid, err := characteristic.GetUuid()
		if err != nil {
			return
		}

		goChar, ok := goChars[uuid]
		if !ok {
			return
		}

		if goChar.writeEvent != nil {
			// TODO: connection?
			goChar.writeEvent(0, int(offset), bufferToSlice(buf))
		}
		if option, err := gattWriteRequest.GetOption(); err == nil && option == genericattributeprofile.GattWriteOptionWriteWithResponse {
			gattWriteRequest.Respond()
		}
	})

	guid = winrt.ParameterizedInstanceGUID(
		foundation.GUIDTypedEventHandler,
		genericattributeprofile.SignatureGattLocalCharacteristic,
		genericattributeprofile.SignatureGattReadRequestedEventArgs)

	readRequestedHandler := foundation.NewTypedEventHandler(ole.NewGUID(guid), func(instance *foundation.TypedEventHandler, sender, args unsafe.Pointer) {
		readReqArgs := (*genericattributeprofile.GattReadRequestedEventArgs)(args)
		reqAsyncOp, err := readReqArgs.GetRequestAsync()
		if err != nil {
			return
		}

		if err = awaitAsyncOperation(reqAsyncOp, genericattributeprofile.SignatureGattReadRequest); err != nil {
			return
		}

		res, err := reqAsyncOp.GetResults()
		if err != nil {
			return
		}

		gattReadRequest := (*genericattributeprofile.GattReadRequest)(res)

		characteristic := (*genericattributeprofile.GattLocalCharacteristic)(sender)
		uuid, err := characteristic.GetUuid()
		if err != nil {
			return
		}

		goChar, ok := goChars[uuid]
		if !ok {
			return
		}

		writer, err := streams.NewDataWriter()
		if err != nil {
			return
		}
		defer writer.Release()

		goChar.valueMtx.Lock()
		defer goChar.valueMtx.Unlock()
		if len(goChar.value) > 0 {
			if err = writer.WriteBytes(uint32(len(goChar.value)), goChar.value); err != nil {
				return
			}
		}

		buf, err := writer.DetachBuffer()
		if err != nil {
			return
		}

		gattReadRequest.RespondWithValue(buf)
		buf.Release()
	})

	for _, char := range s.Characteristics {
		params, err := genericattributeprofile.NewGattLocalCharacteristicParameters()
		if err != nil {
			return err
		}

		if err = params.SetCharacteristicProperties(genericattributeprofile.GattCharacteristicProperties(char.Flags)); err != nil {
			return err
		}

		uuid := syscallUUIDFromUUID(char.UUID)
		createCharOp, err := localService.CreateCharacteristicAsync(uuid, params)
		if err != nil {
			return err
		}

		if err = awaitAsyncOperation(createCharOp, genericattributeprofile.SignatureGattLocalCharacteristicResult); err != nil {
			return err
		}

		res, err := createCharOp.GetResults()
		if err != nil {
			return err
		}

		characteristicResults := (*genericattributeprofile.GattLocalCharacteristicResult)(res)
		characteristic, err := characteristicResults.GetCharacteristic()
		if err != nil {
			return err
		}

		_, err = characteristic.AddWriteRequested(writeRequestedHandler)
		if err != nil {
			return err
		}

		_, err = characteristic.AddReadRequested(readRequestedHandler)
		if err != nil {
			return err
		}

		// Keep the object around for Characteristic.Write.
		if char.Handle != nil {
			char.Handle.wintCharacteristic = characteristic
			char.Handle.value = char.Value
			char.Handle.valueMtx = &sync.Mutex{}
			char.Handle.flags = char.Flags
			char.Handle.writeEvent = char.WriteEvent
			goChars[uuid] = char.Handle
		}
	}

	if err := startServiceAdvertisement(serviceProvider); err != nil {
		return err
	}
	advertisingStarted = true
	if statusErr := waitForAdvertisementStatus(serviceProvider, true); statusErr != nil {
		return statusErr
	}
	a.serviceProvidersMu.Lock()
	if a.serviceProviders == nil {
		a.serviceProviders = make(map[syscall.GUID]*genericattributeprofile.GattServiceProvider)
	}
	serviceUUID := syscallUUIDFromUUID(s.UUID)
	if previous := a.serviceProviders[serviceUUID]; previous != nil {
		_ = previous.StopAdvertising()
		_ = waitForAdvertisementStatus(previous, false)
		previous.Release()
	}
	a.serviceProviders[serviceUUID] = serviceProvider
	providerStored = true
	a.serviceProvidersMu.Unlock()
	return nil
}

const advertisementStatusTimeout = 5 * time.Second

func waitForAdvertisementStatus(provider *genericattributeprofile.GattServiceProvider, started bool) error {
	deadline := time.Now().Add(advertisementStatusTimeout)
	var lastStatus genericattributeprofile.GattServiceProviderAdvertisementStatus
	for {
		status, err := provider.GetAdvertisementStatus()
		if err != nil {
			return fmt.Errorf("read GATT advertisement status: %w", err)
		}
		lastStatus = status
		if started {
			if status == genericattributeprofile.GattServiceProviderAdvertisementStatusStarted ||
				status == genericattributeprofile.GattServiceProviderAdvertisementStatusStartedWithoutAllAdvertisementData {
				return nil
			}
			// WinRT can retain Aborted from a previous advertising operation while
			// StartAdvertising is still being processed. On real hardware the
			// observed sequence can be Aborted -> Started within a few polling
			// intervals, so Aborted is only terminal once the startup deadline
			// expires.
		} else if status == genericattributeprofile.GattServiceProviderAdvertisementStatusStopped {
			return nil
		} else if status == genericattributeprofile.GattServiceProviderAdvertisementStatusAborted {
			// Aborted is terminal for the current advertising operation. Waiting
			// for Stopped here can add another five seconds even though the
			// provider is already unusable for that operation.
			return nil
		}
		if time.Now().After(deadline) {
			if started && lastStatus == genericattributeprofile.GattServiceProviderAdvertisementStatusAborted {
				return fmt.Errorf("%w for %s (status=%s/%d; Windows rejected the request, commonly due to radio resource contention or a driver limitation)", ErrAdvertisementAborted, advertisementStatusTimeout, advertisementStatusName(lastStatus), lastStatus)
			}
			return fmt.Errorf("timed out waiting for GATT advertisement status %s (%d)", advertisementStatusName(lastStatus), lastStatus)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func advertisementStatusName(status genericattributeprofile.GattServiceProviderAdvertisementStatus) string {
	switch status {
	case genericattributeprofile.GattServiceProviderAdvertisementStatusCreated:
		return "created"
	case genericattributeprofile.GattServiceProviderAdvertisementStatusStopped:
		return "stopped"
	case genericattributeprofile.GattServiceProviderAdvertisementStatusStarted:
		return "started"
	case genericattributeprofile.GattServiceProviderAdvertisementStatusAborted:
		return "aborted"
	case genericattributeprofile.GattServiceProviderAdvertisementStatusStartedWithoutAllAdvertisementData:
		return "started-without-all-advertisement-data"
	default:
		return fmt.Sprintf("unknown(%d)", status)
	}
}

func startServiceAdvertisement(provider *genericattributeprofile.GattServiceProvider) error {
	params, err := genericattributeprofile.NewGattServiceProviderAdvertisingParameters()
	if err != nil {
		return fmt.Errorf("create GATT advertisement parameters: %w", err)
	}
	defer params.Release()
	if err = params.SetIsConnectable(true); err != nil {
		return fmt.Errorf("configure GATT advertisement connectability: %w", err)
	}
	if err = params.SetIsDiscoverable(true); err != nil {
		return fmt.Errorf("configure GATT advertisement discoverability: %w", err)
	}
	if err = provider.StartAdvertisingWithParameters(params); err != nil {
		return fmt.Errorf("start GATT service advertisement: %w", err)
	}
	return nil
}

// RestartServiceAdvertisement rebinds the connectable GATT advertisement.
// WinRT can stop advertising after a central disconnects, while the service
// provider itself remains registered.
func (a *Adapter) RestartServiceAdvertisement(s *Service) error {
	serviceUUID := syscallUUIDFromUUID(s.UUID)
	a.serviceProvidersMu.Lock()
	serviceProvider := a.serviceProviders[serviceUUID]
	a.serviceProvidersMu.Unlock()
	if serviceProvider == nil {
		return fmt.Errorf("GATT service provider for %s is not registered", s.UUID)
	}

	status, err := serviceProvider.GetAdvertisementStatus()
	if err != nil {
		return fmt.Errorf("read GATT advertisement status before restart: %w", err)
	}
	if status != genericattributeprofile.GattServiceProviderAdvertisementStatusStopped &&
		status != genericattributeprofile.GattServiceProviderAdvertisementStatusCreated {
		if err := serviceProvider.StopAdvertising(); err != nil {
			return fmt.Errorf("stop GATT service advertisement from %s: %w", advertisementStatusName(status), err)
		}
		if err := waitForAdvertisementStatus(serviceProvider, false); err != nil {
			return err
		}
	}
	if err := startServiceAdvertisement(serviceProvider); err != nil {
		return fmt.Errorf("restart GATT service advertisement: %w", err)
	}
	if err := waitForAdvertisementStatus(serviceProvider, true); err != nil {
		return err
	}
	return nil
}

// RemoveService stops advertising the service and removes it.
func (a *Adapter) RemoveService(s *Service) error {
	serviceUUID := syscallUUIDFromUUID(s.UUID)
	a.serviceProvidersMu.Lock()
	serviceProvider := a.serviceProviders[serviceUUID]
	delete(a.serviceProviders, serviceUUID)
	a.serviceProvidersMu.Unlock()
	if serviceProvider == nil {
		return nil
	}
	err := serviceProvider.StopAdvertising()
	if err == nil {
		err = waitForAdvertisementStatus(serviceProvider, false)
	}
	serviceProvider.Release()
	return err
}

// Write replaces the characteristic value with a new value.
func (c *Characteristic) Write(p []byte) (n int, err error) {
	length := len(p)

	if length == 0 {
		return 0, nil // nothing to do
	}

	if c.writeEvent != nil {
		c.writeEvent(0, 0, p)
	}

	// writes are only actually processed on read events from clients, we just set a variable here.
	c.valueMtx.Lock()
	defer c.valueMtx.Unlock()
	c.value = p

	// only notify if it's enabled, otherwise the below leads to an error
	if c.flags&CharacteristicNotifyPermission != 0 {
		writer, err := streams.NewDataWriter()
		if err != nil {
			return length, err
		}

		defer writer.Release()
		err = writer.WriteBytes(uint32(len(p)), p)
		if err != nil {
			return length, err
		}

		buf, err := writer.DetachBuffer()
		if err != nil {
			return length, err
		}
		defer buf.Release()

		op, err := c.wintCharacteristic.NotifyValueAsync(buf)
		if err != nil {
			return length, err
		}

		// IVectorView<GattClientNotificationResult>
		signature := fmt.Sprintf("pinterface({%s};%s)", collections.GUIDIVectorView, genericattributeprofile.SignatureGattClientNotificationResult)
		if err = awaitAsyncOperation(op, signature); err != nil {
			return length, err
		}
		defer op.Release()

		res, err := op.GetResults()
		if err != nil {
			return length, err
		}

		// TODO: process notification results, just getting this to release
		vec := (*collections.IVectorView)(res)
		vec.Release()
	}

	return length, nil
}

func syscallUUIDFromUUID(uuid UUID) syscall.GUID {
	guid := ole.NewGUID(uuid.String())
	return syscall.GUID{
		Data1: guid.Data1,
		Data2: guid.Data2,
		Data3: guid.Data3,
		Data4: guid.Data4,
	}
}
