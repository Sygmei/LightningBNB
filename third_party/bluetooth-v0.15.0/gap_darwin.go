package bluetooth

import (
	"errors"
	"fmt"
	"time"

	"github.com/tinygo-org/cbgo"
)

// default connection timeout
const defaultConnectionTimeout time.Duration = 10 * time.Second

// CoreBluetooth cancellation is asynchronous. Bound the time spent waiting
// for its disconnect callback so a missing callback cannot leave Connect
// running forever and interfere with the next attempt for the same peripheral.
const connectCancellationTimeout = 2 * time.Second

// Address contains a Bluetooth address which on macOS is a UUID.
type Address struct {
	// UUID since this is macOS.
	UUID
}

// IsRandom ignored on macOS.
func (ad Address) IsRandom() bool {
	return false
}

// SetRandom ignored on macOS.
func (ad *Address) SetRandom(val bool) {
}

// Set the address
func (ad *Address) Set(val string) {
	uuid, err := ParseUUID(val)
	if err != nil {
		return
	}
	ad.UUID = uuid
}

// Scan starts a BLE scan. It is stopped by a call to StopScan. A common pattern
// is to cancel the scan when a particular device has been found.
func (a *Adapter) Scan(callback func(*Adapter, ScanResult)) (err error) {
	if callback == nil {
		return errors.New("must provide callback to Scan function")
	}

	if a.scanChan != nil {
		return errors.New("already calling Scan function")
	}

	a.peripheralFoundHandler = callback

	// Channel that will be closed when the scan is stopped.
	// Detecting whether the scan is stopped can be done by doing a non-blocking
	// read from it. If it succeeds, the scan is stopped.
	a.scanChan = make(chan error)

	a.cm.Scan(nil, &cbgo.CentralManagerScanOpts{
		// Linux BlueZ may place the service UUID in a scan response while the
		// local name arrives in the initial advertisement. Keep receiving
		// callbacks so the application can merge both payloads before deciding
		// whether a peripheral is LightningBNB.
		AllowDuplicates: true,
	})

	// Check whether the scan is stopped. This is necessary to avoid a race
	// condition between the signal channel and the cancelScan channel when
	// the callback calls StopScan() (no new callbacks may be called after
	// StopScan is called).
	select {
	case <-a.scanChan:
		close(a.scanChan)
		a.scanChan = nil
		return nil
	}
}

// StopScan stops any in-progress scan. It can be called from within a Scan
// callback to stop the current scan. If no scan is in progress, an error will
// be returned.
func (a *Adapter) StopScan() error {
	if a.scanChan == nil {
		return errors.New("not calling Scan function")
	}

	a.scanChan <- nil
	a.cm.StopScan()

	return nil
}

var _ GAPDevice = Device{}

// Device is a connection to a remote peripheral.
type Device struct {
	Address Address

	*deviceInternal
}

type deviceInternal struct {
	delegate *peripheralDelegate

	cm   cbgo.CentralManager
	prph cbgo.Peripheral

	servicesChan              chan error
	charsChan                 chan error
	writeWithoutResponseReady chan struct{}

	services map[UUID]DeviceService
}

// Connect starts a connection attempt to the given peripheral device address.
func (a *Adapter) Connect(address Address, params ConnectionParams) (Device, error) {
	uuid, err := cbgo.ParseUUID(address.UUID.String())
	if err != nil {
		return Device{}, err
	}
	prphs := a.cm.RetrievePeripheralsWithIdentifiers([]cbgo.UUID{uuid})
	if len(prphs) == 0 {
		return Device{}, fmt.Errorf("Connect failed: no peer with address: %s", address.UUID.String())
	}

	timeout := defaultConnectionTimeout
	if params.ConnectionTimeout != 0 {
		timeout = time.Duration(int64(params.ConnectionTimeout)*625) * time.Microsecond
	}

	id := prphs[0].Identifier().String()
	// Delegate callbacks must never block if cancellation and callback delivery
	// race. The single connection result is all this call needs.
	prphCh := make(chan cbgo.Peripheral, 1)

	a.connectMap.Store(id, prphCh)
	defer clearConnectAttempt(a, id, prphCh)

	a.cm.Connect(prphs[0], nil)
	timeoutTimer := time.NewTimer(timeout)
	defer timeoutTimer.Stop()

	for {
		// wait on channel for connect
		select {
		case p := <-prphCh:

			// check if we have received a disconnected peripheral
			if p.State() == cbgo.PeripheralStateDisconnected {
				return Device{}, errors.New("connection canceled before completion")
			}

			d := Device{
				Address: address,
				deviceInternal: &deviceInternal{
					cm:                        a.cm,
					prph:                      p,
					servicesChan:              make(chan error),
					charsChan:                 make(chan error),
					writeWithoutResponseReady: make(chan struct{}, 1),
				},
			}

			d.delegate = &peripheralDelegate{d: d}
			p.SetDelegate(d.delegate)

			a.connectHandler(d, true)

			return d, nil

		case <-timeoutTimer.C:
			connectionError := errors.New("timeout on Connect")
			a.cm.CancelConnect(prphs[0])

			// Give CoreBluetooth a bounded opportunity to finish cancellation.
			// The previous implementation waited forever here, allowing an
			// abandoned identity probe to race with the next real connection.
			cancelTimer := time.NewTimer(connectCancellationTimeout)
			select {
			case p := <-prphCh:
				if !cancelTimer.Stop() {
					<-cancelTimer.C
				}
				if p.State() != cbgo.PeripheralStateDisconnected {
					a.cm.CancelConnect(p)
				}
			case <-cancelTimer.C:
			}
			return Device{}, connectionError
		}
	}
}

func clearConnectAttempt(a *Adapter, id string, attempt chan cbgo.Peripheral) {
	a.connectMap.CompareAndDelete(id, attempt)
}

// Disconnect from the BLE device. This method is non-blocking and does not
// wait until the connection is fully gone.
func (d Device) Disconnect() error {
	d.cm.CancelConnect(d.prph)
	return nil
}

// Connected returns whether the device is currently connected.
func (d Device) Connected() (bool, error) {
	return d.prph.State() == cbgo.PeripheralStateConnected, nil
}

// RequestConnectionParams requests a different connection latency and timeout
// of the given device connection. Fields that are unset will be left alone.
// Whether or not the device will actually honor this, depends on the device and
// on the specific parameters.
//
// This call has not yet been implemented on macOS.
func (d Device) RequestConnectionParams(params ConnectionParams) error {
	// TODO: implement this using setDesiredConnectionLatency, see:
	// https://developer.apple.com/documentation/corebluetooth/cbperipheralmanager/1393277-setdesiredconnectionlatency
	return nil
}

// Peripheral delegate functions

type peripheralDelegate struct {
	cbgo.PeripheralDelegateBase

	d Device
}

// DidDiscoverServices is called when the services for a Peripheral
// have been discovered.
func (pd *peripheralDelegate) DidDiscoverServices(prph cbgo.Peripheral, err error) {
	pd.d.servicesChan <- err
}

// DidDiscoverCharacteristics is called when the characteristics for a Service
// for a Peripheral have been discovered.
func (pd *peripheralDelegate) DidDiscoverCharacteristics(prph cbgo.Peripheral, svc cbgo.Service, err error) {
	pd.d.charsChan <- err
}

// DidUpdateValueForCharacteristic is called when the characteristic for a Service
// for a Peripheral receives a notification with a new value,
// or receives a value for a read request.
func (pd *peripheralDelegate) DidUpdateValueForCharacteristic(prph cbgo.Peripheral, chr cbgo.Characteristic, err error) {
	uuid, _ := ParseUUID(chr.UUID().String())
	svcuuid, _ := ParseUUID(chr.Service().UUID().String())

	if svc, ok := pd.d.services[svcuuid]; ok {
		for _, char := range svc.characteristics {

			if char.characteristic == chr && uuid == char.UUID() { // compare pointers
				if err == nil && char.callback != nil {
					char.callback(chr.Value())
				}

				if char.readChan != nil {
					char.readChan <- err
				}
			}

		}

	}
}

// DidWriteValueForCharacteristic is called after the characteristic for a Service
// for a Peripheral trigger a write with response. It contains the returned error or nil.
func (pd *peripheralDelegate) DidWriteValueForCharacteristic(_ cbgo.Peripheral, chr cbgo.Characteristic, err error) {
	uuid, _ := ParseUUID(chr.UUID().String())
	svcuuid, _ := ParseUUID(chr.Service().UUID().String())

	if svc, ok := pd.d.services[svcuuid]; ok {
		for _, char := range svc.characteristics {
			if char.characteristic == chr && uuid == char.UUID() { // compare pointers
				if char.writeChan != nil {
					char.writeChan <- err
				}
			}
		}
	}
}

// IsReadyToSendWriteWithoutResponse is called by CoreBluetooth after its
// write-without-response queue transitions from full to writable. A buffered,
// coalescing signal prevents a callback between the readiness check and wait
// from being lost.
func (pd *peripheralDelegate) IsReadyToSendWriteWithoutResponse(_ cbgo.Peripheral) {
	select {
	case pd.d.writeWithoutResponseReady <- struct{}{}:
	default:
	}
}

// DidUpdateNotificationState is called when the notification state for a characteristic
// has been updated. It reports whether enabling/disabling notifications succeeded.
func (pd *peripheralDelegate) DidUpdateNotificationState(prph cbgo.Peripheral, chr cbgo.Characteristic, err error) {
	uuid, _ := ParseUUID(chr.UUID().String())
	svcuuid, _ := ParseUUID(chr.Service().UUID().String())

	if svc, ok := pd.d.services[svcuuid]; ok {
		for _, char := range svc.characteristics {
			if char.characteristic == chr && uuid == char.UUID() {
				if char.notifyChan != nil {
					char.notifyChan <- err
				}
			}
		}
	}
}
