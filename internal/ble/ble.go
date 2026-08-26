package ble

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Sygmei/LightningBNB/internal/link"
	"tinygo.org/x/bluetooth"
)

const (
	ServiceUUIDString  = "13f0b6a0-4746-4c42-8e2f-1f62e4a0b1a0"
	RXUUIDString       = "13f0b6a1-4746-4c42-8e2f-1f62e4a0b1a0"
	TXUUIDString       = "13f0b6a2-4746-4c42-8e2f-1f62e4a0b1a0"
	IdentityUUIDString = "13f0b6a3-4746-4c42-8e2f-1f62e4a0b1a0"
	TestCompanyID      = 0xffff

	maxPacketMTU          = 244
	probeSettlingDelay    = 250 * time.Millisecond
	identityProbeTimeout  = 15 * time.Second
	connectAttemptTimeout = 15 * time.Second
)

// ConnectAttemptTimeout is long enough for the platform backend to time out
// and finish its bounded cancellation before an application-level retry.
const ConnectAttemptTimeout = connectAttemptTimeout

var (
	ServiceUUID  = mustUUID(ServiceUUIDString)
	RXUUID       = mustUUID(RXUUIDString)
	TXUUID       = mustUUID(TXUUIDString)
	IdentityUUID = mustUUID(IdentityUUIDString)
	marker       = []byte("LBNB1")
)

var errInvalidConnectedDevice = errors.New("bluetooth backend returned an invalid device")

type Device struct {
	ID               string
	ServerID         string
	Name             string
	RSSI             int16
	LightningBNB     bool
	ServiceUUIDs     []string
	ServiceData      []string
	ManufacturerData []string

	address          bluetooth.Address
	connection       *pendingConnection
	identityProbedAt time.Time
}

type pendingConnection struct {
	mu     sync.Mutex
	device *bluetooth.Device
}

func newPendingConnection(device *bluetooth.Device) *pendingConnection {
	return &pendingConnection{device: device}
}

func (c *pendingConnection) take() (bluetooth.Device, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.device == nil {
		return bluetooth.Device{}, false
	}
	device := *c.device
	c.device = nil
	return device, true
}

type PeripheralListener interface {
	Accept(context.Context) (link.PacketConn, error)
	Close() error
}

type Adapter struct {
	native     *bluetooth.Adapter
	enableOnce sync.Once
	enableErr  error
	scanMu     sync.Mutex

	connectionsMu sync.Mutex
	connections   map[string]*clientPacketConn
}

func NewAdapter() *Adapter {
	a := &Adapter{
		native:      bluetooth.DefaultAdapter,
		connections: make(map[string]*clientPacketConn),
	}
	a.native.SetConnectHandler(a.connectionChanged)
	return a
}

func (a *Adapter) Enable() error {
	a.enableOnce.Do(func() { a.enableErr = a.native.Enable() })
	return a.enableErr
}

func (a *Adapter) Scan(ctx context.Context, duration time.Duration) ([]Device, error) {
	devices, err := a.scan(ctx, duration, true, nil)
	if err != nil {
		return nil, err
	}
	for index := range devices {
		// This deadline must outlive the Darwin backend's 10-second connection
		// timeout and bounded cancellation cleanup. Returning sooner abandons a
		// live CoreBluetooth Connect call that can consume the next attempt's
		// callback for the same peripheral.
		identifyCtx, cancel := context.WithTimeout(ctx, identityProbeTimeout)
		serverID, identifyErr := a.identify(identifyCtx, devices[index])
		cancel()
		devices[index].identityProbedAt = time.Now()
		if identifyErr == nil {
			devices[index].ServerID = serverID.String()
		}
	}
	return devices, nil
}

// Discover returns LightningBNB advertisements without opening a temporary
// GATT connection to read each stable server identity. Interactive selection
// uses this path so the selected server is connected exactly once.
func (a *Adapter) Discover(ctx context.Context, duration time.Duration) ([]Device, error) {
	return a.scan(ctx, duration, true, nil)
}

// DiscoverWithCallback reports each newly recognized LightningBNB device as
// soon as its advertisement has been observed. It intentionally does not
// probe the GATT identity, so interactive callers can render results while
// the scan is still running and connect exactly once after selection.
func (a *Adapter) DiscoverWithCallback(ctx context.Context, duration time.Duration, onDevice func(Device)) ([]Device, error) {
	return a.scan(ctx, duration, true, onDevice)
}

func (a *Adapter) ScanAll(ctx context.Context, duration time.Duration) ([]Device, error) {
	return a.scan(ctx, duration, false, nil)
}

// ScanAllWithCallback reports each newly observed BLE advertisement while the
// scan is in progress. The returned slice remains the complete merged result.
func (a *Adapter) ScanAllWithCallback(ctx context.Context, duration time.Duration, onDevice func(Device)) ([]Device, error) {
	return a.scan(ctx, duration, false, onDevice)
}

func (a *Adapter) scan(ctx context.Context, duration time.Duration, onlyLightningBNB bool, onDevice func(Device)) ([]Device, error) {
	if err := a.Enable(); err != nil {
		return nil, fmt.Errorf("enable Bluetooth: %w", err)
	}
	if duration <= 0 {
		duration = 30 * time.Second
	}
	a.scanMu.Lock()
	defer a.scanMu.Unlock()

	scanCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	var mu sync.Mutex
	devices := make(map[string]Device)
	scanErr := make(chan error, 1)
	go func() {
		scanErr <- a.native.Scan(func(_ *bluetooth.Adapter, result bluetooth.ScanResult) {
			matches := isLightningBNB(result.AdvertisementPayload)
			name := result.LocalName()
			if markerName := advertisedMarkerName(result.AdvertisementPayload); markerName != "" {
				name = markerName
			}
			serviceUUIDs, serviceData, manufacturerData := advertisementDetails(result.AdvertisementPayload)
			device := Device{
				ID:               result.Address.String(),
				Name:             name,
				RSSI:             result.RSSI,
				LightningBNB:     matches,
				ServiceUUIDs:     serviceUUIDs,
				ServiceData:      serviceData,
				ManufacturerData: manufacturerData,
				address:          result.Address,
			}
			mu.Lock()
			previous, existed := devices[device.ID]
			if existed {
				device = mergeDevice(previous, device)
			}
			devices[device.ID] = device
			report := onDevice != nil && device.LightningBNB && (!existed || !previous.LightningBNB)
			if onDevice != nil && !onlyLightningBNB {
				report = !existed || (!previous.LightningBNB && device.LightningBNB)
			}
			mu.Unlock()
			if report {
				onDevice(device)
			}
		})
	}()

	select {
	case err := <-scanErr:
		if err != nil {
			return nil, err
		}
	case <-scanCtx.Done():
		stopDeadline := time.Now().Add(time.Second)
		for {
			if err := a.native.StopScan(); err == nil || time.Now().After(stopDeadline) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		select {
		case err := <-scanErr:
			if err != nil && !errors.Is(err, context.Canceled) {
				return nil, err
			}
		case <-time.After(time.Second):
			return nil, errors.New("Bluetooth scan did not stop")
		}
	}
	mu.Lock()
	result := make([]Device, 0, len(devices))
	for _, device := range devices {
		if onlyLightningBNB && !device.LightningBNB {
			continue
		}
		result = append(result, device)
	}
	mu.Unlock()
	return result, nil
}

func advertisementDetails(payload bluetooth.AdvertisementPayload) ([]string, []string, []string) {
	if payload == nil {
		return nil, nil, nil
	}
	services := make([]string, 0, len(payload.ServiceUUIDs()))
	for _, uuid := range payload.ServiceUUIDs() {
		services = appendUnique(services, uuid.String())
	}
	serviceData := make([]string, 0, len(payload.ServiceData()))
	for _, item := range payload.ServiceData() {
		serviceData = appendUnique(serviceData, fmt.Sprintf("%s=%x", item.UUID, item.Data))
	}
	manufacturerData := make([]string, 0, len(payload.ManufacturerData()))
	for _, item := range payload.ManufacturerData() {
		manufacturerData = appendUnique(manufacturerData, fmt.Sprintf("%04x=%x", item.CompanyID, item.Data))
	}
	return services, serviceData, manufacturerData
}

func mergeDevice(previous, next Device) Device {
	if next.Name == "" {
		next.Name = previous.Name
	}
	next.LightningBNB = previous.LightningBNB || next.LightningBNB
	next.ServiceUUIDs = appendUnique(next.ServiceUUIDs, previous.ServiceUUIDs...)
	next.ServiceData = appendUnique(next.ServiceData, previous.ServiceData...)
	next.ManufacturerData = appendUnique(next.ManufacturerData, previous.ManufacturerData...)
	return next
}

func appendUnique(values []string, additions ...string) []string {
	for _, addition := range additions {
		found := false
		for _, value := range values {
			if value == addition {
				found = true
				break
			}
		}
		if !found {
			values = append(values, addition)
		}
	}
	return values
}

func (a *Adapter) Find(ctx context.Context, id string, duration time.Duration) (Device, error) {
	devices, err := a.scan(ctx, duration, true, nil)
	if err != nil {
		return Device{}, err
	}
	for index := range devices {
		if devices[index].ID == id {
			native, serverID, identifyErr := a.identifyConnected(ctx, devices[index])
			if native == nil {
				// Preserve the old platform-ID behavior if the identity probe
				// itself cannot connect. Connect will report the useful error.
				return devices[index], nil
			}
			if identifyErr == nil {
				devices[index].ServerID = serverID.String()
			} else if ctx.Err() != nil {
				_ = native.Disconnect()
				return Device{}, ctx.Err()
			}
			// Even an older server without the identity characteristic can
			// reuse this connection for its transport characteristics.
			devices[index].connection = newPendingConnection(native)
			return devices[index], nil
		}
	}

	normalizedID := strings.ToLower(id)
	requestedID, err := ParseServerID(normalizedID)
	if err != nil {
		return Device{}, fmt.Errorf("LightningBNB server %q not found", id)
	}
	for index := range devices {
		native, serverID, identifyErr := a.identifyConnected(ctx, devices[index])
		if identifyErr != nil {
			if native != nil {
				_ = native.Disconnect()
			}
			if ctx.Err() != nil {
				return Device{}, ctx.Err()
			}
			continue
		}
		devices[index].ServerID = serverID.String()
		if serverID == requestedID {
			// Reuse the connection used to read the stable identity. In
			// particular, CoreBluetooth disconnects asynchronously and may
			// otherwise deliver the old disconnect event to an immediate new
			// connection attempt.
			devices[index].connection = newPendingConnection(native)
			return devices[index], nil
		}
		_ = native.Disconnect()
	}
	return Device{}, fmt.Errorf("LightningBNB server %q not found", id)
}

func (a *Adapter) identify(ctx context.Context, device Device) (ServerID, error) {
	native, serverID, err := a.identifyConnected(ctx, device)
	if native != nil {
		_ = native.Disconnect()
	}
	if err != nil {
		return ServerID{}, err
	}
	return serverID, nil
}

func (a *Adapter) identifyConnected(ctx context.Context, device Device) (*bluetooth.Device, ServerID, error) {
	native, err := a.connectNative(ctx, device)
	if err != nil {
		return nil, ServerID{}, err
	}

	type result struct {
		id  ServerID
		err error
	}
	done := make(chan result, 1)
	go func() {
		id, err := readServerID(native)
		done <- result{id: id, err: err}
	}()
	select {
	case <-ctx.Done():
		_ = native.Disconnect()
		return nil, ServerID{}, ctx.Err()
	case result := <-done:
		if result.err != nil {
			return &native, ServerID{}, result.err
		}
		return &native, result.id, nil
	}
}

func readServerID(native bluetooth.Device) (ServerID, error) {
	services, err := native.DiscoverServices([]bluetooth.UUID{ServiceUUID})
	if err != nil {
		return ServerID{}, err
	}
	if len(services) != 1 {
		return ServerID{}, errors.New("LightningBNB service was not found")
	}
	characteristics, err := services[0].DiscoverCharacteristics([]bluetooth.UUID{IdentityUUID})
	if err != nil {
		return ServerID{}, err
	}
	if len(characteristics) != 1 || characteristics[0].UUID() != IdentityUUID {
		return ServerID{}, errors.New("LightningBNB server identity characteristic was not found")
	}
	var id ServerID
	n, err := characteristics[0].Read(id[:])
	if err != nil {
		return ServerID{}, err
	}
	if n != len(id) {
		return ServerID{}, fmt.Errorf("invalid LightningBNB server identity length %d", n)
	}
	return id, nil
}

func (a *Adapter) connectNative(ctx context.Context, device Device) (bluetooth.Device, error) {
	type result struct {
		device bluetooth.Device
		err    error
	}
	connected := make(chan result, 1)
	go func() {
		native, err := a.native.Connect(device.address, bluetooth.ConnectionParams{
			ConnectionTimeout: bluetooth.NewDuration(10 * time.Second),
		})
		connected <- result{device: native, err: err}
	}()
	select {
	case <-ctx.Done():
		go func() {
			result := <-connected
			if result.err == nil {
				_ = result.device.Disconnect()
			}
		}()
		return bluetooth.Device{}, ctx.Err()
	case result := <-connected:
		if result.err != nil {
			return bluetooth.Device{}, fmt.Errorf("connect to %s: %w", device.ID, result.err)
		}
		if err := validateConnectedDevice(result.device, device.address); err != nil {
			return bluetooth.Device{}, fmt.Errorf("connect to %s: %w", device.ID, err)
		}
		return result.device, nil
	}
}

func validateConnectedDevice(device bluetooth.Device, expectedAddress bluetooth.Address) error {
	if device.Address != expectedAddress {
		return errInvalidConnectedDevice
	}
	return nil
}

func (a *Adapter) Connect(ctx context.Context, device Device) (link.PacketConn, error) {
	if err := a.Enable(); err != nil {
		return nil, fmt.Errorf("enable Bluetooth: %w", err)
	}
	native, havePendingConnection := bluetooth.Device{}, false
	if device.connection != nil {
		native, havePendingConnection = device.connection.take()
	}
	if !havePendingConnection {
		if wait := probeSettlingDelay - time.Since(device.identityProbedAt); !device.identityProbedAt.IsZero() && wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
		var err error
		native, err = a.connectNative(ctx, device)
		if err != nil {
			return nil, err
		}
	}

	services, err := native.DiscoverServices([]bluetooth.UUID{ServiceUUID})
	if err != nil {
		_ = native.Disconnect()
		return nil, fmt.Errorf("discover LightningBNB service: %w", err)
	}
	if len(services) != 1 {
		_ = native.Disconnect()
		return nil, errors.New("LightningBNB service was not found on the selected device")
	}
	// Discover all characteristics in one request. Besides allowing older
	// servers without the identity characteristic, this keeps the Darwin
	// backend's characteristic callback cache intact for TX notifications.
	characteristics, err := services[0].DiscoverCharacteristics(nil)
	if err != nil {
		_ = native.Disconnect()
		return nil, fmt.Errorf("discover LightningBNB characteristics: %w", err)
	}
	var rx bluetooth.DeviceCharacteristic
	var tx bluetooth.DeviceCharacteristic
	var identity bluetooth.DeviceCharacteristic
	haveRX := false
	haveTX := false
	haveIdentity := false
	for _, characteristic := range characteristics {
		switch characteristic.UUID() {
		case RXUUID:
			rx = characteristic
			haveRX = true
		case TXUUID:
			tx = characteristic
			haveTX = true
		case IdentityUUID:
			identity = characteristic
			haveIdentity = true
		}
	}
	if !haveRX || !haveTX {
		_ = native.Disconnect()
		return nil, errors.New("selected device does not expose the complete LightningBNB transport")
	}
	serverID := ""
	if haveIdentity {
		var id ServerID
		if n, readErr := identity.Read(id[:]); readErr == nil && n == len(id) {
			serverID = id.String()
		}
	}
	conn := newClientPacketConn(device.ID, serverID, native, rx, tx)
	a.connectionsMu.Lock()
	a.connections[device.ID] = conn
	a.connectionsMu.Unlock()
	conn.onClose = func() {
		a.connectionsMu.Lock()
		if a.connections[device.ID] == conn {
			delete(a.connections, device.ID)
		}
		a.connectionsMu.Unlock()
	}
	if err := conn.start(); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("subscribe to LightningBNB transport: %w", err)
	}
	return conn, nil
}

// ConnectedServerID returns the stable identity read on the transport's GATT
// connection, or an empty string for older servers and non-client transports.
func ConnectedServerID(conn link.PacketConn) string {
	if client, ok := conn.(*clientPacketConn); ok {
		return client.serverID
	}
	return ""
}

func (a *Adapter) connectionChanged(device bluetooth.Device, connected bool) {
	if connected {
		return
	}
	id := device.Address.String()
	a.connectionsMu.Lock()
	conn := a.connections[id]
	a.connectionsMu.Unlock()
	if conn != nil {
		conn.close(false)
	}
}

func isLightningBNB(payload bluetooth.AdvertisementPayload) bool {
	if payload == nil {
		return false
	}
	if payload.HasServiceUUID(ServiceUUID) {
		return true
	}
	for _, item := range payload.ManufacturerData() {
		if item.CompanyID == TestCompanyID && bytes.HasPrefix(item.Data, marker) {
			return true
		}
	}
	return false
}

func advertisedMarkerName(payload bluetooth.AdvertisementPayload) string {
	if payload == nil {
		return ""
	}
	for _, item := range payload.ManufacturerData() {
		if item.CompanyID == TestCompanyID && bytes.HasPrefix(item.Data, marker) {
			return string(bytes.TrimSpace(item.Data[len(marker):]))
		}
	}
	return ""
}

func mustUUID(value string) bluetooth.UUID {
	uuid, err := bluetooth.ParseUUID(value)
	if err != nil {
		panic(err)
	}
	return uuid
}

func packetMTU(characteristic bluetooth.DeviceCharacteristic) int {
	mtu, err := characteristic.GetMTU()
	if err != nil || mtu <= 3 {
		return 20
	}
	return min(int(mtu)-3, maxPacketMTU)
}
