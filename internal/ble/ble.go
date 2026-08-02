package ble

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Sygmei/LightningBNB/internal/link"
	"tinygo.org/x/bluetooth"
)

const (
	ServiceUUIDString = "13f0b6a0-4746-4c42-8e2f-1f62e4a0b1a0"
	RXUUIDString      = "13f0b6a1-4746-4c42-8e2f-1f62e4a0b1a0"
	TXUUIDString      = "13f0b6a2-4746-4c42-8e2f-1f62e4a0b1a0"
	TestCompanyID     = 0xffff

	maxPacketMTU = 244
)

var (
	ServiceUUID = mustUUID(ServiceUUIDString)
	RXUUID      = mustUUID(RXUUIDString)
	TXUUID      = mustUUID(TXUUIDString)
	marker      = []byte("LBNB1")
)

type Device struct {
	ID               string
	Name             string
	RSSI             int16
	LightningBNB     bool
	ServiceUUIDs     []string
	ServiceData      []string
	ManufacturerData []string

	address bluetooth.Address
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
	return a.scan(ctx, duration, true)
}

func (a *Adapter) ScanAll(ctx context.Context, duration time.Duration) ([]Device, error) {
	return a.scan(ctx, duration, false)
}

func (a *Adapter) scan(ctx context.Context, duration time.Duration, onlyLightningBNB bool) ([]Device, error) {
	if err := a.Enable(); err != nil {
		return nil, fmt.Errorf("enable Bluetooth: %w", err)
	}
	if duration <= 0 {
		duration = 5 * time.Second
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
			if onlyLightningBNB && !matches {
				return
			}
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
			if previous, ok := devices[device.ID]; ok {
				device = mergeDevice(previous, device)
			}
			devices[device.ID] = device
			mu.Unlock()
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
	devices, err := a.Scan(ctx, duration)
	if err != nil {
		return Device{}, err
	}
	for _, device := range devices {
		if device.ID == id {
			return device, nil
		}
	}
	return Device{}, fmt.Errorf("LightningBNB server %q not found", id)
}

func (a *Adapter) Connect(ctx context.Context, device Device) (link.PacketConn, error) {
	if err := a.Enable(); err != nil {
		return nil, fmt.Errorf("enable Bluetooth: %w", err)
	}
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
	var native bluetooth.Device
	select {
	case <-ctx.Done():
		go func() {
			result := <-connected
			if result.err == nil {
				_ = result.device.Disconnect()
			}
		}()
		return nil, ctx.Err()
	case result := <-connected:
		if result.err != nil {
			return nil, fmt.Errorf("connect to %s: %w", device.ID, result.err)
		}
		native = result.device
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
	characteristics, err := services[0].DiscoverCharacteristics([]bluetooth.UUID{RXUUID, TXUUID})
	if err != nil {
		_ = native.Disconnect()
		return nil, fmt.Errorf("discover LightningBNB characteristics: %w", err)
	}
	var rx bluetooth.DeviceCharacteristic
	var tx bluetooth.DeviceCharacteristic
	haveRX := false
	haveTX := false
	for _, characteristic := range characteristics {
		switch characteristic.UUID() {
		case RXUUID:
			rx = characteristic
			haveRX = true
		case TXUUID:
			tx = characteristic
			haveTX = true
		}
	}
	if !haveRX || !haveTX {
		_ = native.Disconnect()
		return nil, errors.New("selected device does not expose the complete LightningBNB transport")
	}
	conn := newClientPacketConn(device.ID, native, rx, tx)
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
