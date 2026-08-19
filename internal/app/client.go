package app

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/Sygmei/LightningBNB/internal/ble"
	"github.com/Sygmei/LightningBNB/internal/bridge"
	"github.com/Sygmei/LightningBNB/internal/link"
	"github.com/Sygmei/LightningBNB/internal/mux"
	"github.com/Sygmei/LightningBNB/internal/traffic"
)

type ClientConfig struct {
	ListenHost         string
	ListenPort         int
	Services           []ClientService
	DeviceID           string
	ScanTimeout        time.Duration
	ResumeTimeout      time.Duration
	MaxConnections     int
	StatsInterval      time.Duration
	StatsTUI           bool
	Compression        bool
	TransportDebug     bool
	Benchmark          *BenchmarkClientConfig
	SuppressListenAddr bool
	Interactive        bool
	Input              io.Reader
	Output             io.Writer
	ErrorOutput        io.Writer
}

func RunClient(ctx context.Context, cfg ClientConfig) error {
	if cfg.Input == nil {
		cfg.Input = strings.NewReader("")
	}
	if cfg.Output == nil {
		cfg.Output = io.Discard
	}
	if cfg.ErrorOutput == nil {
		cfg.ErrorOutput = io.Discard
	}
	console := newRuntimeConsole(cfg.ErrorOutput, cfg.StatsTUI)
	defer console.Close()
	logger := console.Logger()
	if err := ValidateClientServices(cfg.Services); err != nil {
		return err
	}
	listeners := make(map[string][]net.Listener)
	var allListeners []net.Listener
	if len(cfg.Services) == 0 {
		listener, listenErr := net.Listen("tcp", net.JoinHostPort(cfg.ListenHost, strconv.Itoa(cfg.ListenPort)))
		if listenErr != nil {
			return fmt.Errorf("listen for local TCP connections: %w", listenErr)
		}
		listeners[""] = []net.Listener{listener}
		allListeners = append(allListeners, listener)
		if !cfg.SuppressListenAddr {
			_, _ = fmt.Fprintf(cfg.Output, "LISTEN_ADDR=%s\n", listener.Addr())
		}
	} else {
		for _, service := range cfg.Services {
			listener, listenErr := net.Listen("tcp", net.JoinHostPort(cfg.ListenHost, strconv.Itoa(service.LocalPort)))
			if listenErr != nil {
				for _, opened := range allListeners {
					_ = opened.Close()
				}
				return fmt.Errorf("listen for service %q: %w", service.Remote, listenErr)
			}
			listeners[service.Remote] = append(listeners[service.Remote], listener)
			allListeners = append(allListeners, listener)
			if !cfg.SuppressListenAddr {
				_, _ = fmt.Fprintf(cfg.Output, "LISTEN_ADDR[%s]=%s\n", service.Remote, listener.Addr())
			}
		}
	}
	defer func() {
		for _, listener := range allListeners {
			_ = listener.Close()
		}
	}()

	adapter := ble.NewAdapter()
	deviceID := cfg.DeviceID
	var selectedDevice *ble.Device
	var lastDevice *ble.Device
	if deviceID == "" {
		if !cfg.Interactive {
			return errors.New("--device is required when stdin is not an interactive terminal")
		}
		selected, err := chooseDevice(ctx, adapter, cfg.ScanTimeout, cfg.Input, cfg.ErrorOutput)
		if err != nil {
			return err
		}
		selectedDevice = &selected
		deviceID = selected.ServerID
		if deviceID == "" {
			deviceID = selected.ID
		}
	}

	counter := &traffic.Counter{}
	stopStats := startTrafficReporter(ctx, cfg.StatsInterval, counter, console.ReportTraffic)
	defer stopStats()
	clientBridge := bridge.NewClientWithTraffic(cfg.ResumeTimeout, cfg.MaxConnections, logger.Printf, counter)
	bridgeErr := make(chan error, 1)
	go func() { bridgeErr <- clientBridge.ServeServices(ctx, listeners) }()
	var benchmarkDone <-chan error
	var cancelBenchmark context.CancelFunc
	defer func() {
		if cancelBenchmark != nil {
			cancelBenchmark()
		}
	}()

	for ctx.Err() == nil {
		linkSession, err := link.NewSession(link.Config{
			ResumeTimeout:  cfg.ResumeTimeout,
			ReplayWindow:   link.DefaultReplayWindow,
			MaxConnections: cfg.MaxConnections,
			Compression:    cfg.Compression,
		})
		if err != nil {
			return err
		}
		muxSession := mux.NewClientWithCompressionAndTraffic(linkSession, cfg.MaxConnections, cfg.Compression, counter)
		if cfg.StatsTUI {
			console.SetLinkSession(linkSession)
			startLinkHealthReporter(ctx, linkSession, func(snapshot link.TransportSnapshot) {
				bridgeSnapshot := clientBridge.Snapshot()
				console.ReportLinkAndBufferFor(linkSession, snapshot, bridgeSnapshot.WaitingConnections, bridgeSnapshot.OpeningConnections, bridgeSnapshot.ActiveConnections, snapshot.OutstandingBytes+snapshot.BufferedRXBytes)
			})
		}
		if cfg.TransportDebug {
			startTransportDebugReporter(ctx, linkSession, logger.Printf)
		}
		clientBridge.SetEndpoint(&bridge.Endpoint{Link: linkSession, Mux: muxSession, Reset: func() { _ = linkSession.Close() }})
		logger.Printf("starting BLE session for server %s", deviceID)

		for ctx.Err() == nil {
			select {
			case err := <-benchmarkDone:
				_ = muxSession.Close()
				_ = linkSession.Close()
				if err != nil {
					return fmt.Errorf("benchmark failed: %w", err)
				}
				return nil
			case <-linkSession.Done():
				_ = muxSession.Close()
				logger.Printf("BLE session ended: %v", linkSession.Err())
				if cfg.Benchmark != nil {
					return fmt.Errorf("benchmark BLE session ended: %w", linkSession.Err())
				}
				goto nextSession
			default:
			}
			if linkSession.IsBound() {
				select {
				case <-ctx.Done():
				case <-linkSession.Done():
				case <-time.After(200 * time.Millisecond):
				}
				continue
			}

			var device ble.Device
			if selectedDevice != nil {
				device = *selectedDevice
				selectedDevice = nil
			} else {
				device, err = adapter.Find(ctx, deviceID, cfg.ScanTimeout)
				if err != nil {
					logger.Printf("discover server: %v", err)
					if lastDevice != nil {
						cached := *lastDevice
						lastDevice = nil
						selectedDevice = &cached
						logger.Printf("trying last known platform device %s", cached.ID)
						continue
					}
					if !waitContext(ctx, time.Second) {
						break
					}
					continue
				}
			}
			if device.ServerID != "" && deviceID != device.ServerID {
				logger.Printf("resolved platform device %s to stable server ID %s", device.ID, device.ServerID)
				deviceID = device.ServerID
			}
			connectCtx, cancel := context.WithTimeout(ctx, ble.ConnectAttemptTimeout)
			packetConn, err := adapter.Connect(connectCtx, device)
			cancel()
			if err == nil {
				if connectedID := ble.ConnectedServerID(packetConn); connectedID != "" && deviceID != connectedID {
					logger.Printf("resolved platform device %s to stable server ID %s", device.ID, connectedID)
					deviceID = connectedID
				}
				handshakeCtx, cancelHandshake := context.WithTimeout(ctx, 15*time.Second)
				err = linkSession.BindClient(handshakeCtx, packetConn)
				cancelHandshake()
			}
			if err == nil {
				lastDevice = &device
			}
			if err != nil {
				if packetConn != nil {
					_ = packetConn.Close()
				}
				logger.Printf("connect to server: %v", err)
				if link.RejectionReason(err) == "resume failed" {
					logger.Printf("server no longer has the resumable session; starting a fresh session")
					_ = muxSession.Close()
					_ = linkSession.Close()
					goto nextSession
				}
				if !waitContext(ctx, time.Second) {
					break
				}
				continue
			}
			logger.Printf(
				"connected to %s; packet-mtu=%d compression=%t",
				deviceID,
				linkSession.PacketMTU(),
				linkSession.Config().Compression,
			)
			if cfg.Benchmark != nil && benchmarkDone == nil {
				benchmarkCfg := *cfg.Benchmark
				benchmarkCfg.ErrorOutput = cfg.ErrorOutput
				benchmarkCfg.StatsTUI = cfg.StatsTUI
				benchmarkCfg.console = console
				benchmarkCfg.counter = counter
				benchmarkCtx, cancel := context.WithCancel(ctx)
				cancelBenchmark = cancel
				done := make(chan error, 1)
				benchmarkDone = done
				go func() { done <- RunMuxBenchmarkClient(benchmarkCtx, benchmarkCfg, muxSession) }()
			}
		}
		_ = muxSession.Close()
		_ = linkSession.Close()
		break

	nextSession:
		continue
	}

	select {
	case err := <-bridgeErr:
		return err
	default:
		return nil
	}
}

func chooseDevice(ctx context.Context, adapter *ble.Adapter, timeout time.Duration, input io.Reader, output io.Writer) (ble.Device, error) {
	// Resolve the GATT identity while listing devices. The platform address
	// (Device.ID) can change between BLE sessions, whereas ServerID is persisted
	// by the server and is the value that must be passed back to --device.
	devices, err := adapter.Scan(ctx, timeout)
	if err != nil {
		return ble.Device{}, err
	}
	sortDevices(devices)
	if len(devices) == 0 {
		return ble.Device{}, errors.New("no LightningBNB servers found")
	}
	_, _ = fmt.Fprintln(output, "Nearby LightningBNB servers:")
	for i, device := range devices {
		name := deviceDisplayName(device)
		selectedID := device.ServerID
		if selectedID == "" {
			selectedID = device.ID
		}
		_, _ = fmt.Fprintf(output, "  %d) %s  RSSI=%d  ID=%s  PLATFORM_ID=%s\n", i+1, name, device.RSSI, selectedID, device.ID)
	}
	_, _ = fmt.Fprint(output, "Select server: ")
	scanner := bufio.NewScanner(input)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return ble.Device{}, err
		}
		return ble.Device{}, errors.New("no server selection provided")
	}
	selection, err := strconv.Atoi(strings.TrimSpace(scanner.Text()))
	if err != nil || selection < 1 || selection > len(devices) {
		return ble.Device{}, errors.New("invalid server selection")
	}
	return devices[selection-1], nil
}

func deviceDisplayName(device ble.Device) string {
	if name := strings.TrimSpace(device.Name); name != "" {
		return name
	}
	if device.ServerID != "" {
		return device.ServerID
	}
	return "(unnamed)"
}

func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
