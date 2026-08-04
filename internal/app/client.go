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
	DeviceID           string
	ScanTimeout        time.Duration
	ResumeTimeout      time.Duration
	MaxConnections     int
	StatsInterval      time.Duration
	StatsTUI           bool
	Compression        bool
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
	listener, err := net.Listen("tcp", net.JoinHostPort(cfg.ListenHost, strconv.Itoa(cfg.ListenPort)))
	if err != nil {
		return fmt.Errorf("listen for local TCP connections: %w", err)
	}
	defer listener.Close()
	if !cfg.SuppressListenAddr {
		_, _ = fmt.Fprintf(cfg.Output, "LISTEN_ADDR=%s\n", listener.Addr())
	}

	adapter := ble.NewAdapter()
	deviceID := cfg.DeviceID
	if deviceID == "" {
		if !cfg.Interactive {
			return errors.New("--device is required when stdin is not an interactive terminal")
		}
		deviceID, err = chooseDevice(ctx, adapter, cfg.ScanTimeout, cfg.Input, cfg.ErrorOutput)
		if err != nil {
			return err
		}
	}

	counter := &traffic.Counter{}
	stopStats := startTrafficReporter(ctx, cfg.StatsInterval, counter, console.ReportTraffic)
	defer stopStats()
	clientBridge := bridge.NewClientWithTraffic(cfg.ResumeTimeout, cfg.MaxConnections, logger.Printf, counter)
	bridgeErr := make(chan error, 1)
	go func() { bridgeErr <- clientBridge.Serve(ctx, listener) }()
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
		muxSession := mux.NewClientWithCompression(linkSession, cfg.MaxConnections, cfg.Compression)
		clientBridge.SetEndpoint(&bridge.Endpoint{Link: linkSession, Mux: muxSession})
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

			device, err := adapter.Find(ctx, deviceID, cfg.ScanTimeout)
			if err != nil {
				logger.Printf("discover server: %v", err)
				if !waitContext(ctx, time.Second) {
					break
				}
				continue
			}
			connectCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			packetConn, err := adapter.Connect(connectCtx, device)
			if err == nil {
				err = linkSession.BindClient(connectCtx, packetConn)
			}
			cancel()
			if err != nil {
				if packetConn != nil {
					_ = packetConn.Close()
				}
				logger.Printf("connect to server: %v", err)
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

func chooseDevice(ctx context.Context, adapter *ble.Adapter, timeout time.Duration, input io.Reader, output io.Writer) (string, error) {
	devices, err := adapter.Scan(ctx, timeout)
	if err != nil {
		return "", err
	}
	sortDevices(devices)
	if len(devices) == 0 {
		return "", errors.New("no LightningBNB servers found")
	}
	_, _ = fmt.Fprintln(output, "Nearby LightningBNB servers:")
	for i, device := range devices {
		name := device.Name
		if name == "" {
			name = "(unnamed)"
		}
		_, _ = fmt.Fprintf(output, "  %d) %s  RSSI=%d  ID=%s\n", i+1, name, device.RSSI, device.ID)
	}
	_, _ = fmt.Fprint(output, "Select server: ")
	scanner := bufio.NewScanner(input)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return "", err
		}
		return "", errors.New("no server selection provided")
	}
	selection, err := strconv.Atoi(strings.TrimSpace(scanner.Text()))
	if err != nil || selection < 1 || selection > len(devices) {
		return "", errors.New("invalid server selection")
	}
	return devices[selection-1].ID, nil
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
