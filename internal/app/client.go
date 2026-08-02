package app

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/Sygmei/LightningBNB/internal/ble"
	"github.com/Sygmei/LightningBNB/internal/bridge"
	"github.com/Sygmei/LightningBNB/internal/link"
	"github.com/Sygmei/LightningBNB/internal/mux"
)

type ClientConfig struct {
	ListenHost     string
	ListenPort     int
	DeviceID       string
	ScanTimeout    time.Duration
	ResumeTimeout  time.Duration
	MaxConnections int
	Interactive    bool
	Input          io.Reader
	Output         io.Writer
	ErrorOutput    io.Writer
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
	logger := log.New(cfg.ErrorOutput, "lightningbnb: ", log.LstdFlags)
	listener, err := net.Listen("tcp", net.JoinHostPort(cfg.ListenHost, strconv.Itoa(cfg.ListenPort)))
	if err != nil {
		return fmt.Errorf("listen for local TCP connections: %w", err)
	}
	defer listener.Close()
	_, _ = fmt.Fprintf(cfg.Output, "LISTEN_ADDR=%s\n", listener.Addr())

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

	clientBridge := bridge.NewClient(cfg.ResumeTimeout, cfg.MaxConnections, logger.Printf)
	bridgeErr := make(chan error, 1)
	go func() { bridgeErr <- clientBridge.Serve(ctx, listener) }()

	for ctx.Err() == nil {
		linkSession, err := link.NewSession(link.Config{
			ResumeTimeout:  cfg.ResumeTimeout,
			ReplayWindow:   link.DefaultReplayWindow,
			MaxConnections: cfg.MaxConnections,
		})
		if err != nil {
			return err
		}
		muxSession := mux.NewClient(linkSession, cfg.MaxConnections)
		clientBridge.SetEndpoint(&bridge.Endpoint{Link: linkSession, Mux: muxSession})
		logger.Printf("starting BLE session for server %s", deviceID)

		for ctx.Err() == nil {
			select {
			case <-linkSession.Done():
				_ = muxSession.Close()
				logger.Printf("BLE session ended: %v", linkSession.Err())
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
			logger.Printf("connected to %s", deviceID)
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
