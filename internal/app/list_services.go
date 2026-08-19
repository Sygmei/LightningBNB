package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Sygmei/LightningBNB/internal/ble"
	"github.com/Sygmei/LightningBNB/internal/link"
	"github.com/Sygmei/LightningBNB/internal/mux"
)

type ServicesConfig struct {
	DeviceID    string
	ScanTimeout time.Duration
	Interactive bool
	Input       io.Reader
	Output      io.Writer
	ErrorOutput io.Writer
}

func ListServices(ctx context.Context, cfg ServicesConfig) error {
	if cfg.Input == nil {
		cfg.Input = strings.NewReader("")
	}
	if cfg.Output == nil {
		cfg.Output = io.Discard
	}
	if cfg.ErrorOutput == nil {
		cfg.ErrorOutput = io.Discard
	}
	adapter := ble.NewAdapter()
	deviceID := cfg.DeviceID
	var selected *ble.Device
	if deviceID == "" {
		if !cfg.Interactive {
			return errors.New("--device is required when stdin is not an interactive terminal")
		}
		device, err := chooseDevice(ctx, adapter, cfg.ScanTimeout, cfg.Input, cfg.ErrorOutput)
		if err != nil {
			return err
		}
		selected = &device
		deviceID = device.ServerID
		if deviceID == "" {
			deviceID = device.ID
		}
	}
	device := ble.Device{}
	var err error
	if selected != nil {
		device = *selected
	} else {
		device, err = adapter.Find(ctx, deviceID, cfg.ScanTimeout)
		if err != nil {
			return fmt.Errorf("discover server: %w", err)
		}
	}
	connectCtx, cancel := context.WithTimeout(ctx, ble.ConnectAttemptTimeout)
	packetConn, err := adapter.Connect(connectCtx, device)
	cancel()
	if err != nil {
		return fmt.Errorf("connect to server: %w", err)
	}
	defer packetConn.Close()
	if connectedID := ble.ConnectedServerID(packetConn); connectedID != "" {
		deviceID = connectedID
	}
	linkSession, err := link.NewSession(link.Config{
		ResumeTimeout:  60 * time.Second,
		ReplayWindow:   link.DefaultReplayWindow,
		MaxConnections: link.DefaultMaxConnections,
	})
	if err != nil {
		return err
	}
	defer linkSession.Close()
	handshakeCtx, cancelHandshake := context.WithTimeout(ctx, 15*time.Second)
	err = linkSession.BindClient(handshakeCtx, packetConn)
	cancelHandshake()
	if err != nil {
		return fmt.Errorf("connect session: %w", err)
	}
	muxSession := mux.NewClient(linkSession, link.DefaultMaxConnections)
	defer muxSession.Close()
	servicesCtx, cancelServices := context.WithTimeout(ctx, 10*time.Second)
	services, err := muxSession.Services(servicesCtx)
	cancelServices()
	if err != nil {
		return fmt.Errorf("read services: %w", err)
	}
	_, _ = fmt.Fprintf(cfg.Output, "SERVER_ID=%s\nNAME\tPORT\n", deviceID)
	for _, service := range services {
		name := service.Name
		if name == "" {
			name = "(default)"
		}
		_, _ = fmt.Fprintf(cfg.Output, "%s\t%d\n", name, service.Port)
	}
	return nil
}
