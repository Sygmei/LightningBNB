package app

import (
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/Sygmei/LightningBNB/internal/ble"
	"github.com/Sygmei/LightningBNB/internal/bridge"
	"github.com/Sygmei/LightningBNB/internal/link"
	"github.com/Sygmei/LightningBNB/internal/mux"
	"github.com/Sygmei/LightningBNB/internal/traffic"
)

type ServerConfig struct {
	TargetHost     string
	TargetPort     int
	Services       []mux.Service
	Name           string
	DialTimeout    time.Duration
	ResumeTimeout  time.Duration
	MaxConnections int
	StatsInterval  time.Duration
	StatsTUI       bool
	Benchmark      bool
	Compression    bool
	TransportDebug bool
	PreventSleep   bool
	ServerIDFile   string
	ErrorOutput    io.Writer
}

func RunServer(ctx context.Context, cfg ServerConfig) error {
	if cfg.ErrorOutput == nil {
		cfg.ErrorOutput = io.Discard
	}
	console := newRuntimeConsole(cfg.ErrorOutput, cfg.StatsTUI)
	defer console.Close()
	logger := console.Logger()
	target := ""
	if !cfg.Benchmark {
		if len(cfg.Services) == 0 {
			target = net.JoinHostPort(cfg.TargetHost, strconv.Itoa(cfg.TargetPort))
		} else {
			for _, service := range cfg.Services {
				if target == "" {
					target = net.JoinHostPort(cfg.TargetHost, strconv.Itoa(service.Port))
				}
			}
		}
	}
	serverID, serverIDPath, err := loadOrCreateServerID(cfg.ServerIDFile)
	if err != nil {
		return err
	}
	listener, err := startBLEServer(ctx, cfg.Name, serverID, func(format string, args ...any) { logger.Printf(format, args...) })
	if err != nil {
		return fmt.Errorf("start Bluetooth server: %w", err)
	}
	defer listener.Close()
	logger.Printf("stable server ID %s (stored in %s)", serverID, serverIDPath)
	if cfg.PreventSleep {
		inhibitor, err := acquireSleepInhibitor(ctx)
		if err != nil {
			return fmt.Errorf("prevent system sleep: %w", err)
		}
		defer func() {
			if err := inhibitor.Close(); err != nil {
				logger.Printf("release system sleep inhibitor: %v", err)
			}
		}()
		logger.Printf("automatic system sleep prevention enabled")
	}
	if cfg.Benchmark {
		logger.Printf("advertising %q in benchmark mode", cfg.Name)
	} else if len(cfg.Services) > 0 {
		logger.Printf("advertising %q; forwarding configured services", cfg.Name)
	} else {
		logger.Printf("advertising %q; forwarding TCP streams to %s", cfg.Name, target)
	}
	counter := &traffic.Counter{}
	stopStats := startTrafficReporter(ctx, cfg.StatsInterval, counter, console.ReportTraffic)
	defer stopStats()

	var currentLink *link.Session
	var currentMux *mux.Session
	defer func() {
		if currentMux != nil {
			_ = currentMux.Close()
		}
		if currentLink != nil {
			_ = currentLink.Close()
		}
	}()
	for {
		packetConn, err := listener.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		handshakeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		hello, err := link.ReadHello(handshakeCtx, packetConn)
		if err != nil {
			cancel()
			_ = packetConn.Close()
			logger.Printf("rejecting invalid BLE handshake: %v", err)
			continue
		}

		if currentLink != nil {
			select {
			case <-currentLink.Done():
				currentLink = nil
				currentMux = nil
			default:
			}
		}
		if currentLink != nil && !currentLink.Matches(hello.ID) {
			_ = link.SendReject(handshakeCtx, packetConn, "server busy")
			cancel()
			_ = packetConn.Close()
			logger.Printf("rejected another BLE client while a session is resumable")
			continue
		}
		if hello.Compression && !cfg.Compression {
			_ = link.SendReject(handshakeCtx, packetConn, "compression off")
			cancel()
			_ = packetConn.Close()
			logger.Printf("rejected BLE client requesting disabled compression")
			continue
		}
		createdSession := false
		if currentLink == nil {
			createdSession = true
			currentLink = link.NewSessionWithID(hello.ID, link.Config{
				ResumeTimeout:  cfg.ResumeTimeout,
				ReplayWindow:   link.DefaultReplayWindow,
				MaxConnections: cfg.MaxConnections,
				Compression:    hello.Compression,
			})
			services := cfg.Services
			if len(services) == 0 && !cfg.Benchmark {
				services = []mux.Service{{Port: cfg.TargetPort}}
			}
			currentMux = mux.NewServerWithServicesAndCompressionAndTraffic(currentLink, cfg.MaxConnections, hello.Compression, counter, services)
			if cfg.StatsTUI {
				console.SetLinkSession(currentLink)
				sessionMux := currentMux
				startLinkHealthReporter(ctx, currentLink, func(snapshot link.TransportSnapshot) {
					muxSnapshot := sessionMux.Snapshot()
					console.ReportLinkAndBufferFor(currentLink, snapshot, muxSnapshot.PendingAccepts, muxSnapshot.OpeningStreams, muxSnapshot.Streams, snapshot.OutstandingBytes+snapshot.BufferedRXBytes)
				})
			}
			if cfg.TransportDebug {
				startTransportDebugReporter(ctx, currentLink, logger.Printf)
			}
			muxForBridge := currentMux
			go func(sessionMux *mux.Session, sessionLink *link.Session) {
				<-sessionMux.Done()
				if sessionLink.IsBound() {
					_ = sessionLink.Close()
				}
			}(currentMux, currentLink)
			if cfg.Benchmark {
				go func() {
					if err := ServeBenchmarkStreams(ctx, muxForBridge, counter, logger.Printf); err != nil && ctx.Err() == nil {
						logger.Printf("benchmark session ended: %v", err)
					}
				}()
			} else {
				serverServices := append([]mux.Service(nil), services...)
				go func() {
					if len(serverServices) == 0 {
						if err := bridge.ServeServerWithTraffic(ctx, muxForBridge, target, cfg.DialTimeout, logger.Printf, counter); err != nil && ctx.Err() == nil {
							logger.Printf("TCP bridge session ended: %v", err)
						}
						return
					}
					if err := bridge.ServeServerWithServicesWithTraffic(ctx, muxForBridge, cfg.TargetHost, serverServices, cfg.DialTimeout, logger.Printf, counter); err != nil && ctx.Err() == nil {
						logger.Printf("TCP bridge session ended: %v", err)
					}
				}()
			}
		}
		if err := currentLink.BindServer(handshakeCtx, packetConn, hello); err != nil {
			_ = link.SendReject(handshakeCtx, packetConn, "resume failed")
			cancel()
			_ = packetConn.Close()
			logger.Printf("BLE session bind failed: %v", err)
			if createdSession {
				_ = currentMux.Close()
				_ = currentLink.Close()
				currentMux = nil
				currentLink = nil
			}
			continue
		}
		cancel()
		logger.Printf(
			"BLE client connected; packet-mtu=%d compression=%t session has %d-stream limit",
			currentLink.PacketMTU(),
			currentLink.Config().Compression,
			currentLink.Config().MaxConnections,
		)
	}
}

func startBLEServer(ctx context.Context, name string, serverID ble.ServerID, logf func(string, ...any)) (ble.PeripheralListener, error) {
	const attempts = 4
	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		var listener ble.PeripheralListener
		listener, err = ble.StartServer(ctx, name, serverID)
		if err == nil {
			return listener, nil
		}
		if runtime.GOOS != "windows" || attempt == attempts || ctx.Err() != nil {
			break
		}
		// Windows can leave the WinRT GATT publisher in an asynchronous
		// stopping state for a short period after process shutdown.
		if !strings.Contains(err.Error(), "HRESULT") && !strings.Contains(err.Error(), "async operation") {
			break
		}
		delay := time.Duration(1<<(attempt-1)) * time.Second
		if logf != nil {
			logf("Bluetooth server startup attempt %d/%d failed: %v; retrying in %s", attempt, attempts, err, delay)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, err
}
