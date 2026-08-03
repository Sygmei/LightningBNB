package app

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
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
	Name           string
	DialTimeout    time.Duration
	ResumeTimeout  time.Duration
	MaxConnections int
	StatsInterval  time.Duration
	Benchmark      bool
	ErrorOutput    io.Writer
}

func RunServer(ctx context.Context, cfg ServerConfig) error {
	if cfg.ErrorOutput == nil {
		cfg.ErrorOutput = io.Discard
	}
	logger := log.New(cfg.ErrorOutput, "lightningbnb: ", log.LstdFlags)
	target := ""
	if !cfg.Benchmark {
		target = net.JoinHostPort(cfg.TargetHost, strconv.Itoa(cfg.TargetPort))
	}
	listener, err := ble.StartServer(ctx, cfg.Name)
	if err != nil {
		return fmt.Errorf("start Bluetooth server: %w", err)
	}
	defer listener.Close()
	if cfg.Benchmark {
		logger.Printf("advertising %q in benchmark mode", cfg.Name)
	} else {
		logger.Printf("advertising %q; forwarding TCP streams to %s", cfg.Name, target)
	}
	counter := &traffic.Counter{}
	stopStats := startTrafficReporter(ctx, cfg.StatsInterval, counter, logger.Printf)
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
		createdSession := false
		if currentLink == nil {
			createdSession = true
			currentLink = link.NewSessionWithID(hello.ID, link.Config{
				ResumeTimeout:  cfg.ResumeTimeout,
				ReplayWindow:   link.DefaultReplayWindow,
				MaxConnections: cfg.MaxConnections,
			})
			currentMux = mux.NewServer(currentLink, cfg.MaxConnections)
			muxForBridge := currentMux
			if cfg.Benchmark {
				go func() {
					if err := ServeBenchmarkStreams(ctx, muxForBridge, counter, logger.Printf); err != nil && ctx.Err() == nil {
						logger.Printf("benchmark session ended: %v", err)
					}
				}()
			} else {
				go func() {
					if err := bridge.ServeServerWithTraffic(ctx, muxForBridge, target, cfg.DialTimeout, logger.Printf, counter); err != nil && ctx.Err() == nil {
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
			"BLE client connected; packet-mtu=%d session has %d-stream limit",
			currentLink.PacketMTU(),
			currentLink.Config().MaxConnections,
		)
	}
}
