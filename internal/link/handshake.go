package link

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/Sygmei/LightningBNB/internal/protocol"
)

const (
	packetHelloID  byte = 1
	packetHello    byte = 2
	packetHelloAck byte = 3
	packetData     byte = 4
	packetAck      byte = 5
	packetPing     byte = 6
	packetPong     byte = 7
	packetClose    byte = 8
	packetReject   byte = 9

	minimumPacketMTU = 20

	capabilityCompression byte = 1 << 0
	knownCapabilities          = capabilityCompression
)

type SessionID [16]byte

type Hello struct {
	ID                  SessionID
	ExpectedRemote      uint64
	ResumeTimeout       time.Duration
	MaxConnections      int
	AdvertisedPacketMTU int
	Compression         bool
}

var (
	ErrProtocolVersion = errors.New("unsupported LightningBNB protocol version")
	ErrRejected        = errors.New("BLE session rejected")
	ErrSessionState    = errors.New("peer requested unavailable session state")
	ErrHandshake       = errors.New("invalid BLE handshake")
)

func ReadHello(ctx context.Context, conn PacketConn) (Hello, error) {
	var hello Hello
	var haveID bool
	for {
		select {
		case <-ctx.Done():
			return Hello{}, ctx.Err()
		case packet, ok := <-conn.Receive():
			if !ok {
				return Hello{}, errors.New("BLE connection closed during handshake")
			}
			if len(packet) == 0 {
				continue
			}
			switch packet[0] {
			case packetHelloID:
				if len(packet) != 18 {
					continue
				}
				if packet[1] != protocol.Version {
					return Hello{}, ErrProtocolVersion
				}
				copy(hello.ID[:], packet[2:18])
				haveID = true
			case packetHello:
				if !haveID || (len(packet) != 17 && len(packet) != 18) {
					continue
				}
				hello.ExpectedRemote = binary.BigEndian.Uint64(packet[1:9])
				hello.ResumeTimeout = time.Duration(binary.BigEndian.Uint32(packet[9:13])) * time.Millisecond
				hello.MaxConnections = int(binary.BigEndian.Uint16(packet[13:15]))
				hello.AdvertisedPacketMTU = int(binary.BigEndian.Uint16(packet[15:17]))
				if len(packet) == 18 {
					if packet[17]&^knownCapabilities != 0 {
						return Hello{}, ErrHandshake
					}
					hello.Compression = packet[17]&capabilityCompression != 0
				}
				if hello.ResumeTimeout <= 0 || hello.MaxConnections <= 0 || hello.AdvertisedPacketMTU < minimumPacketMTU {
					return Hello{}, ErrHandshake
				}
				return hello, nil
			}
		}
	}
}

func SendReject(ctx context.Context, conn PacketConn, reason string) error {
	if len(reason) > 18 {
		reason = reason[:18]
	}
	return conn.Send(ctx, append([]byte{packetReject}, reason...))
}

func encodeHelloID(id SessionID) []byte {
	packet := make([]byte, 18)
	packet[0] = packetHelloID
	packet[1] = protocol.Version
	copy(packet[2:], id[:])
	return packet
}

func encodeHello(id SessionID, expectedRemote uint64, cfg Config, packetMTU int) ([]byte, []byte) {
	length := 17
	if cfg.Compression {
		length++
	}
	second := make([]byte, length)
	second[0] = packetHello
	binary.BigEndian.PutUint64(second[1:9], expectedRemote)
	binary.BigEndian.PutUint32(second[9:13], durationMilliseconds(cfg.ResumeTimeout))
	binary.BigEndian.PutUint16(second[13:15], uint16(cfg.MaxConnections))
	binary.BigEndian.PutUint16(second[15:17], uint16(normalizeMTU(packetMTU)))
	if cfg.Compression {
		second[17] = capabilityCompression
	}
	return encodeHelloID(id), second
}

func encodeHelloAck(expectedRemote uint64, cfg Config, packetMTU int) []byte {
	length := 18
	if cfg.Compression {
		length++
	}
	packet := make([]byte, length)
	packet[0] = packetHelloAck
	packet[1] = protocol.Version
	binary.BigEndian.PutUint64(packet[2:10], expectedRemote)
	binary.BigEndian.PutUint32(packet[10:14], durationMilliseconds(cfg.ResumeTimeout))
	binary.BigEndian.PutUint16(packet[14:16], uint16(cfg.MaxConnections))
	binary.BigEndian.PutUint16(packet[16:18], uint16(normalizeMTU(packetMTU)))
	if cfg.Compression {
		packet[18] = capabilityCompression
	}
	return packet
}

func decodeHelloAck(packet []byte) (uint64, Config, int, error) {
	if (len(packet) != 18 && len(packet) != 19) || packet[0] != packetHelloAck {
		return 0, Config{}, 0, ErrHandshake
	}
	if packet[1] != protocol.Version {
		return 0, Config{}, 0, ErrProtocolVersion
	}
	cfg := Config{
		ResumeTimeout:  time.Duration(binary.BigEndian.Uint32(packet[10:14])) * time.Millisecond,
		MaxConnections: int(binary.BigEndian.Uint16(packet[14:16])),
	}
	if len(packet) == 19 {
		if packet[18]&^knownCapabilities != 0 {
			return 0, Config{}, 0, ErrHandshake
		}
		cfg.Compression = packet[18]&capabilityCompression != 0
	}
	mtu := int(binary.BigEndian.Uint16(packet[16:18]))
	if cfg.ResumeTimeout <= 0 || cfg.MaxConnections <= 0 || mtu < minimumPacketMTU {
		return 0, Config{}, 0, ErrHandshake
	}
	return binary.BigEndian.Uint64(packet[2:10]), cfg, mtu, nil
}

func durationMilliseconds(d time.Duration) uint32 {
	ms := d / time.Millisecond
	if ms <= 0 {
		return 1
	}
	if ms > time.Duration(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(ms)
}

func normalizeMTU(mtu int) int {
	if mtu < minimumPacketMTU {
		return minimumPacketMTU
	}
	if mtu > 244 {
		return 244
	}
	return mtu
}

func validatePeerExpected(base, next, expected uint64) error {
	if expected < base || expected > next {
		return fmt.Errorf("%w: peer expects %d, retained range is [%d,%d]", ErrSessionState, expected, base, next)
	}
	return nil
}
