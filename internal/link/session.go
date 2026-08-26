package link

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/Sygmei/LightningBNB/internal/protocol"
)

const (
	DefaultReplayWindow   = 1 << 20
	DefaultResumeTimeout  = 60 * time.Second
	DefaultMaxConnections = 32

	retransmitInterval       = time.Second
	heartbeatInterval        = 3 * time.Second
	heartbeatResponseTimeout = 2 * time.Second
	heartbeatFailureLimit    = 2
	heartbeatCheckInterval   = 250 * time.Millisecond
	sendTimeout              = 5 * time.Second
	// Keep enough queued live data and in-flight packets to cover the
	// bandwidth-delay product of desktop BLE links. The previous 16 KiB/32-	// packet limits could cap a high-MTU connection near 50 KiB/s when ACK
	// latency approached 150ms.
	liveWriteWindow            = 64 << 10
	fastRetransmitACKs         = 3
	ackBatchPackets            = 8
	initialFlightWindowPackets = ackBatchPackets
	maxFlightWindowPackets     = 128
	ackMaxDelay                = 40 * time.Millisecond
	sendLoopInterval           = 10 * time.Millisecond
)

var (
	ErrResumeTimeout     = errors.New("BLE resume timeout expired")
	ErrNotConnected      = errors.New("BLE session is not connected")
	ErrSequenceExhausted = errors.New("BLE session byte sequence exhausted")
	ErrCompression       = errors.New("BLE peer does not support requested compression")
)

type Config struct {
	ResumeTimeout  time.Duration
	ReplayWindow   int
	MaxConnections int
	Compression    bool
}

func (cfg Config) normalized() Config {
	if cfg.ResumeTimeout <= 0 {
		cfg.ResumeTimeout = DefaultResumeTimeout
	}
	if cfg.ReplayWindow <= 0 {
		cfg.ReplayWindow = DefaultReplayWindow
	}
	if cfg.MaxConnections <= 0 {
		cfg.MaxConnections = DefaultMaxConnections
	}
	if cfg.MaxConnections > int(^uint16(0)) {
		cfg.MaxConnections = int(^uint16(0))
	}
	return cfg
}

type binding struct {
	conn              PacketConn
	gen               uint64
	mtu               int
	cancel            context.CancelFunc
	lastRX            time.Time
	lastTX            time.Time
	lastHeartbeat     time.Time
	heartbeatPending  bool
	heartbeatSentAt   time.Time
	heartbeatFailures int
	sendNext          uint64
	retransmitAt      time.Time
	duplicateACK      int
	flightPackets     int
}

// Session is a reliable, ordered, resumable byte stream over replaceable BLE
// packet connections. It implements net.Conn for the multiplexing layer.
type Session struct {
	mu sync.Mutex

	id     SessionID
	config Config

	txBase uint64
	txNext uint64
	txBuf  []byte
	rxNext uint64
	rxBuf  []byte
	// rxPending retains valid DATA packets that arrived ahead of rxNext. Some
	// desktop GATT stacks deliver write callbacks concurrently even though ATT
	// writes were issued in order. Keeping one bounded reordering window avoids
	// turning that callback scheduling into a retransmission storm.
	rxPending      map[uint64][]byte
	rxPendingBytes int

	current    *binding
	bindGen    uint64
	detachedAt time.Time

	ackDirty         bool
	ackPackets       int
	ackDeadline      time.Time
	pongDirty        bool
	helloReply       []byte
	pendingHelloID   SessionID
	havePendingHello bool
	stats            transportStats

	readDeadline  time.Time
	writeDeadline time.Time

	notify chan struct{}
	done   chan struct{}
	closed bool
	err    error
}

func NewSession(cfg Config) (*Session, error) {
	var id SessionID
	if _, err := rand.Read(id[:]); err != nil {
		return nil, fmt.Errorf("create session id: %w", err)
	}
	return NewSessionWithID(id, cfg), nil
}

func NewSessionWithID(id SessionID, cfg Config) *Session {
	return &Session{
		id:     id,
		config: cfg.normalized(),
		notify: make(chan struct{}),
		done:   make(chan struct{}),
	}
}

func (s *Session) ID() SessionID {
	return s.id
}

func (s *Session) Config() Config {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.config
}

func (s *Session) Matches(id SessionID) bool {
	return s.id == id
}

func (s *Session) BindClient(ctx context.Context, conn PacketConn) error {
	if conn == nil {
		return errors.New("nil BLE packet connection")
	}
	s.mu.Lock()
	if s.closed {
		err := s.sessionErrorLocked()
		s.mu.Unlock()
		return err
	}
	id := s.id
	rxNext := s.rxNext
	cfg := s.config
	s.mu.Unlock()

	first, second := encodeHello(id, rxNext, cfg, conn.MTU())
	var peerExpected uint64
	var peerCfg Config
	var peerMTU int
	for {
		if err := conn.Send(ctx, first); err != nil {
			return fmt.Errorf("send session identity: %w", err)
		}
		if err := conn.Send(ctx, second); err != nil {
			return fmt.Errorf("send session resume state: %w", err)
		}
		retry := time.NewTimer(retransmitInterval)
	waitForReply:
		for {
			select {
			case <-ctx.Done():
				retry.Stop()
				return ctx.Err()
			case <-retry.C:
				break waitForReply
			case packet, ok := <-conn.Receive():
				if !ok {
					retry.Stop()
					return errors.New("BLE connection closed during handshake")
				}
				if len(packet) > 0 && packet[0] == packetReject {
					retry.Stop()
					return &RejectedError{Reason: string(packet[1:])}
				}
				var err error
				peerExpected, peerCfg, peerMTU, err = decodeHelloAck(packet)
				if err == nil {
					retry.Stop()
					break waitForReply
				}
			}
		}
		if peerMTU != 0 {
			break
		}
	}
	if cfg.Compression && !peerCfg.Compression {
		return ErrCompression
	}
	if !cfg.Compression && peerCfg.Compression {
		return ErrHandshake
	}

	s.mu.Lock()
	if err := s.reconcilePeerLocked(peerExpected); err != nil {
		s.mu.Unlock()
		return err
	}
	mtu := min(normalizeMTU(conn.MTU()), peerMTU)
	s.negotiateLocked(peerCfg)
	s.mu.Unlock()
	return s.attach(conn, mtu)
}

func (s *Session) BindServer(ctx context.Context, conn PacketConn, hello Hello) error {
	if !s.Matches(hello.ID) {
		return ErrRejected
	}
	if hello.Compression != s.Config().Compression {
		return ErrCompression
	}
	s.mu.Lock()
	if s.closed {
		err := s.sessionErrorLocked()
		s.mu.Unlock()
		return err
	}
	if err := s.reconcilePeerLocked(hello.ExpectedRemote); err != nil {
		s.mu.Unlock()
		return err
	}
	peerCfg := Config{
		ResumeTimeout:  hello.ResumeTimeout,
		MaxConnections: hello.MaxConnections,
		Compression:    hello.Compression,
	}
	mtu := min(normalizeMTU(conn.MTU()), normalizeMTU(hello.AdvertisedPacketMTU))
	s.negotiateLocked(peerCfg)
	ack := encodeHelloAck(s.rxNext, s.config, mtu)
	s.mu.Unlock()
	if err := conn.Send(ctx, ack); err != nil {
		return fmt.Errorf("send handshake response: %w", err)
	}
	return s.attach(conn, mtu)
}

func (s *Session) attach(conn PacketConn, mtu int) error {
	ctx, cancel := context.WithCancel(context.Background())
	now := time.Now()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		cancel()
		return s.sessionError()
	}
	old := s.current
	s.bindGen++
	b := &binding{
		conn:            conn,
		gen:             s.bindGen,
		mtu:             min(normalizeMTU(conn.MTU()), normalizeMTU(mtu)),
		cancel:          cancel,
		lastRX:          now,
		lastTX:          now,
		heartbeatSentAt: now,
		sendNext:        s.txBase,
		flightPackets:   initialFlightWindowPackets,
	}
	// Pending packets were not cumulatively acknowledged. A replacement binding
	// can use a different MTU and therefore different fragment boundaries, so
	// replay them from the sender instead of retaining incompatible fragments.
	s.rxPending = nil
	s.rxPendingBytes = 0
	s.current = b
	s.detachedAt = time.Time{}
	if s.ackDirty {
		s.ackDeadline = now
	}
	s.signalLocked()
	s.mu.Unlock()
	if old != nil {
		old.cancel()
		_ = old.conn.Close()
	}
	go s.receiveLoop(ctx, b)
	go s.sendLoop(ctx, b)
	go s.heartbeatLoop(ctx, b)
	return nil
}

func (s *Session) receiveLoop(ctx context.Context, b *binding) {
	for {
		select {
		case <-ctx.Done():
			return
		case packet, ok := <-b.conn.Receive():
			if !ok {
				s.failBinding(b, time.Now())
				return
			}
			if err := s.handlePacket(b, packet); err != nil {
				s.failBinding(b, time.Now())
				return
			}
		}
	}
}

func (s *Session) handlePacket(b *binding, packet []byte) error {
	if len(packet) == 0 {
		return nil
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current != b || s.closed {
		return nil
	}
	b.lastRX = now
	// Any valid packet proves that the ATT path is still alive. Clear an
	// outstanding probe even when the peer is sending data rather than PONG.
	b.heartbeatPending = false
	b.heartbeatFailures = 0
	switch packet[0] {
	case packetData:
		if len(packet) <= 9 {
			return ErrHandshake
		}
		seq := binary.BigEndian.Uint64(packet[1:9])
		payload := packet[9:]
		s.stats.dataRXPackets++
		advancedPackets, buffered, rejected, err := s.acceptDataLocked(seq, payload)
		if err != nil {
			return err
		}
		if rejected {
			s.stats.rejectedDataPackets++
		}
		s.ackDirty = true
		if advancedPackets > 0 {
			s.ackPackets += advancedPackets
			if s.ackDeadline.IsZero() {
				s.ackDeadline = now.Add(ackMaxDelay)
			}
		} else if buffered || rejected {
			// Gaps, duplicates, overlaps, and receive-window pressure need an immediate
			// cumulative ACK so the sender can restart at the expected offset.
			s.ackPackets = ackBatchPackets
			s.ackDeadline = now
		}
		s.signalLocked()
	case packetAck:
		if len(packet) != 9 {
			return ErrHandshake
		}
		if err := s.handleAckLocked(b, binary.BigEndian.Uint64(packet[1:9]), now); err != nil {
			return err
		}
		s.stats.ackRXPackets++
	case packetPing:
		if len(packet) != 1 {
			return ErrHandshake
		}
		s.pongDirty = true
		s.signalLocked()
	case packetPong:
		if len(packet) != 1 {
			return ErrHandshake
		}
		s.stats.heartbeatRX++
		b.lastHeartbeat = now
	case packetClose:
		go s.closeWithError(io.EOF)
	case packetHelloID:
		if len(packet) == 18 && packet[1] == protocol.Version {
			copy(s.pendingHelloID[:], packet[2:])
			s.havePendingHello = true
		}
	case packetHello:
		if (len(packet) != 17 && len(packet) != 18) || !s.havePendingHello || s.pendingHelloID != s.id {
			return nil
		}
		peerCompression := len(packet) == 18 && packet[17]&capabilityCompression != 0
		if (len(packet) == 18 && packet[17]&^knownCapabilities != 0) || peerCompression != s.config.Compression {
			return ErrCompression
		}
		peerExpected := binary.BigEndian.Uint64(packet[1:9])
		if err := s.reconcilePeerLocked(peerExpected); err != nil {
			return err
		}
		peerCfg := Config{
			ResumeTimeout:  time.Duration(binary.BigEndian.Uint32(packet[9:13])) * time.Millisecond,
			MaxConnections: int(binary.BigEndian.Uint16(packet[13:15])),
			Compression:    peerCompression,
		}
		mtu := min(b.mtu, normalizeMTU(int(binary.BigEndian.Uint16(packet[15:17]))))
		s.negotiateLocked(peerCfg)
		b.mtu = min(b.mtu, mtu)
		s.helloReply = encodeHelloAck(s.rxNext, s.config, mtu)
		s.havePendingHello = false
		s.signalLocked()
	case packetReject:
		return ErrRejected
	default:
		return ErrHandshake
	}
	return nil
}

// acceptDataLocked adds one unique packet to the bounded receive window. It
// reports how many packet fragments made the contiguous receive offset advance,
// whether this packet was retained ahead of a gap, and whether it was rejected.
func (s *Session) acceptDataLocked(seq uint64, payload []byte) (advancedPackets int, buffered, rejected bool, err error) {
	if uint64(len(payload)) > ^uint64(0)-seq {
		return 0, false, false, ErrSequenceExhausted
	}
	end := seq + uint64(len(payload))
	if seq < s.rxNext {
		return 0, false, true, nil
	}
	if end-s.rxNext > uint64(s.config.ReplayWindow) {
		return 0, false, true, nil
	}
	if len(s.rxBuf)+s.rxPendingBytes+len(payload) > s.config.ReplayWindow {
		return 0, false, true, nil
	}
	for pendingSeq, pendingPayload := range s.rxPending {
		pendingEnd := pendingSeq + uint64(len(pendingPayload))
		if seq < pendingEnd && pendingSeq < end {
			return 0, false, true, nil
		}
	}

	if seq > s.rxNext {
		if len(s.rxPending) >= maxFlightWindowPackets {
			return 0, false, true, nil
		}
		if s.rxPending == nil {
			s.rxPending = make(map[uint64][]byte)
		}
		s.rxPending[seq] = append([]byte(nil), payload...)
		s.rxPendingBytes += len(payload)
		s.stats.dataRXBytes += uint64(len(payload))
		s.stats.outOfOrderDataPackets++
		return 0, true, false, nil
	}

	s.rxBuf = append(s.rxBuf, payload...)
	s.rxNext = end
	s.stats.dataRXBytes += uint64(len(payload))
	advancedPackets = 1
	for {
		pending, ok := s.rxPending[s.rxNext]
		if !ok {
			break
		}
		delete(s.rxPending, s.rxNext)
		s.rxPendingBytes -= len(pending)
		s.rxBuf = append(s.rxBuf, pending...)
		s.rxNext += uint64(len(pending))
		advancedPackets++
	}
	if len(s.rxPending) == 0 {
		s.rxPending = nil
	}
	s.signalLocked()
	return advancedPackets, false, false, nil
}

func (s *Session) sendLoop(ctx context.Context, b *binding) {
	ticker := time.NewTicker(sendLoopInterval)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		if s.current != b || s.closed {
			s.mu.Unlock()
			return
		}
		notify := s.notify
		now := time.Now()
		packet := s.nextPacketLocked(b, now)
		s.mu.Unlock()

		if packet == nil {
			select {
			case <-ctx.Done():
				return
			case <-notify:
			case <-ticker.C:
			}
			continue
		}
		sendCtx, cancel := context.WithTimeout(ctx, sendTimeout)
		sendStarted := time.Now()
		err := b.conn.Send(sendCtx, packet)
		sendDuration := time.Since(sendStarted)
		cancel()
		s.mu.Lock()
		s.stats.sendCalls++
		s.stats.sendDuration += sendDuration
		if sendDuration > s.stats.maxSendDuration {
			s.stats.maxSendDuration = sendDuration
		}
		if err == nil {
			switch packet[0] {
			case packetData:
				s.stats.dataTXPackets++
				s.stats.dataTXBytes += uint64(len(packet) - 9)
			case packetAck:
				s.stats.ackTXPackets++
			}
		}
		if s.current == b && err == nil {
			b.lastTX = time.Now()
		}
		s.mu.Unlock()
		if err != nil {
			s.failBinding(b, time.Now())
			return
		}
	}
}

func (s *Session) nextPacketLocked(b *binding, now time.Time) []byte {
	if len(s.helloReply) > 0 {
		packet := append([]byte(nil), s.helloReply...)
		s.helloReply = nil
		return packet
	}
	if s.pongDirty {
		s.pongDirty = false
		return []byte{packetPong}
	}
	if s.ackDirty && (s.ackPackets >= ackBatchPackets || s.ackDeadline.IsZero() || !now.Before(s.ackDeadline)) {
		s.ackDirty = false
		s.ackPackets = 0
		s.ackDeadline = time.Time{}
		packet := make([]byte, 9)
		packet[0] = packetAck
		binary.BigEndian.PutUint64(packet[1:], s.rxNext)
		return packet
	}
	if b.sendNext < s.txBase || b.sendNext > s.txNext {
		b.sendNext = s.txBase
		b.retransmitAt = time.Time{}
		b.duplicateACK = 0
	}
	if b.sendNext > s.txBase && !b.retransmitAt.IsZero() && !now.Before(b.retransmitAt) {
		b.sendNext = s.txBase
		b.retransmitAt = now.Add(retransmitInterval)
		b.duplicateACK = 0
		reduceFlightWindow(b)
		s.stats.retransmissions++
	}
	payloadSize := b.mtu - 9
	if payloadSize < 1 {
		payloadSize = 1
	}
	// Start with one ACK batch, then grow only while cumulative ACKs advance.
	// The bounded adaptive window hides ACK latency without letting a complete
	// 16 KiB live buffer enter CoreBluetooth and starve reverse control traffic.
	flightPackets := b.flightPackets
	if flightPackets < initialFlightWindowPackets {
		flightPackets = initialFlightWindowPackets
		b.flightPackets = flightPackets
	}
	flightWindow := uint64(payloadSize * flightPackets)
	windowEnd := min(s.txNext, s.txBase+flightWindow)
	if b.sendNext < windowEnd {
		payloadSize = min(payloadSize, int(windowEnd-b.sendNext))
		packet := make([]byte, 9+payloadSize)
		packet[0] = packetData
		binary.BigEndian.PutUint64(packet[1:9], b.sendNext)
		start := int(b.sendNext - s.txBase)
		copy(packet[9:], s.txBuf[start:start+payloadSize])
		if b.sendNext == s.txBase && b.retransmitAt.IsZero() {
			b.retransmitAt = now.Add(retransmitInterval)
		}
		b.sendNext += uint64(payloadSize)
		return packet
	}
	return nil
}

// heartbeatLoop actively probes the binding independently of application
// traffic. This is separate from sendLoop so a GATT implementation that stops
// delivering notifications or blocks a data write cannot also prevent
// liveness detection. A successful PONG (or any other valid packet) resets the
// failure count. After two unanswered probes the binding is detached and the
// application reconnect loop can attach a replacement without discarding the
// resumable session.
func (s *Session) heartbeatLoop(ctx context.Context, b *binding) {
	ticker := time.NewTicker(heartbeatCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		now := time.Now()
		s.mu.Lock()
		if s.current != b || s.closed {
			s.mu.Unlock()
			return
		}
		probe, detach := s.heartbeatCheckLocked(b, now)
		s.mu.Unlock()
		if detach {
			s.failBinding(b, now)
			return
		}
		if !probe {
			continue
		}

		sendCtx, cancel := context.WithTimeout(ctx, heartbeatResponseTimeout)
		sendStarted := time.Now()
		err := b.conn.Send(sendCtx, []byte{packetPing})
		sendDuration := time.Since(sendStarted)
		cancel()
		s.mu.Lock()
		s.stats.sendCalls++
		s.stats.sendDuration += sendDuration
		if sendDuration > s.stats.maxSendDuration {
			s.stats.maxSendDuration = sendDuration
		}
		if err == nil {
			s.stats.heartbeatTX++
			if s.current == b {
				b.lastTX = time.Now()
			}
		}
		s.mu.Unlock()
		if err != nil {
			s.failBinding(b, time.Now())
			return
		}
	}
}

// heartbeatCheckLocked decides whether the next probe should be sent. It is
// kept deterministic so stalled-link behavior can be tested without waiting
// through real heartbeat intervals.
func (s *Session) heartbeatCheckLocked(b *binding, now time.Time) (probe, detach bool) {
	probeAfterFailure := false
	if b.heartbeatPending {
		if now.Sub(b.heartbeatSentAt) < heartbeatResponseTimeout {
			return false, false
		}
		b.heartbeatPending = false
		b.heartbeatFailures++
		s.stats.heartbeatFailures++
		if b.heartbeatFailures >= heartbeatFailureLimit {
			return false, true
		}
		probeAfterFailure = true
	}
	if !probeAfterFailure && !b.heartbeatSentAt.IsZero() && now.Sub(b.heartbeatSentAt) < heartbeatInterval {
		return false, false
	}
	b.heartbeatPending = true
	b.heartbeatSentAt = now
	return true, false
}

func (s *Session) failBinding(b *binding, detachedAt time.Time) {
	s.mu.Lock()
	if s.current != b || s.closed {
		s.mu.Unlock()
		return
	}
	s.current = nil
	s.detachedAt = detachedAt
	b.cancel()
	s.signalLocked()
	timeout := s.config.ResumeTimeout
	s.mu.Unlock()
	_ = b.conn.Close()
	go s.expireAfter(detachedAt, timeout)
}

func (s *Session) expireAfter(detachedAt time.Time, timeout time.Duration) {
	delay := time.Until(detachedAt.Add(timeout))
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-s.done:
			return
		}
	}
	s.mu.Lock()
	shouldClose := !s.closed && s.current == nil && s.detachedAt.Equal(detachedAt)
	s.mu.Unlock()
	if shouldClose {
		s.closeWithError(ErrResumeTimeout)
	}
}

func (s *Session) reconcilePeerLocked(expected uint64) error {
	if err := validatePeerExpected(s.txBase, s.txNext, expected); err != nil {
		return err
	}
	delta := int(expected - s.txBase)
	if delta > 0 {
		s.txBuf = s.txBuf[delta:]
		s.txBase = expected
		s.signalLocked()
	}
	return nil
}

func (s *Session) handleAckLocked(b *binding, expected uint64, now time.Time) error {
	if expected < s.txBase {
		return nil
	}
	previousBase := s.txBase
	if err := s.reconcilePeerLocked(expected); err != nil {
		return err
	}
	if s.txBase > previousBase {
		b.duplicateACK = 0
		if b.flightPackets < initialFlightWindowPackets {
			b.flightPackets = initialFlightWindowPackets
		} else if b.flightPackets < maxFlightWindowPackets {
			b.flightPackets++
		}
		if b.sendNext < s.txBase {
			b.sendNext = s.txBase
		}
		if b.sendNext > s.txBase {
			b.retransmitAt = now.Add(retransmitInterval)
		} else {
			b.retransmitAt = time.Time{}
		}
		return nil
	}
	if expected == s.txBase && b.sendNext > s.txBase {
		b.duplicateACK++
		if b.duplicateACK >= fastRetransmitACKs {
			b.sendNext = s.txBase
			b.retransmitAt = now.Add(retransmitInterval)
			b.duplicateACK = 0
			reduceFlightWindow(b)
			s.stats.fastRetransmissions++
			s.signalLocked()
		}
	}
	return nil
}

func reduceFlightWindow(b *binding) {
	flightPackets := b.flightPackets
	if flightPackets < initialFlightWindowPackets {
		flightPackets = initialFlightWindowPackets
	}
	b.flightPackets = max(initialFlightWindowPackets, flightPackets/2)
}

func (s *Session) negotiateLocked(peer Config) {
	if peer.ResumeTimeout > 0 && peer.ResumeTimeout < s.config.ResumeTimeout {
		s.config.ResumeTimeout = peer.ResumeTimeout
	}
	if peer.MaxConnections > 0 && peer.MaxConnections < s.config.MaxConnections {
		s.config.MaxConnections = peer.MaxConnections
	}
	s.config.Compression = s.config.Compression && peer.Compression
}

func (s *Session) IsBound() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current != nil && !s.closed
}

// PacketMTU returns the effective packet size negotiated for the current BLE
// binding. It returns zero while the session is disconnected.
func (s *Session) PacketMTU() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil || s.closed {
		return 0
	}
	return s.current.mtu
}

func (s *Session) WaitBound(ctx context.Context) error {
	for {
		s.mu.Lock()
		if s.closed {
			err := s.sessionErrorLocked()
			s.mu.Unlock()
			return err
		}
		if s.current != nil {
			s.mu.Unlock()
			return nil
		}
		notify := s.notify
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
	}
}

func (s *Session) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		s.mu.Lock()
		if len(s.rxBuf) > 0 {
			n := copy(p, s.rxBuf)
			s.rxBuf = s.rxBuf[n:]
			s.signalLocked()
			s.mu.Unlock()
			return n, nil
		}
		if s.closed {
			err := s.sessionErrorLocked()
			s.mu.Unlock()
			return 0, err
		}
		notify := s.notify
		deadline := s.readDeadline
		s.mu.Unlock()
		if err := waitForSignal(notify, s.done, deadline); err != nil {
			return 0, err
		}
	}
}

func (s *Session) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		s.mu.Lock()
		if s.closed {
			err := s.sessionErrorLocked()
			s.mu.Unlock()
			return written, err
		}
		space := s.config.ReplayWindow - len(s.txBuf)
		if s.current != nil {
			space = min(space, liveWriteWindow-len(s.txBuf))
		}
		if space > 0 {
			n := min(space, len(p))
			if uint64(n) > ^uint64(0)-s.txNext {
				s.mu.Unlock()
				return written, ErrSequenceExhausted
			}
			s.txBuf = append(s.txBuf, p[:n]...)
			s.txNext += uint64(n)
			written += n
			p = p[n:]
			s.signalLocked()
			s.mu.Unlock()
			continue
		}
		notify := s.notify
		deadline := s.writeDeadline
		s.mu.Unlock()
		if err := waitForSignal(notify, s.done, deadline); err != nil {
			return written, err
		}
	}
	return written, nil
}

func (s *Session) Close() error {
	s.mu.Lock()
	b := s.current
	s.mu.Unlock()
	if b != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = b.conn.Send(ctx, []byte{packetClose})
		cancel()
	}
	s.closeWithError(io.EOF)
	return nil
}

func (s *Session) closeWithError(err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.err = err
	b := s.current
	s.current = nil
	close(s.done)
	s.signalLocked()
	s.mu.Unlock()
	if b != nil {
		b.cancel()
		_ = b.conn.Close()
	}
}

func (s *Session) Done() <-chan struct{} { return s.done }

func (s *Session) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *Session) LocalAddr() net.Addr  { return sessionAddr("local") }
func (s *Session) RemoteAddr() net.Addr { return sessionAddr("remote") }

func (s *Session) SetDeadline(deadline time.Time) error {
	s.mu.Lock()
	s.readDeadline = deadline
	s.writeDeadline = deadline
	s.signalLocked()
	s.mu.Unlock()
	return nil
}

func (s *Session) SetReadDeadline(deadline time.Time) error {
	s.mu.Lock()
	s.readDeadline = deadline
	s.signalLocked()
	s.mu.Unlock()
	return nil
}

func (s *Session) SetWriteDeadline(deadline time.Time) error {
	s.mu.Lock()
	s.writeDeadline = deadline
	s.signalLocked()
	s.mu.Unlock()
	return nil
}

func (s *Session) signalLocked() {
	close(s.notify)
	s.notify = make(chan struct{})
}

func (s *Session) sessionError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessionErrorLocked()
}

func (s *Session) sessionErrorLocked() error {
	if s.err == nil || errors.Is(s.err, io.EOF) {
		return io.EOF
	}
	return s.err
}

func waitForSignal(notify, done <-chan struct{}, deadline time.Time) error {
	if deadline.IsZero() {
		select {
		case <-notify:
			return nil
		case <-done:
			return nil
		}
	}
	delay := time.Until(deadline)
	if delay <= 0 {
		return timeoutError{}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-notify:
		return nil
	case <-done:
		return nil
	case <-timer.C:
		return timeoutError{}
	}
}

type sessionAddr string

func (a sessionAddr) Network() string { return "lightningbnb" }
func (a sessionAddr) String() string  { return string(a) }

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }
