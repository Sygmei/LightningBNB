package mux

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/Sygmei/LightningBNB/internal/protocol"
	"github.com/Sygmei/LightningBNB/internal/traffic"
)

const (
	InitialStreamWindow = 64 * 1024
	acceptBacklogFactor = 2
	maxServiceName      = 64
)

var (
	ErrSessionClosed  = errors.New("multiplexing session closed")
	ErrTooManyStreams = errors.New("maximum concurrent stream count reached")
	ErrStreamRejected = errors.New("stream rejected")
	ErrProtocol       = errors.New("multiplexing protocol error")
)

type Session struct {
	conn        net.Conn
	client      bool
	maxStreams  int
	compression bool
	traffic     *traffic.Counter
	services    []Service

	mu      sync.Mutex
	streams map[uint32]*Stream
	nextID  uint32
	closed  bool
	err     error

	control       chan protocol.Frame
	ready         chan uint32
	accept        chan *Stream
	done          chan struct{}
	once          sync.Once
	servicesReady chan struct{}
	servicesErr   error
}

// Service is a TCP target advertised by the server. Name is the selector
// used by clients; a numeric port selector is also accepted for every service.
type Service struct {
	Name string
	Port int
}

// Snapshot reports multiplexed stream pressure for diagnostics and the live
// console. PendingAccepts are OPEN frames waiting for the server bridge to
// accept them; Streams includes active and opening streams.
type Snapshot struct {
	Streams        int
	PendingAccepts int
	OpeningStreams int
}

func NewClient(conn net.Conn, maxStreams int) *Session {
	return newSession(conn, true, maxStreams, false, nil)
}

func NewServer(conn net.Conn, maxStreams int) *Session {
	return newSession(conn, false, maxStreams, false, nil)
}

func NewClientWithCompression(conn net.Conn, maxStreams int, compression bool) *Session {
	return newSession(conn, true, maxStreams, compression, nil)
}

func NewServerWithCompression(conn net.Conn, maxStreams int, compression bool) *Session {
	return newSession(conn, false, maxStreams, compression, nil)
}

func NewClientWithCompressionAndTraffic(conn net.Conn, maxStreams int, compression bool, counter *traffic.Counter) *Session {
	return newSession(conn, true, maxStreams, compression, counter)
}

func NewServerWithCompressionAndTraffic(conn net.Conn, maxStreams int, compression bool, counter *traffic.Counter) *Session {
	return newSession(conn, false, maxStreams, compression, counter)
}

func NewServerWithServicesAndCompressionAndTraffic(conn net.Conn, maxStreams int, compression bool, counter *traffic.Counter, services []Service) *Session {
	s := newSession(conn, false, maxStreams, compression, counter)
	s.services = append([]Service(nil), services...)
	// The control queue is buffered and the writer goroutine is already running,
	// so the list is sent before any later OPEN frames.
	s.control <- protocol.Frame{Type: protocol.FrameServiceList, Payload: encodeServices(s.services)}
	return s
}

func newSession(conn net.Conn, client bool, maxStreams int, compression bool, counter *traffic.Counter) *Session {
	if maxStreams <= 0 {
		maxStreams = 1
	}
	if compression && counter != nil {
		counter.EnableCompression()
	}
	nextID := uint32(2)
	if client {
		nextID = 1
	}
	s := &Session{
		conn:          conn,
		client:        client,
		maxStreams:    maxStreams,
		compression:   compression,
		traffic:       counter,
		streams:       make(map[uint32]*Stream),
		nextID:        nextID,
		control:       make(chan protocol.Frame, maxStreams*8),
		ready:         make(chan uint32, maxStreams),
		accept:        make(chan *Stream, maxStreams*acceptBacklogFactor),
		done:          make(chan struct{}),
		servicesReady: make(chan struct{}),
	}
	go s.readLoop()
	go s.writeLoop()
	return s
}

func (s *Session) Open(ctx context.Context) (*Stream, error) {
	return s.OpenService(ctx, "")
}

// OpenService opens a stream targeting the named server service. An empty
// selector is the legacy single-target form.
func (s *Session) OpenService(ctx context.Context, service string) (*Stream, error) {
	if !s.client {
		return nil, errors.New("server sessions cannot open target streams")
	}
	if len(service) > maxServiceName {
		return nil, errors.New("service selector is too long")
	}
	s.mu.Lock()
	if s.closed {
		err := s.sessionErrorLocked()
		s.mu.Unlock()
		return nil, err
	}
	if len(s.streams) >= s.maxStreams {
		s.mu.Unlock()
		return nil, ErrTooManyStreams
	}
	id := s.nextID
	s.nextID += 2
	if id == 0 {
		s.mu.Unlock()
		return nil, errors.New("stream id space exhausted")
	}
	stream := newStream(s, id)
	s.streams[id] = stream
	s.mu.Unlock()
	if err := s.sendControl(protocol.Frame{Type: protocol.FrameOpen, StreamID: id, Payload: []byte(service)}); err != nil {
		s.removeStream(id)
		return nil, err
	}
	if err := stream.waitOpened(ctx); err != nil {
		stream.Reset(err)
		return nil, err
	}
	return stream, nil
}

// Services waits for the server's advertised service list.
func (s *Session) Services(ctx context.Context) ([]Service, error) {
	select {
	case <-s.servicesReady:
		s.mu.Lock()
		services := append([]Service(nil), s.services...)
		err := s.servicesErr
		s.mu.Unlock()
		return services, err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.done:
		return nil, s.Err()
	}
}

func (s *Session) Accept(ctx context.Context) (*Stream, error) {
	if s.client {
		return nil, errors.New("client sessions cannot accept target streams")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.done:
		return nil, s.Err()
	case stream := <-s.accept:
		return stream, nil
	}
}

func (s *Session) Done() <-chan struct{} { return s.done }

func (s *Session) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessionErrorLocked()
}

func (s *Session) NumStreams() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.streams)
}

func (s *Session) Snapshot() Snapshot {
	s.mu.Lock()
	streams := make([]*Stream, 0, len(s.streams))
	for _, stream := range s.streams {
		streams = append(streams, stream)
	}
	pending := len(s.accept)
	s.mu.Unlock()
	opening := 0
	for _, stream := range streams {
		stream.mu.Lock()
		if !stream.opened && stream.openErr == nil && !stream.closed {
			opening++
		}
		stream.mu.Unlock()
	}
	return Snapshot{Streams: len(streams), PendingAccepts: pending, OpeningStreams: opening}
}

func (s *Session) Close() error {
	s.shutdown(io.EOF)
	return nil
}

func (s *Session) readLoop() {
	for {
		frame, err := protocol.ReadFrame(s.conn)
		if err != nil {
			s.shutdown(err)
			return
		}
		if err := s.handleFrame(frame); err != nil {
			s.shutdown(err)
			return
		}
	}
}

func (s *Session) handleFrame(frame protocol.Frame) error {
	if frame.Type == protocol.FrameServiceList {
		if !s.client || frame.StreamID != 0 {
			return fmt.Errorf("%w: unexpected service list", ErrProtocol)
		}
		services, err := decodeServices(frame.Payload)
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.services = services
		s.servicesErr = nil
		select {
		case <-s.servicesReady:
		default:
			close(s.servicesReady)
		}
		s.mu.Unlock()
		return nil
	}
	if frame.Type == protocol.FrameOpen {
		return s.handleOpen(frame)
	}
	s.mu.Lock()
	stream := s.streams[frame.StreamID]
	s.mu.Unlock()
	if stream == nil {
		if frame.Type == protocol.FrameReset || frame.Type == protocol.FrameOpenError {
			return nil
		}
		_ = s.sendControl(protocol.Frame{Type: protocol.FrameReset, StreamID: frame.StreamID, Payload: []byte("unknown stream")})
		return nil
	}
	switch frame.Type {
	case protocol.FrameOpenOK:
		if !s.client {
			return fmt.Errorf("%w: server received OPEN_OK", ErrProtocol)
		}
		stream.markOpened(nil)
	case protocol.FrameOpenError:
		stream.markOpened(fmt.Errorf("%w: %s", ErrStreamRejected, frame.Payload))
		s.removeStream(frame.StreamID)
	case protocol.FrameData:
		return stream.receiveData(frame.Payload)
	case protocol.FrameWindowUpdate:
		return stream.updateSendWindow(protocol.WindowAmount(frame))
	case protocol.FrameFIN:
		stream.receiveFIN()
	case protocol.FrameReset:
		stream.receiveReset(errors.New(string(frame.Payload)))
		s.removeStream(frame.StreamID)
	default:
		return fmt.Errorf("%w: unexpected frame type %d", ErrProtocol, frame.Type)
	}
	return nil
}

func (s *Session) handleOpen(frame protocol.Frame) error {
	if s.client || frame.StreamID%2 == 0 {
		return fmt.Errorf("%w: invalid OPEN stream id %d", ErrProtocol, frame.StreamID)
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrSessionClosed
	}
	if _, exists := s.streams[frame.StreamID]; exists {
		s.mu.Unlock()
		return fmt.Errorf("%w: duplicate stream id %d", ErrProtocol, frame.StreamID)
	}
	if len(s.streams) >= s.maxStreams {
		s.mu.Unlock()
		return s.sendControl(protocol.Frame{Type: protocol.FrameOpenError, StreamID: frame.StreamID, Payload: []byte("server stream limit reached")})
	}
	stream := newStream(s, frame.StreamID)
	stream.service = string(frame.Payload)
	s.streams[frame.StreamID] = stream
	s.mu.Unlock()
	select {
	case <-s.done:
		return ErrSessionClosed
	case s.accept <- stream:
		return nil
	default:
		s.removeStream(frame.StreamID)
		return s.sendControl(protocol.Frame{Type: protocol.FrameOpenError, StreamID: frame.StreamID, Payload: []byte("server accept backlog full")})
	}
}

func encodeServices(services []Service) []byte {
	length := 2
	for _, service := range services {
		length += 1 + len(service.Name) + 2
	}
	payload := make([]byte, length)
	binary.BigEndian.PutUint16(payload[:2], uint16(len(services)))
	offset := 2
	for _, service := range services {
		payload[offset] = byte(len(service.Name))
		offset++
		copy(payload[offset:], service.Name)
		offset += len(service.Name)
		binary.BigEndian.PutUint16(payload[offset:offset+2], uint16(service.Port))
		offset += 2
	}
	return payload
}

func decodeServices(payload []byte) ([]Service, error) {
	if len(payload) < 2 {
		return nil, fmt.Errorf("%w: truncated service list", ErrProtocol)
	}
	count := int(binary.BigEndian.Uint16(payload[:2]))
	services := make([]Service, 0, count)
	offset := 2
	for range count {
		if offset >= len(payload) {
			return nil, fmt.Errorf("%w: truncated service name", ErrProtocol)
		}
		nameLength := int(payload[offset])
		offset++
		if nameLength > maxServiceName || offset+nameLength+2 > len(payload) {
			return nil, fmt.Errorf("%w: invalid service entry", ErrProtocol)
		}
		name := string(payload[offset : offset+nameLength])
		offset += nameLength
		port := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2
		if port < 1 || port > 65535 {
			return nil, fmt.Errorf("%w: invalid service port", ErrProtocol)
		}
		services = append(services, Service{Name: name, Port: port})
	}
	if offset != len(payload) {
		return nil, fmt.Errorf("%w: trailing service list data", ErrProtocol)
	}
	return services, nil
}

func (s *Session) writeLoop() {
	for {
		var frame protocol.Frame
		select {
		case <-s.done:
			return
		case frame = <-s.control:
		default:
			select {
			case <-s.done:
				return
			case frame = <-s.control:
			case streamID := <-s.ready:
				stream := s.stream(streamID)
				if stream == nil {
					continue
				}
				var ok bool
				frame, ok = stream.nextOutbound()
				if !ok {
					continue
				}
			}
		}
		if err := protocol.WriteFrame(s.conn, frame); err != nil {
			s.shutdown(err)
			return
		}
	}
}

func (s *Session) stream(id uint32) *Stream {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streams[id]
}

func (s *Session) sendControl(frame protocol.Frame) error {
	select {
	case <-s.done:
		return s.Err()
	case s.control <- frame:
		return nil
	default:
		// A full control queue means the mux writer is no longer making
		// progress. Blocking here would strand OPEN/RESET callers forever;
		// tear down the mux so the owning BLE session can be rebound.
		err := errors.New("multiplexing control queue full")
		s.shutdown(err)
		return err
	}
}

func (s *Session) schedule(stream *Stream) error {
	select {
	case <-s.done:
		return s.Err()
	case s.ready <- stream.id:
		return nil
	default:
		err := errors.New("multiplexing scheduler queue full")
		s.shutdown(err)
		return err
	}
}

func (s *Session) removeStream(id uint32) {
	s.mu.Lock()
	delete(s.streams, id)
	s.mu.Unlock()
}

func (s *Session) shutdown(err error) {
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		if err == nil || errors.Is(err, io.EOF) {
			s.err = ErrSessionClosed
		} else {
			s.err = err
		}
		streams := make([]*Stream, 0, len(s.streams))
		for _, stream := range s.streams {
			streams = append(streams, stream)
		}
		s.streams = make(map[uint32]*Stream)
		close(s.done)
		s.mu.Unlock()
		_ = s.conn.Close()
		for _, stream := range streams {
			stream.receiveReset(s.err)
		}
	})
}

func (s *Session) sessionErrorLocked() error {
	if s.err != nil {
		return s.err
	}
	return ErrSessionClosed
}
