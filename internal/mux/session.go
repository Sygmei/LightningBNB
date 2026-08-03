package mux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/Sygmei/LightningBNB/internal/protocol"
)

const (
	InitialStreamWindow = 64 * 1024
	acceptBacklogFactor = 2
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

	mu      sync.Mutex
	streams map[uint32]*Stream
	nextID  uint32
	closed  bool
	err     error

	control chan protocol.Frame
	ready   chan uint32
	accept  chan *Stream
	done    chan struct{}
	once    sync.Once
}

func NewClient(conn net.Conn, maxStreams int) *Session {
	return newSession(conn, true, maxStreams, false)
}

func NewServer(conn net.Conn, maxStreams int) *Session {
	return newSession(conn, false, maxStreams, false)
}

func NewClientWithCompression(conn net.Conn, maxStreams int, compression bool) *Session {
	return newSession(conn, true, maxStreams, compression)
}

func NewServerWithCompression(conn net.Conn, maxStreams int, compression bool) *Session {
	return newSession(conn, false, maxStreams, compression)
}

func newSession(conn net.Conn, client bool, maxStreams int, compression bool) *Session {
	if maxStreams <= 0 {
		maxStreams = 1
	}
	nextID := uint32(2)
	if client {
		nextID = 1
	}
	s := &Session{
		conn:        conn,
		client:      client,
		maxStreams:  maxStreams,
		compression: compression,
		streams:     make(map[uint32]*Stream),
		nextID:      nextID,
		control:     make(chan protocol.Frame, maxStreams*8),
		ready:       make(chan uint32, maxStreams),
		accept:      make(chan *Stream, maxStreams*acceptBacklogFactor),
		done:        make(chan struct{}),
	}
	go s.readLoop()
	go s.writeLoop()
	return s
}

func (s *Session) Open(ctx context.Context) (*Stream, error) {
	if !s.client {
		return nil, errors.New("server sessions cannot open target streams")
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
	if err := s.sendControl(protocol.Frame{Type: protocol.FrameOpen, StreamID: id}); err != nil {
		s.removeStream(id)
		return nil, err
	}
	if err := stream.waitOpened(ctx); err != nil {
		stream.Reset(err)
		return nil, err
	}
	return stream, nil
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
	}
}

func (s *Session) schedule(stream *Stream) error {
	select {
	case <-s.done:
		return s.Err()
	case s.ready <- stream.id:
		return nil
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
