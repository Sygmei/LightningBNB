package mux

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/Sygmei/LightningBNB/internal/protocol"
)

type Stream struct {
	session *Session
	id      uint32

	mu                sync.Mutex
	opened            bool
	openErr           error
	closed            bool
	err               error
	localWriteClosed  bool
	remoteWriteClosed bool
	closeAfterDrain   bool

	sendWindow    uint32
	receiveWindow uint32
	inbound       []byte
	outbound      []protocol.Frame
	scheduled     bool

	readDeadline  time.Time
	writeDeadline time.Time
	notify        chan struct{}
}

func newStream(session *Session, id uint32) *Stream {
	return &Stream{
		session:       session,
		id:            id,
		sendWindow:    InitialStreamWindow,
		receiveWindow: InitialStreamWindow,
		notify:        make(chan struct{}),
	}
}

func (s *Stream) ID() uint32 { return s.id }

func (s *Stream) Approve() error {
	s.mu.Lock()
	if s.closed {
		err := s.streamErrorLocked()
		s.mu.Unlock()
		return err
	}
	if s.opened {
		s.mu.Unlock()
		return nil
	}
	s.opened = true
	s.signalLocked()
	s.mu.Unlock()
	return s.session.sendControl(protocol.Frame{Type: protocol.FrameOpenOK, StreamID: s.id})
}

func (s *Stream) Reject(err error) error {
	message := errorMessage(err)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.err = ErrStreamRejected
	s.signalLocked()
	s.mu.Unlock()
	s.session.removeStream(s.id)
	return s.session.sendControl(protocol.Frame{Type: protocol.FrameOpenError, StreamID: s.id, Payload: []byte(message)})
}

func (s *Stream) waitOpened(ctx context.Context) error {
	for {
		s.mu.Lock()
		if s.opened {
			s.mu.Unlock()
			return nil
		}
		if s.openErr != nil || s.closed {
			err := s.streamErrorLocked()
			s.mu.Unlock()
			return err
		}
		notify := s.notify
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.session.done:
			return s.session.Err()
		case <-notify:
		}
	}
}

func (s *Stream) markOpened(err error) {
	s.mu.Lock()
	if !s.closed {
		s.openErr = err
		s.opened = err == nil
		if err != nil {
			s.closed = true
			s.err = err
		}
		s.signalLocked()
	}
	s.mu.Unlock()
}

func (s *Stream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		s.mu.Lock()
		if len(s.inbound) > 0 {
			n := copy(p, s.inbound)
			s.inbound = s.inbound[n:]
			s.receiveWindow += uint32(n)
			s.mu.Unlock()
			_ = s.session.sendControl(protocol.WindowUpdate(s.id, uint32(n)))
			return n, nil
		}
		if s.remoteWriteClosed {
			s.mu.Unlock()
			return 0, io.EOF
		}
		if s.closed {
			err := s.streamErrorLocked()
			s.mu.Unlock()
			return 0, err
		}
		notify := s.notify
		deadline := s.readDeadline
		s.mu.Unlock()
		if err := waitStream(notify, s.session.done, deadline); err != nil {
			return 0, err
		}
	}
}

func (s *Stream) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		s.mu.Lock()
		if !s.opened {
			err := s.streamErrorLocked()
			s.mu.Unlock()
			if err == nil {
				err = ErrStreamRejected
			}
			return written, err
		}
		if s.closed || s.localWriteClosed {
			err := s.streamErrorLocked()
			if err == nil {
				err = io.ErrClosedPipe
			}
			s.mu.Unlock()
			return written, err
		}
		if s.sendWindow > 0 {
			n := min(len(p), protocol.MaxDataPayload, int(s.sendWindow))
			payload := append([]byte(nil), p[:n]...)
			s.sendWindow -= uint32(n)
			needsSchedule := s.enqueueLocked(protocol.Frame{Type: protocol.FrameData, StreamID: s.id, Payload: payload})
			s.mu.Unlock()
			if needsSchedule {
				if err := s.session.schedule(s); err != nil {
					return written, err
				}
			}
			written += n
			p = p[n:]
			continue
		}
		notify := s.notify
		deadline := s.writeDeadline
		s.mu.Unlock()
		if err := waitStream(notify, s.session.done, deadline); err != nil {
			return written, err
		}
	}
	return written, nil
}

func (s *Stream) receiveData(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.remoteWriteClosed || !s.opened {
		return ErrProtocol
	}
	if uint32(len(data)) > s.receiveWindow {
		return errors.New("peer exceeded stream receive window")
	}
	s.receiveWindow -= uint32(len(data))
	s.inbound = append(s.inbound, data...)
	s.signalLocked()
	return nil
}

func (s *Stream) updateSendWindow(amount uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if amount == 0 || amount > InitialStreamWindow-s.sendWindow {
		return errors.New("invalid stream window update")
	}
	s.sendWindow += amount
	s.signalLocked()
	return nil
}

func (s *Stream) receiveFIN() {
	s.mu.Lock()
	if !s.closed {
		s.remoteWriteClosed = true
		s.signalLocked()
	}
	s.mu.Unlock()
}

func (s *Stream) receiveReset(err error) {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		if err == nil {
			err = io.ErrClosedPipe
		}
		s.err = err
		s.signalLocked()
	}
	s.mu.Unlock()
}

func (s *Stream) CloseWrite() error {
	s.mu.Lock()
	if s.localWriteClosed || s.closed {
		s.mu.Unlock()
		return nil
	}
	s.localWriteClosed = true
	needsSchedule := s.enqueueLocked(protocol.Frame{Type: protocol.FrameFIN, StreamID: s.id})
	s.signalLocked()
	s.mu.Unlock()
	if needsSchedule {
		return s.session.schedule(s)
	}
	return nil
}

func (s *Stream) CloseRead() error {
	s.mu.Lock()
	s.remoteWriteClosed = true
	s.inbound = nil
	s.signalLocked()
	s.mu.Unlock()
	return nil
}

func (s *Stream) Close() error {
	s.mu.Lock()
	graceful := s.localWriteClosed && s.remoteWriteClosed
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.closeAfterDrain = graceful
	drained := len(s.outbound) == 0
	s.signalLocked()
	s.mu.Unlock()
	if graceful {
		if drained {
			s.session.removeStream(s.id)
		}
		return nil
	}
	s.session.removeStream(s.id)
	return s.session.sendControl(protocol.Frame{Type: protocol.FrameReset, StreamID: s.id, Payload: []byte("stream closed")})
}

func (s *Stream) Reset(err error) error {
	message := errorMessage(err)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.err = err
	s.outbound = nil
	s.scheduled = false
	s.signalLocked()
	s.mu.Unlock()
	s.session.removeStream(s.id)
	return s.session.sendControl(protocol.Frame{Type: protocol.FrameReset, StreamID: s.id, Payload: []byte(message)})
}

func (s *Stream) enqueueLocked(frame protocol.Frame) bool {
	s.outbound = append(s.outbound, frame)
	if s.scheduled {
		return false
	}
	s.scheduled = true
	return true
}

func (s *Stream) nextOutbound() (protocol.Frame, bool) {
	s.mu.Lock()
	if len(s.outbound) == 0 {
		s.scheduled = false
		s.mu.Unlock()
		return protocol.Frame{}, false
	}
	frame := s.outbound[0]
	s.outbound = s.outbound[1:]
	requeue := len(s.outbound) > 0
	if !requeue {
		s.scheduled = false
	}
	removeAfterDrain := !requeue && s.closeAfterDrain
	s.mu.Unlock()
	if requeue {
		_ = s.session.schedule(s)
	} else if removeAfterDrain {
		s.session.removeStream(s.id)
	}
	return frame, true
}

func (s *Stream) LocalAddr() net.Addr  { return s.session.conn.LocalAddr() }
func (s *Stream) RemoteAddr() net.Addr { return s.session.conn.RemoteAddr() }

func (s *Stream) SetDeadline(deadline time.Time) error {
	s.mu.Lock()
	s.readDeadline = deadline
	s.writeDeadline = deadline
	s.signalLocked()
	s.mu.Unlock()
	return nil
}

func (s *Stream) SetReadDeadline(deadline time.Time) error {
	s.mu.Lock()
	s.readDeadline = deadline
	s.signalLocked()
	s.mu.Unlock()
	return nil
}

func (s *Stream) SetWriteDeadline(deadline time.Time) error {
	s.mu.Lock()
	s.writeDeadline = deadline
	s.signalLocked()
	s.mu.Unlock()
	return nil
}

func (s *Stream) streamErrorLocked() error {
	if s.err != nil {
		return s.err
	}
	if s.openErr != nil {
		return s.openErr
	}
	if s.closed {
		return io.ErrClosedPipe
	}
	return nil
}

func (s *Stream) signalLocked() {
	close(s.notify)
	s.notify = make(chan struct{})
}

func errorMessage(err error) string {
	if err == nil {
		return "stream reset"
	}
	message := err.Error()
	if len(message) > protocol.MaxErrorPayload {
		message = message[:protocol.MaxErrorPayload]
	}
	return message
}

func waitStream(notify, done <-chan struct{}, deadline time.Time) error {
	if deadline.IsZero() {
		select {
		case <-notify:
			return nil
		case <-done:
			return ErrSessionClosed
		}
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return streamTimeout{}
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-notify:
		return nil
	case <-done:
		return ErrSessionClosed
	case <-timer.C:
		return streamTimeout{}
	}
}

type streamTimeout struct{}

func (streamTimeout) Error() string   { return "i/o timeout" }
func (streamTimeout) Timeout() bool   { return true }
func (streamTimeout) Temporary() bool { return true }
