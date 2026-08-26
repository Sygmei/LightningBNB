package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Version is the on-wire LightningBNB protocol version.
const Version uint8 = 2

const (
	HeaderSize      = 9
	MaxDataPayload  = 16 * 1024
	MaxErrorPayload = 256
)

type FrameType uint8

const (
	FrameOpen FrameType = iota + 1
	FrameOpenOK
	FrameOpenError
	FrameData
	FrameWindowUpdate
	FrameFIN
	FrameReset
	FrameServiceList
)

var (
	ErrInvalidFrame  = errors.New("invalid multiplexing frame")
	ErrFrameTooLarge = errors.New("multiplexing frame is too large")
)

type Frame struct {
	Type     FrameType
	StreamID uint32
	Payload  []byte
}

func WriteFrame(w io.Writer, frame Frame) error {
	if err := Validate(frame); err != nil {
		return err
	}
	header := make([]byte, HeaderSize)
	header[0] = byte(frame.Type)
	binary.BigEndian.PutUint32(header[1:5], frame.StreamID)
	binary.BigEndian.PutUint32(header[5:9], uint32(len(frame.Payload)))
	// Submit the complete mux frame in one writer call. In particular, the
	// reliable BLE session may otherwise transmit the header as a separate
	// packet before the payload arrives in its buffer, wasting a packet and
	// adding avoidable scheduling latency.
	encoded := make([]byte, HeaderSize+len(frame.Payload))
	copy(encoded, header)
	copy(encoded[HeaderSize:], frame.Payload)
	return writeAll(w, encoded)
}

func ReadFrame(r io.Reader) (Frame, error) {
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return Frame{}, err
	}
	length := binary.BigEndian.Uint32(header[5:9])
	if length > MaxDataPayload {
		return Frame{}, fmt.Errorf("%w: payload length %d", ErrFrameTooLarge, length)
	}
	frame := Frame{
		Type:     FrameType(header[0]),
		StreamID: binary.BigEndian.Uint32(header[1:5]),
		Payload:  make([]byte, int(length)),
	}
	if _, err := io.ReadFull(r, frame.Payload); err != nil {
		return Frame{}, err
	}
	if err := Validate(frame); err != nil {
		return Frame{}, err
	}
	return frame, nil
}

func Validate(frame Frame) error {
	if frame.StreamID == 0 && frame.Type != FrameServiceList {
		return fmt.Errorf("%w: stream id is zero", ErrInvalidFrame)
	}
	switch frame.Type {
	case FrameOpenOK, FrameFIN:
		if len(frame.Payload) != 0 {
			return fmt.Errorf("%w: frame type %d must not contain a payload", ErrInvalidFrame, frame.Type)
		}
	case FrameOpen:
		if len(frame.Payload) > 128 {
			return fmt.Errorf("%w: service selector too long", ErrInvalidFrame)
		}
	case FrameOpenError, FrameReset:
		if len(frame.Payload) > MaxErrorPayload {
			return fmt.Errorf("%w: error payload length %d", ErrFrameTooLarge, len(frame.Payload))
		}
	case FrameServiceList:
		if len(frame.Payload) == 0 {
			return fmt.Errorf("%w: empty service list", ErrInvalidFrame)
		}
	case FrameData:
		if len(frame.Payload) == 0 || len(frame.Payload) > MaxDataPayload {
			return fmt.Errorf("%w: invalid data payload length %d", ErrInvalidFrame, len(frame.Payload))
		}
	case FrameWindowUpdate:
		if len(frame.Payload) != 4 || binary.BigEndian.Uint32(frame.Payload) == 0 {
			return fmt.Errorf("%w: invalid window update", ErrInvalidFrame)
		}
	default:
		return fmt.Errorf("%w: unknown type %d", ErrInvalidFrame, frame.Type)
	}
	return nil
}

func WindowUpdate(streamID uint32, amount uint32) Frame {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, amount)
	return Frame{Type: FrameWindowUpdate, StreamID: streamID, Payload: payload}
}

func WindowAmount(frame Frame) uint32 {
	if frame.Type != FrameWindowUpdate || len(frame.Payload) != 4 {
		return 0
	}
	return binary.BigEndian.Uint32(frame.Payload)
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
