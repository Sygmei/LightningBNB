package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

type countingWriter struct {
	bytes.Buffer
	writes int
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.writes++
	return w.Buffer.Write(p)
}

func TestFrameRoundTrip(t *testing.T) {
	t.Parallel()
	want := Frame{Type: FrameData, StreamID: 7, Payload: []byte("hello")}
	var encoded bytes.Buffer
	if err := WriteFrame(&encoded, want); err != nil {
		t.Fatal(err)
	}
	got, err := ReadFrame(&encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != want.Type || got.StreamID != want.StreamID || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestWriteFrameUsesOneWriteForHeaderAndPayload(t *testing.T) {
	var writer countingWriter
	frame := Frame{Type: FrameData, StreamID: 7, Payload: []byte("hello")}

	if err := WriteFrame(&writer, frame); err != nil {
		t.Fatal(err)
	}
	if writer.writes != 1 {
		t.Fatalf("WriteFrame used %d writes, want 1", writer.writes)
	}
	if got, err := ReadFrame(bytes.NewReader(writer.Bytes())); err != nil || !bytes.Equal(got.Payload, frame.Payload) {
		t.Fatalf("round trip = frame=%+v err=%v, want payload %q", got, err, frame.Payload)
	}
}

func TestFrameRejectsMalformedInput(t *testing.T) {
	t.Parallel()
	tests := []Frame{
		{Type: FrameOpen, StreamID: 0},
		{Type: FrameOpen, StreamID: 1, Payload: make([]byte, 129)},
		{Type: FrameData, StreamID: 1},
		{Type: FrameWindowUpdate, StreamID: 1, Payload: make([]byte, 4)},
		{Type: 99, StreamID: 1},
	}
	for _, test := range tests {
		if err := Validate(test); !errors.Is(err, ErrInvalidFrame) {
			t.Fatalf("Validate(%#v) = %v", test, err)
		}
	}
}

func TestReadFrameRejectsOversizedLengthBeforeAllocating(t *testing.T) {
	t.Parallel()
	header := make([]byte, HeaderSize)
	header[0] = byte(FrameData)
	binary.BigEndian.PutUint32(header[1:5], 1)
	binary.BigEndian.PutUint32(header[5:9], MaxDataPayload+1)
	_, err := ReadFrame(bytes.NewReader(header))
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("ReadFrame error = %v", err)
	}
}
