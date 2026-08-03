package mux

import (
	"bytes"
	"compress/flate"
	"errors"
	"fmt"
	"io"

	"github.com/Sygmei/LightningBNB/internal/protocol"
)

const (
	compressionRaw byte = iota
	compressionDeflate

	compressionMinimumSize = 64
	windowUpdateThreshold  = InitialStreamWindow / 8
)

func encodeCompressedPayload(data []byte) ([]byte, error) {
	if len(data) == 0 || len(data) > protocol.MaxDataPayload-1 {
		return nil, errors.New("invalid uncompressed DATA size")
	}
	if len(data) < compressionMinimumSize {
		return append([]byte{compressionRaw}, data...), nil
	}

	var compressed bytes.Buffer
	compressed.WriteByte(compressionDeflate)
	writer, err := flate.NewWriter(&compressed, flate.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err := writer.Write(data); err != nil {
		_ = writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	if compressed.Len() >= len(data)+1 {
		return append([]byte{compressionRaw}, data...), nil
	}
	return compressed.Bytes(), nil
}

func decodeCompressedPayload(data []byte) ([]byte, error) {
	if len(data) < 2 {
		return nil, errors.New("compressed DATA has no payload")
	}
	switch data[0] {
	case compressionRaw:
		return append([]byte(nil), data[1:]...), nil
	case compressionDeflate:
		reader := flate.NewReader(bytes.NewReader(data[1:]))
		decoded, err := io.ReadAll(io.LimitReader(reader, protocol.MaxDataPayload))
		closeErr := reader.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if len(decoded) == 0 || len(decoded) > protocol.MaxDataPayload-1 {
			return nil, fmt.Errorf("invalid decompressed DATA size %d", len(decoded))
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("unknown compression encoding %d", data[0])
	}
}
