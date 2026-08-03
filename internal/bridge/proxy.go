package bridge

import (
	"errors"
	"io"
	"net"

	"github.com/Sygmei/LightningBNB/internal/mux"
	"github.com/Sygmei/LightningBNB/internal/traffic"
)

type closeWriter interface {
	CloseWrite() error
}

// Proxy copies a TCP connection and a multiplexed stream in both directions,
// preserving TCP half-closes. It returns the first non-clean copy error.
func Proxy(tcp net.Conn, stream *mux.Stream) error {
	return ProxyWithTraffic(tcp, stream, nil)
}

// ProxyWithTraffic copies a TCP connection and a multiplexed stream in both
// directions while recording successfully forwarded TCP payload bytes. TX is
// tcp-to-stream traffic and RX is stream-to-tcp traffic.
func ProxyWithTraffic(tcp net.Conn, stream *mux.Stream, counter *traffic.Counter) error {
	errorsCh := make(chan error, 2)
	go func() {
		_, err := io.Copy(countingWriter{writer: stream, add: counterTX(counter)}, tcp)
		if closeWriteErr := stream.CloseWrite(); err == nil {
			err = closeWriteErr
		}
		errorsCh <- cleanCopyError(err)
	}()
	go func() {
		_, err := io.Copy(countingWriter{writer: tcp, add: counterRX(counter)}, stream)
		if writer, ok := tcp.(closeWriter); ok {
			if closeWriteErr := writer.CloseWrite(); err == nil {
				err = closeWriteErr
			}
		}
		errorsCh <- cleanCopyError(err)
	}()

	var firstErr error
	for range 2 {
		if err := <-errorsCh; err != nil && firstErr == nil {
			firstErr = err
			_ = stream.Reset(err)
			_ = tcp.Close()
		}
	}
	_ = stream.Close()
	_ = tcp.Close()
	return firstErr
}

type countingWriter struct {
	writer io.Writer
	add    func(uint64)
}

func (w countingWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	if n > 0 && w.add != nil {
		w.add(uint64(n))
	}
	return n, err
}

func counterTX(counter *traffic.Counter) func(uint64) {
	if counter == nil {
		return nil
	}
	return counter.AddTX
}

func counterRX(counter *traffic.Counter) func(uint64) {
	if counter == nil {
		return nil
	}
	return counter.AddRX
}

func cleanCopyError(err error) error {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}
