package bridge

import (
	"errors"
	"io"
	"net"

	"github.com/Sygmei/LightningBNB/internal/mux"
)

type closeWriter interface {
	CloseWrite() error
}

// Proxy copies a TCP connection and a multiplexed stream in both directions,
// preserving TCP half-closes. It returns the first non-clean copy error.
func Proxy(tcp net.Conn, stream *mux.Stream) error {
	errorsCh := make(chan error, 2)
	go func() {
		_, err := io.Copy(stream, tcp)
		if closeWriteErr := stream.CloseWrite(); err == nil {
			err = closeWriteErr
		}
		errorsCh <- cleanCopyError(err)
	}()
	go func() {
		_, err := io.Copy(tcp, stream)
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

func cleanCopyError(err error) error {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}
