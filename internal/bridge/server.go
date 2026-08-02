package bridge

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/Sygmei/LightningBNB/internal/mux"
)

func ServeServer(ctx context.Context, session *mux.Session, target string, dialTimeout time.Duration, logf func(string, ...any)) error {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	dialer := net.Dialer{Timeout: dialTimeout}
	for {
		stream, err := session.Accept(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		}
		go func() {
			conn, err := dialer.DialContext(ctx, "tcp", target)
			if err != nil {
				logf("target connection for stream %d failed: %v", stream.ID(), err)
				_ = stream.Reject(fmt.Errorf("target unavailable: %w", err))
				return
			}
			if err := stream.Approve(); err != nil {
				_ = conn.Close()
				return
			}
			if err := Proxy(conn, stream); err != nil {
				logf("target connection for stream %d ended with error: %v", stream.ID(), err)
			}
		}()
	}
}
