package bridge

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/Sygmei/LightningBNB/internal/mux"
	"github.com/Sygmei/LightningBNB/internal/traffic"
)

func ServeServer(ctx context.Context, session *mux.Session, target string, dialTimeout time.Duration, logf func(string, ...any)) error {
	return ServeServerWithTraffic(ctx, session, target, dialTimeout, logf, nil)
}

func ServeServerWithTraffic(ctx context.Context, session *mux.Session, target string, dialTimeout time.Duration, logf func(string, ...any), counter *traffic.Counter) error {
	return serveServer(ctx, session, dialTimeout, logf, counter, func(string) (string, bool) {
		return target, true
	})
}

// ServeServerWithServicesWithTraffic serves streams by resolving their
// selector against the advertised service list. A selector may be an alias or
// a numeric port, but numeric ports are valid only when advertised.
func ServeServerWithServicesWithTraffic(ctx context.Context, session *mux.Session, host string, services []mux.Service, dialTimeout time.Duration, logf func(string, ...any), counter *traffic.Counter) error {
	return serveServer(ctx, session, dialTimeout, logf, counter, func(selector string) (string, bool) {
		for _, service := range services {
			if selector == service.Name || (selector == "" && service.Name == "") || selector == strconv.Itoa(service.Port) {
				targetHost := host
				if service.Host != "" {
					targetHost = service.Host
				}
				return net.JoinHostPort(targetHost, strconv.Itoa(service.Port)), true
			}
		}
		return "", false
	})
}

func serveServer(ctx context.Context, session *mux.Session, dialTimeout time.Duration, logf func(string, ...any), counter *traffic.Counter, resolve func(string) (string, bool)) error {
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
			target, ok := resolve(stream.Service())
			if !ok {
				logf("service %q is not available for stream %d", stream.Service(), stream.ID())
				_ = stream.Reject(fmt.Errorf("service unavailable: %s", stream.Service()))
				return
			}
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
			if err := ProxyWithTraffic(conn, stream, counter); err != nil {
				logf("target connection for stream %d ended with error: %v", stream.ID(), err)
			}
		}()
	}
}
