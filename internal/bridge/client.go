package bridge

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/Sygmei/LightningBNB/internal/mux"
	"github.com/Sygmei/LightningBNB/internal/traffic"
)

type Availability interface {
	IsBound() bool
	Done() <-chan struct{}
}

type Endpoint struct {
	Link Availability
	Mux  *mux.Session
}

type Client struct {
	resumeTimeout time.Duration
	limit         chan struct{}
	logf          func(string, ...any)
	traffic       *traffic.Counter

	mu       sync.Mutex
	endpoint *Endpoint
	notify   chan struct{}
}

func NewClient(resumeTimeout time.Duration, maxConnections int, logf func(string, ...any)) *Client {
	return NewClientWithTraffic(resumeTimeout, maxConnections, logf, nil)
}

func NewClientWithTraffic(resumeTimeout time.Duration, maxConnections int, logf func(string, ...any), counter *traffic.Counter) *Client {
	if resumeTimeout <= 0 {
		resumeTimeout = 60 * time.Second
	}
	if maxConnections <= 0 {
		maxConnections = 32
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Client{
		resumeTimeout: resumeTimeout,
		limit:         make(chan struct{}, maxConnections),
		logf:          logf,
		traffic:       counter,
		notify:        make(chan struct{}),
	}
}

func (c *Client) SetEndpoint(endpoint *Endpoint) {
	c.mu.Lock()
	c.endpoint = endpoint
	c.signalLocked()
	c.mu.Unlock()
	if endpoint != nil {
		go func() {
			<-endpoint.Mux.Done()
			c.mu.Lock()
			if c.endpoint == endpoint {
				c.endpoint = nil
				c.signalLocked()
			}
			c.mu.Unlock()
		}()
	}
}

func (c *Client) Serve(ctx context.Context, listener net.Listener) error {
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept local TCP connection: %w", err)
		}
		select {
		case c.limit <- struct{}{}:
			go c.handle(ctx, conn)
		default:
			c.logf("rejecting local connection from %s: connection limit reached", conn.RemoteAddr())
			_ = conn.Close()
		}
	}
}

func (c *Client) handle(parent context.Context, conn net.Conn) {
	defer func() { <-c.limit }()
	defer conn.Close()
	ctx, cancel := context.WithTimeout(parent, c.resumeTimeout)
	defer cancel()
	endpoint, err := c.waitReady(ctx)
	if err != nil {
		c.logf("closing queued connection from %s: %v", conn.RemoteAddr(), err)
		return
	}
	stream, err := endpoint.Mux.Open(ctx)
	if err != nil {
		c.logf("opening bridged stream for %s: %v", conn.RemoteAddr(), err)
		return
	}
	if err := ProxyWithTraffic(conn, stream, c.traffic); err != nil {
		c.logf("bridged connection %s ended with error: %v", conn.RemoteAddr(), err)
	}
}

func (c *Client) waitReady(ctx context.Context) (*Endpoint, error) {
	for {
		c.mu.Lock()
		endpoint := c.endpoint
		notify := c.notify
		c.mu.Unlock()
		if endpoint != nil && endpoint.Link.IsBound() {
			select {
			case <-endpoint.Mux.Done():
			default:
				return endpoint, nil
			}
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-notify:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (c *Client) signalLocked() {
	close(c.notify)
	c.notify = make(chan struct{})
}
