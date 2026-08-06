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
	// Reset closes the owning reliable session when multiplexed control stops
	// making progress, allowing the application to establish a clean binding.
	Reset func()
}

// Snapshot reports local TCP pressure. ActiveConnections includes both
// connections waiting for a BLE endpoint and connections already proxied;
// WaitingConnections is the subset whose socket reads are intentionally
// paused while the link is unavailable.
type Snapshot struct {
	ActiveConnections  int
	WaitingConnections int
	OpeningConnections int
}

const streamOpenTimeout = 15 * time.Second

type Client struct {
	resumeTimeout time.Duration
	limit         chan struct{}
	logf          func(string, ...any)
	traffic       *traffic.Counter
	waiting       int
	opening       int

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
			if endpoint.Reset != nil {
				endpoint.Reset()
			}
			c.mu.Lock()
			if c.endpoint == endpoint {
				c.endpoint = nil
				c.signalLocked()
			}
			c.mu.Unlock()
		}()
	}
}

func (c *Client) Snapshot() Snapshot {
	c.mu.Lock()
	waiting, opening := c.waiting, c.opening
	c.mu.Unlock()
	return Snapshot{ActiveConnections: len(c.limit), WaitingConnections: waiting, OpeningConnections: opening}
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
	c.mu.Lock()
	c.waiting++
	c.mu.Unlock()
	endpoint, err := c.waitReady(ctx)
	c.mu.Lock()
	c.waiting--
	c.mu.Unlock()
	if err != nil {
		c.logf("closing queued connection from %s: %v", conn.RemoteAddr(), err)
		return
	}
	c.mu.Lock()
	c.opening++
	c.mu.Unlock()
	openCtx, openCancel := context.WithTimeout(ctx, streamOpenTimeout)
	stream, err := endpoint.Mux.Open(openCtx)
	openCancel()
	c.mu.Lock()
	c.opening--
	c.mu.Unlock()
	if err != nil {
		c.logf("opening bridged stream for %s: %v", conn.RemoteAddr(), err)
		if errors.Is(err, context.DeadlineExceeded) && endpoint.Reset != nil {
			c.logf("multiplexed stream open stalled; resetting BLE session")
			endpoint.Reset()
		}
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
