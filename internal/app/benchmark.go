package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Sygmei/LightningBNB/internal/traffic"
)

const (
	benchmarkMagic     = "LBNBBEN1"
	benchmarkBlockSize = 64 * 1024
	benchmarkReady     = byte(1)
	benchmarkStart     = byte(1)
)

type benchmarkDirection byte

const (
	benchmarkUpload benchmarkDirection = iota + 1
	benchmarkDownload
	benchmarkBoth
)

type BenchmarkServerConfig struct {
	ListenHost     string
	ListenPort     int
	MaxConnections int
	StatsInterval  time.Duration
	Output         io.Writer
	ErrorOutput    io.Writer
}

type BenchmarkClientConfig struct {
	Address       string
	Direction     string
	Duration      time.Duration
	DialTimeout   time.Duration
	Connections   int
	StatsInterval time.Duration
	ErrorOutput   io.Writer
}

// RunBenchmarkServer starts the benchmark source/sink that should be selected
// as the ordinary LightningBNB server's fixed TCP target.
func RunBenchmarkServer(ctx context.Context, cfg BenchmarkServerConfig) error {
	if cfg.ListenHost == "" {
		return errors.New("benchmark listen host must not be empty")
	}
	if cfg.ListenPort < 0 || cfg.ListenPort > 65535 {
		return errors.New("benchmark listen port must be between 0 and 65535")
	}
	if cfg.MaxConnections <= 0 {
		return errors.New("benchmark max connections must be greater than zero")
	}
	if cfg.StatsInterval < 0 {
		return errors.New("benchmark stats interval must not be negative")
	}
	if cfg.Output == nil {
		cfg.Output = io.Discard
	}
	if cfg.ErrorOutput == nil {
		cfg.ErrorOutput = io.Discard
	}
	logger := log.New(cfg.ErrorOutput, "lightningbnb: ", log.LstdFlags)
	listener, err := net.Listen("tcp", net.JoinHostPort(cfg.ListenHost, strconv.Itoa(cfg.ListenPort)))
	if err != nil {
		return fmt.Errorf("listen for benchmark connections: %w", err)
	}
	_, _ = fmt.Fprintf(cfg.Output, "LISTEN_ADDR=%s\n", listener.Addr())
	logger.Printf("benchmark responder listening on %s", listener.Addr())

	runCtx, cancel := context.WithCancel(ctx)
	counter := &traffic.Counter{}
	stopStats := startTrafficReporter(runCtx, cfg.StatsInterval, counter, logger.Printf)
	limit := make(chan struct{}, cfg.MaxConnections)
	var handlers sync.WaitGroup
	go func() {
		<-runCtx.Done()
		_ = listener.Close()
	}()
	defer func() {
		cancel()
		_ = listener.Close()
		handlers.Wait()
		stopStats()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept benchmark connection: %w", err)
		}
		select {
		case limit <- struct{}{}:
			handlers.Add(1)
			go func() {
				defer handlers.Done()
				defer func() { <-limit }()
				if err := handleBenchmarkConnection(runCtx, conn, counter); err != nil && runCtx.Err() == nil {
					logger.Printf("benchmark connection from %s ended: %v", conn.RemoteAddr(), err)
				}
			}()
		default:
			logger.Printf("rejecting benchmark connection from %s: connection limit reached", conn.RemoteAddr())
			_ = conn.Close()
		}
	}
}

// RunBenchmarkClient drives as much payload as possible through an existing
// LightningBNB client listener for the requested duration.
func RunBenchmarkClient(ctx context.Context, cfg BenchmarkClientConfig) error {
	if cfg.Address == "" {
		return errors.New("benchmark address must not be empty")
	}
	if cfg.Duration <= 0 || cfg.DialTimeout <= 0 {
		return errors.New("benchmark duration and dial timeout must be greater than zero")
	}
	if cfg.Connections <= 0 || cfg.Connections > 32 {
		return errors.New("benchmark connections must be between 1 and 32")
	}
	if cfg.StatsInterval < 0 {
		return errors.New("benchmark stats interval must not be negative")
	}
	if cfg.ErrorOutput == nil {
		cfg.ErrorOutput = io.Discard
	}
	direction, err := parseBenchmarkDirection(cfg.Direction)
	if err != nil {
		return err
	}
	logger := log.New(cfg.ErrorOutput, "lightningbnb: ", log.LstdFlags)
	dialer := net.Dialer{Timeout: cfg.DialTimeout}
	connections := make([]net.Conn, 0, cfg.Connections)
	defer func() { closeBenchmarkConnections(connections) }()
	header := benchmarkHeader(direction)
	for range cfg.Connections {
		conn, err := dialer.DialContext(ctx, "tcp", cfg.Address)
		if err != nil {
			return fmt.Errorf("connect to benchmark endpoint %s: %w", cfg.Address, err)
		}
		configureBenchmarkTCP(conn)
		_ = conn.SetDeadline(time.Now().Add(cfg.DialTimeout))
		if err := writeFull(conn, header); err != nil {
			_ = conn.Close()
			return fmt.Errorf("start benchmark connection: %w", err)
		}
		connections = append(connections, conn)
	}
	for _, conn := range connections {
		ready := []byte{0}
		if _, err := io.ReadFull(conn, ready); err != nil {
			return fmt.Errorf("wait for benchmark responder: %w", err)
		}
		if ready[0] != benchmarkReady {
			return errors.New("invalid benchmark readiness marker")
		}
	}
	for _, conn := range connections {
		if err := writeFull(conn, []byte{benchmarkStart}); err != nil {
			return fmt.Errorf("release benchmark connection: %w", err)
		}
		_ = conn.SetWriteDeadline(time.Time{})
	}

	counter := &traffic.Counter{}
	stopStats := startTrafficReporter(ctx, cfg.StatsInterval, counter, logger.Printf)
	defer stopStats()
	logger.Printf(
		"benchmark started: endpoint=%s direction=%s connections=%d duration=%s",
		cfg.Address,
		benchmarkDirectionName(direction),
		cfg.Connections,
		cfg.Duration,
	)

	results := make(chan error, cfg.Connections*2)
	var pumps sync.WaitGroup
	for _, conn := range connections {
		startBenchmarkClientPumps(conn, direction, counter, results, &pumps)
	}
	startedAt := time.Now()
	timer := time.NewTimer(cfg.Duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		closeBenchmarkConnections(connections)
		pumps.Wait()
		return nil
	case <-timer.C:
		closeBenchmarkConnections(connections)
		pumps.Wait()
		elapsed := time.Since(startedAt)
		logger.Printf(
			"benchmark result duration=%s %s",
			elapsed.Round(time.Millisecond),
			formatTraffic(counter.Snapshot(), traffic.Snapshot{}, elapsed),
		)
		return nil
	case err := <-results:
		closeBenchmarkConnections(connections)
		pumps.Wait()
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("benchmark connection ended before %s: %w", cfg.Duration, err)
	}
}

func handleBenchmarkConnection(ctx context.Context, conn net.Conn, counter *traffic.Counter) error {
	defer conn.Close()
	configureBenchmarkTCP(conn)
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	header := make([]byte, len(benchmarkMagic)+1)
	if _, err := io.ReadFull(conn, header); err != nil {
		return fmt.Errorf("read benchmark handshake: %w", err)
	}
	if string(header[:len(benchmarkMagic)]) != benchmarkMagic {
		return errors.New("invalid benchmark handshake")
	}
	direction := benchmarkDirection(header[len(benchmarkMagic)])
	if !direction.valid() {
		return errors.New("invalid benchmark direction")
	}
	if err := writeFull(conn, []byte{benchmarkReady}); err != nil {
		return fmt.Errorf("confirm benchmark readiness: %w", err)
	}
	start := []byte{0}
	if _, err := io.ReadFull(conn, start); err != nil {
		return fmt.Errorf("wait for benchmark start: %w", err)
	}
	if start[0] != benchmarkStart {
		return errors.New("invalid benchmark start marker")
	}
	_ = conn.SetReadDeadline(time.Time{})

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	defer close(done)

	switch direction {
	case benchmarkUpload:
		return cleanBenchmarkServerError(pumpBenchmarkRead(conn, counter.AddRX))
	case benchmarkDownload:
		return cleanBenchmarkServerError(pumpBenchmarkWrite(conn, counter.AddTX))
	case benchmarkBoth:
		results := make(chan error, 2)
		go func() { results <- pumpBenchmarkWrite(conn, counter.AddTX) }()
		go func() { results <- pumpBenchmarkRead(conn, counter.AddRX) }()
		first := <-results
		_ = conn.Close()
		second := <-results
		if err := cleanBenchmarkServerError(first); err != nil {
			return err
		}
		return cleanBenchmarkServerError(second)
	default:
		return errors.New("invalid benchmark direction")
	}
}

func startBenchmarkClientPumps(conn net.Conn, direction benchmarkDirection, counter *traffic.Counter, results chan<- error, pumps *sync.WaitGroup) {
	start := func(pump func() error) {
		pumps.Add(1)
		go func() {
			defer pumps.Done()
			results <- pump()
		}()
	}
	if direction == benchmarkUpload || direction == benchmarkBoth {
		start(func() error { return pumpBenchmarkWrite(conn, counter.AddTX) })
	}
	if direction == benchmarkDownload || direction == benchmarkBoth {
		start(func() error { return pumpBenchmarkRead(conn, counter.AddRX) })
	}
}

func pumpBenchmarkWrite(writer io.Writer, add func(uint64)) error {
	payload := make([]byte, benchmarkBlockSize)
	for i := range payload {
		payload[i] = byte(i)
	}
	for {
		n, err := writer.Write(payload)
		if n > 0 {
			add(uint64(n))
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
}

func pumpBenchmarkRead(reader io.Reader, add func(uint64)) error {
	buffer := make([]byte, benchmarkBlockSize)
	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			add(uint64(n))
		}
		if err != nil {
			return err
		}
	}
}

func parseBenchmarkDirection(value string) (benchmarkDirection, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "upload", "tx":
		return benchmarkUpload, nil
	case "download", "rx":
		return benchmarkDownload, nil
	case "both", "bidirectional":
		return benchmarkBoth, nil
	default:
		return 0, errors.New("--direction must be upload, download, or both")
	}
}

func benchmarkDirectionName(direction benchmarkDirection) string {
	switch direction {
	case benchmarkUpload:
		return "upload"
	case benchmarkDownload:
		return "download"
	case benchmarkBoth:
		return "both"
	default:
		return "unknown"
	}
}

func (d benchmarkDirection) valid() bool {
	return d == benchmarkUpload || d == benchmarkDownload || d == benchmarkBoth
}

func benchmarkHeader(direction benchmarkDirection) []byte {
	header := make([]byte, len(benchmarkMagic)+1)
	copy(header, benchmarkMagic)
	header[len(benchmarkMagic)] = byte(direction)
	return header
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
		data = data[n:]
	}
	return nil
}

func configureBenchmarkTCP(conn net.Conn) {
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
		_ = tcp.SetReadBuffer(1 << 20)
		_ = tcp.SetWriteBuffer(1 << 20)
	}
}

func closeBenchmarkConnections(connections []net.Conn) {
	for _, conn := range connections {
		_ = conn.Close()
	}
}

func cleanBenchmarkServerError(err error) error {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe) {
		return nil
	}
	return err
}
