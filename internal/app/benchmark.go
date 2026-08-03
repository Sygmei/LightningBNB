package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Sygmei/LightningBNB/internal/mux"
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

type BenchmarkClientConfig struct {
	Address       string
	Direction     string
	Duration      time.Duration
	DialTimeout   time.Duration
	Connections   int
	StatsInterval time.Duration
	ErrorOutput   io.Writer
}

// RunBenchmarkClient drives as much payload as possible through the local
// listener owned by a LightningBNB benchmark client.
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
		"benchmark started: direction=%s streams=%d duration=%s",
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

func ServeBenchmarkStreams(ctx context.Context, session *mux.Session, counter *traffic.Counter, logf func(string, ...any)) error {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	for {
		stream, err := session.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go func() {
			if err := stream.Approve(); err != nil {
				_ = stream.Close()
				return
			}
			if err := handleBenchmarkConnection(ctx, stream, counter); err != nil && ctx.Err() == nil {
				logf("benchmark stream %d ended: %v", stream.ID(), err)
			}
		}()
	}
}

type benchmarkConnection interface {
	io.Reader
	io.Writer
	io.Closer
}

func handleBenchmarkConnection(ctx context.Context, conn benchmarkConnection, counter *traffic.Counter) error {
	defer conn.Close()
	configureBenchmarkTCP(conn)
	setBenchmarkDeadline(conn, time.Now().Add(10*time.Second))
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	defer close(done)
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
	setBenchmarkDeadline(conn, time.Time{})

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

func ValidateBenchmarkDirection(value string) error {
	_, err := parseBenchmarkDirection(value)
	return err
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

func configureBenchmarkTCP(conn benchmarkConnection) {
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
		_ = tcp.SetReadBuffer(1 << 20)
		_ = tcp.SetWriteBuffer(1 << 20)
	}
}

func setBenchmarkDeadline(conn benchmarkConnection, deadline time.Time) {
	if deadlineConn, ok := conn.(interface{ SetDeadline(time.Time) error }); ok {
		_ = deadlineConn.SetDeadline(deadline)
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
