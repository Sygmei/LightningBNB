package app

import (
	"context"
	"encoding/binary"
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
	benchmarkMagic             = "LBNBBEN1"
	benchmarkBlockSize         = 1024
	benchmarkOutstandingWindow = 4 * benchmarkBlockSize
	benchmarkReady             = byte(1)
	benchmarkStart             = byte(1)
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

type benchmarkConnection interface {
	io.Reader
	io.Writer
	io.Closer
}

type benchmarkPeer struct {
	conn      benchmarkConnection
	direction benchmarkDirection
}

type benchmarkOpener func(context.Context) (benchmarkConnection, error)

// RunBenchmarkClient drives the benchmark through a TCP endpoint. It remains
// useful to integration tests; the public benchmark command opens multiplexed
// streams directly with RunMuxBenchmarkClient.
func RunBenchmarkClient(ctx context.Context, cfg BenchmarkClientConfig) error {
	if cfg.Address == "" {
		return errors.New("benchmark address must not be empty")
	}
	dialer := net.Dialer{Timeout: cfg.DialTimeout}
	return runBenchmarkClient(ctx, cfg, func(ctx context.Context) (benchmarkConnection, error) {
		conn, err := dialer.DialContext(ctx, "tcp", cfg.Address)
		if err != nil {
			return nil, fmt.Errorf("connect to benchmark endpoint %s: %w", cfg.Address, err)
		}
		return conn, nil
	})
}

func RunMuxBenchmarkClient(ctx context.Context, cfg BenchmarkClientConfig, session *mux.Session) error {
	return runBenchmarkClient(ctx, cfg, func(ctx context.Context) (benchmarkConnection, error) {
		return session.Open(ctx)
	})
}

func runBenchmarkClient(ctx context.Context, cfg BenchmarkClientConfig, open benchmarkOpener) error {
	if cfg.Duration <= 0 || cfg.DialTimeout <= 0 {
		return errors.New("benchmark duration and setup timeout must be greater than zero")
	}
	directions, err := benchmarkDirections(cfg.Direction, cfg.Connections)
	if err != nil {
		return err
	}
	if cfg.StatsInterval < 0 {
		return errors.New("benchmark stats interval must not be negative")
	}
	if cfg.ErrorOutput == nil {
		cfg.ErrorOutput = io.Discard
	}
	direction, _ := parseBenchmarkDirection(cfg.Direction)
	logger := log.New(cfg.ErrorOutput, "lightningbnb: ", log.LstdFlags)
	peers := make([]benchmarkPeer, 0, len(directions))
	defer func() { closeBenchmarkConnections(peers) }()
	for _, streamDirection := range directions {
		setupCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
		conn, err := open(setupCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("open benchmark stream: %w", err)
		}
		configureBenchmarkTCP(conn)
		setBenchmarkDeadline(conn, time.Now().Add(cfg.DialTimeout))
		if err := writeFull(conn, benchmarkHeader(streamDirection)); err != nil {
			_ = conn.Close()
			return fmt.Errorf("start benchmark stream: %w", err)
		}
		peers = append(peers, benchmarkPeer{conn: conn, direction: streamDirection})
	}
	for _, peer := range peers {
		ready := []byte{0}
		if _, err := io.ReadFull(peer.conn, ready); err != nil {
			return fmt.Errorf("wait for benchmark responder: %w", err)
		}
		if ready[0] != benchmarkReady {
			return errors.New("invalid benchmark readiness marker")
		}
	}
	for _, peer := range peers {
		if err := writeFull(peer.conn, []byte{benchmarkStart}); err != nil {
			return fmt.Errorf("release benchmark stream: %w", err)
		}
		setBenchmarkDeadline(peer.conn, time.Time{})
	}

	counter := &traffic.Counter{}
	stopStats := startTrafficReporter(ctx, cfg.StatsInterval, counter, logger.Printf)
	defer stopStats()
	logger.Printf(
		"benchmark started: direction=%s streams=%d duration=%s",
		benchmarkDirectionName(direction),
		len(peers),
		cfg.Duration,
	)

	results := make(chan error, len(peers))
	var pumps sync.WaitGroup
	for _, peer := range peers {
		peer := peer
		pumps.Add(1)
		go func() {
			defer pumps.Done()
			if peer.direction == benchmarkUpload {
				results <- pumpBenchmarkSend(peer.conn, counter.AddTX)
			} else {
				results <- pumpBenchmarkReceive(peer.conn, counter.AddRX)
			}
		}()
	}
	startedAt := time.Now()
	timer := time.NewTimer(cfg.Duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		closeBenchmarkConnections(peers)
		pumps.Wait()
		return nil
	case <-timer.C:
		closeBenchmarkConnections(peers)
		pumps.Wait()
		elapsed := time.Since(startedAt)
		logger.Printf(
			"benchmark result duration=%s %s",
			elapsed.Round(time.Millisecond),
			formatTraffic(counter.Snapshot(), traffic.Snapshot{}, elapsed),
		)
		return nil
	case err := <-results:
		closeBenchmarkConnections(peers)
		pumps.Wait()
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("benchmark stream ended before %s: %w", cfg.Duration, err)
	}
}

func ServeBenchmarkStreams(ctx context.Context, session *mux.Session, counter *traffic.Counter, logf func(string, ...any)) error {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	for {
		stream, err := session.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil || cleanBenchmarkSessionError(err) {
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
	if direction != benchmarkUpload && direction != benchmarkDownload {
		return errors.New("invalid benchmark stream direction")
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

	if direction == benchmarkUpload {
		return cleanBenchmarkServerError(pumpBenchmarkReceive(conn, counter.AddRX))
	}
	return cleanBenchmarkServerError(pumpBenchmarkSend(conn, counter.AddTX))
}

type benchmarkACK struct {
	total uint64
	err   error
}

// pumpBenchmarkSend maintains a small application window and counts only bytes
// cumulatively acknowledged by the receiver. This prevents socket/mux buffers
// from being reported as Bluetooth throughput.
func pumpBenchmarkSend(conn benchmarkConnection, addConfirmed func(uint64)) error {
	payload := make([]byte, benchmarkBlockSize)
	for i := range payload {
		payload[i] = byte(i)
	}
	acks := make(chan benchmarkACK, benchmarkOutstandingWindow/benchmarkBlockSize+2)
	go func() {
		var encoded [8]byte
		for {
			if _, err := io.ReadFull(conn, encoded[:]); err != nil {
				acks <- benchmarkACK{err: err}
				return
			}
			acks <- benchmarkACK{total: binary.BigEndian.Uint64(encoded[:])}
		}
	}()

	var sent uint64
	var confirmed uint64
	for {
		for sent-confirmed < benchmarkOutstandingWindow {
			remaining := benchmarkOutstandingWindow - (sent - confirmed)
			block := min(uint64(len(payload)), remaining)
			if err := writeFull(conn, payload[:block]); err != nil {
				return err
			}
			sent += block
		}
		ack := <-acks
		if ack.err != nil {
			return ack.err
		}
		if ack.total < confirmed || ack.total > sent {
			return errors.New("invalid benchmark acknowledgement")
		}
		if ack.total > confirmed {
			addConfirmed(ack.total - confirmed)
			confirmed = ack.total
		}
	}
}

// pumpBenchmarkReceive counts delivered bytes and returns cumulative
// acknowledgements after each 1 KiB of progress.
func pumpBenchmarkReceive(conn benchmarkConnection, addReceived func(uint64)) error {
	buffer := make([]byte, benchmarkBlockSize)
	var total uint64
	var acknowledged uint64
	for {
		n, err := conn.Read(buffer)
		if n > 0 {
			total += uint64(n)
			addReceived(uint64(n))
			if total-acknowledged >= benchmarkBlockSize {
				var encoded [8]byte
				binary.BigEndian.PutUint64(encoded[:], total)
				if writeErr := writeFull(conn, encoded[:]); writeErr != nil {
					return writeErr
				}
				acknowledged = total
			}
		}
		if err != nil {
			return err
		}
	}
}

func benchmarkDirections(value string, connections int) ([]benchmarkDirection, error) {
	if connections <= 0 || connections > 32 {
		return nil, errors.New("benchmark connections must be between 1 and 32")
	}
	direction, err := parseBenchmarkDirection(value)
	if err != nil {
		return nil, err
	}
	streamCount := connections
	if direction == benchmarkBoth {
		streamCount *= 2
	}
	if streamCount > 32 {
		return nil, errors.New("bidirectional benchmarks support at most 16 streams per direction")
	}
	directions := make([]benchmarkDirection, 0, streamCount)
	for range connections {
		if direction == benchmarkUpload || direction == benchmarkBoth {
			directions = append(directions, benchmarkUpload)
		}
		if direction == benchmarkDownload || direction == benchmarkBoth {
			directions = append(directions, benchmarkDownload)
		}
	}
	return directions, nil
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

func ValidateBenchmarkConnections(direction string, connections int) error {
	_, err := benchmarkDirections(direction, connections)
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
	}
}

func setBenchmarkDeadline(conn benchmarkConnection, deadline time.Time) {
	if deadlineConn, ok := conn.(interface{ SetDeadline(time.Time) error }); ok {
		_ = deadlineConn.SetDeadline(deadline)
	}
}

func closeBenchmarkConnections(peers []benchmarkPeer) {
	for _, peer := range peers {
		_ = peer.conn.Close()
	}
}

func cleanBenchmarkServerError(err error) error {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, mux.ErrSessionClosed) {
		return nil
	}
	if err.Error() == "stream closed" {
		return nil
	}
	return err
}

func cleanBenchmarkSessionError(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, mux.ErrSessionClosed)
}
