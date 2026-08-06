package app

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Sygmei/LightningBNB/internal/link"
	"github.com/Sygmei/LightningBNB/internal/traffic"
)

const (
	trafficBarWidth     = 14
	trafficContentWidth = 64
)

type runtimeConsole struct {
	mu     sync.Mutex
	output io.Writer
	tui    bool
	color  bool
	logger *log.Logger

	lines            []string
	peakRate         float64
	peakCombinedRate float64
	linkHealth       linkHealthState
	bufferState      bufferState
	healthSession    *link.Session
	closed           bool
}

type linkHealthState struct {
	known              bool
	bound              bool
	heartbeatPending   bool
	failures           int
	applicationPending bool
}

type bufferState struct {
	known   bool
	queued  int
	opening int
	active  int
	bytes   uint64
}

func newRuntimeConsole(output io.Writer, tui bool) *runtimeConsole {
	if output == nil {
		output = io.Discard
	}
	console := &runtimeConsole{
		output: output,
		tui:    tui,
		color:  tui && os.Getenv("NO_COLOR") == "",
	}
	console.logger = log.New(console, "lightningbnb: ", log.LstdFlags)
	return console
}

func (c *runtimeConsole) Logger() *log.Logger { return c.logger }

// Write lets log.Logger coexist with the in-place dashboard. A diagnostic is
// printed above the dashboard, which is then redrawn below it.
func (c *runtimeConsole) Write(data []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.tui || len(c.lines) == 0 || c.closed {
		return c.output.Write(data)
	}
	c.clearLocked()
	n, err := c.output.Write(data)
	if drawErr := c.drawLocked(); err == nil {
		err = drawErr
	}
	return n, err
}

func (c *runtimeConsole) ReportTraffic(current, previous traffic.Snapshot, elapsed time.Duration, final bool) {
	if !c.tui {
		label := "traffic"
		if final {
			label = "traffic final"
		}
		c.logger.Printf("%s %s", label, formatTraffic(current, previous, elapsed))
		return
	}

	txRate, rxRate := trafficRates(current, previous, elapsed)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	if txRate > c.peakRate {
		c.peakRate = txRate
	}
	if rxRate > c.peakRate {
		c.peakRate = rxRate
	}
	if combinedRate := txRate + rxRate; combinedRate > c.peakCombinedRate {
		c.peakCombinedRate = combinedRate
	}
	c.clearLocked()
	c.lines = c.trafficLines(current, txRate, rxRate, final)
	_ = c.drawLocked()
}

// ReportLinkHealth updates the small status indicator in the live dashboard.
// It intentionally does not affect line-oriented output when the TUI is off.
func (c *runtimeConsole) SetLinkSession(session *link.Session) {
	if !c.tui {
		return
	}
	c.mu.Lock()
	c.healthSession = session
	c.mu.Unlock()
}

func (c *runtimeConsole) ReportLinkHealth(snapshot link.TransportSnapshot) {
	c.reportLinkHealth(nil, snapshot)
}

func (c *runtimeConsole) ReportLinkHealthFor(session *link.Session, snapshot link.TransportSnapshot) {
	c.reportLinkHealth(session, snapshot)
}

func (c *runtimeConsole) ReportBuffer(queued, active int, bytes uint64) {
	c.reportBuffer(nil, queued, active, bytes)
}

func (c *runtimeConsole) ReportBufferFor(session *link.Session, queued, active int, bytes uint64) {
	c.reportBuffer(session, queued, active, bytes)
}

func (c *runtimeConsole) ReportLinkAndBufferFor(session *link.Session, snapshot link.TransportSnapshot, queued, opening, active int, bytes uint64) {
	if !c.tui {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || (session != nil && c.healthSession != session) {
		return
	}
	c.linkHealth = linkHealthState{
		known:              true,
		bound:              snapshot.Bound,
		heartbeatPending:   snapshot.HeartbeatPending,
		failures:           snapshot.HeartbeatConsecutiveFailures,
		applicationPending: opening > 0,
	}
	c.bufferState = bufferState{known: true, queued: queued, opening: opening, active: active, bytes: bytes}
	if len(c.lines) > 0 {
		c.clearLocked()
		c.lines = c.trafficLinesFromCurrent(c.lines)
		_ = c.drawLocked()
	}
}

func (c *runtimeConsole) reportBuffer(session *link.Session, queued, active int, bytes uint64) {
	if !c.tui {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || (session != nil && c.healthSession != session) {
		return
	}
	c.bufferState = bufferState{known: true, queued: queued, active: active, bytes: bytes}
	if len(c.lines) > 0 {
		c.clearLocked()
		c.lines = c.trafficLinesFromCurrent(c.lines)
		_ = c.drawLocked()
	}
}

func (c *runtimeConsole) reportLinkHealth(session *link.Session, snapshot link.TransportSnapshot) {
	if !c.tui {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || (session != nil && c.healthSession != session) {
		return
	}
	c.linkHealth = linkHealthState{
		known:            true,
		bound:            snapshot.Bound,
		heartbeatPending: snapshot.HeartbeatPending,
		failures:         snapshot.HeartbeatConsecutiveFailures,
	}
	if len(c.lines) > 0 {
		c.clearLocked()
		c.lines = c.trafficLinesFromCurrent(c.lines)
		_ = c.drawLocked()
	}
}

func (c *runtimeConsole) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.tui && len(c.lines) > 0 {
		_, err := io.WriteString(c.output, "\n")
		return err
	}
	return nil
}

func (c *runtimeConsole) trafficLines(current traffic.Snapshot, txRate, rxRate float64, final bool) []string {
	state := "LIVE"
	if final {
		state = "FINAL"
	}
	combinedRate := txRate + rxRate
	txBar := c.rateBar(txRate)
	rxBar := c.rateBar(rxRate)
	top := dashboardTop(state)
	healthLine := c.healthLine()
	bufferLine := c.bufferLine()
	txLine := dashboardLine(fmt.Sprintf("TX  total %-10s now %-11s %s", formatBytes(float64(current.TX)), formatBytes(txRate)+"/s", txBar))
	rxLine := dashboardLine(fmt.Sprintf("RX  total %-10s now %-11s %s", formatBytes(float64(current.RX)), formatBytes(rxRate)+"/s", rxBar))
	if c.color {
		top = c.paint(top, "1;36")
		txLine = strings.Replace(txLine, "TX", c.paint("TX", "1;33"), 1)
		txLine = strings.Replace(txLine, txBar, c.paint(txBar, "33"), 1)
		rxLine = strings.Replace(rxLine, "RX", c.paint("RX", "1;32"), 1)
		rxLine = strings.Replace(rxLine, rxBar, c.paint(rxBar, "32"), 1)
	}
	lines := []string{
		top,
		healthLine,
		bufferLine,
		txLine,
		rxLine,
	}
	if current.CompressionEnabled {
		txCompression := dashboardLine(compressionLine("TX", current.TXUncompressed, current.TXCompressed))
		rxCompression := dashboardLine(compressionLine("RX", current.RXUncompressed, current.RXCompressed))
		if c.color {
			txCompression = strings.Replace(txCompression, "TX", c.paint("TX", "1;33"), 1)
			rxCompression = strings.Replace(rxCompression, "RX", c.paint("RX", "1;32"), 1)
		}
		lines = append(lines, txCompression, rxCompression)
	}
	lines = append(
		lines,
		dashboardLine(fmt.Sprintf("⇅   total %-10s now %-11s peak %-9s", formatBytes(float64(current.TX+current.RX)), formatBytes(combinedRate)+"/s", formatBytes(c.peakCombinedRate)+"/s")),
		c.paint("╰"+strings.Repeat("─", trafficContentWidth+2)+"╯", "1;36"),
	)
	return lines
}

func (c *runtimeConsole) trafficLinesFromCurrent(previous []string) []string {
	// The current traffic values are already represented in the existing rows;
	// replace only the status rows so a diagnostic update does not reset rates
	// or peak bars between traffic samples.
	if len(previous) < 3 {
		return previous
	}
	updated := append([]string(nil), previous...)
	updated[1] = c.healthLine()
	updated[2] = c.bufferLine()
	return updated
}

func (c *runtimeConsole) healthLine() string {
	dot, label, color := "●", "UNKNOWN", "90"
	if c.linkHealth.known {
		switch {
		case !c.linkHealth.bound:
			label, color = "OFFLINE", "31"
		case c.linkHealth.applicationPending:
			label, color = "DEGRADED", "33"
		case c.linkHealth.heartbeatPending || c.linkHealth.failures > 0:
			label, color = "CHECKING", "33"
		default:
			label, color = "HEALTHY", "32"
		}
	}
	content := fmt.Sprintf("%s link %-9s", dot, label)
	line := dashboardLine(content)
	if c.color {
		line = strings.Replace(line, dot, c.paint(dot, color), 1)
	}
	return line
}

func (c *runtimeConsole) bufferLine() string {
	if !c.bufferState.known {
		return dashboardLine("BUF queued -- reqs active -- data --")
	}
	content := fmt.Sprintf("BUF queued %-4d opening %-4d active %-4d data %-10s", c.bufferState.queued, c.bufferState.opening, c.bufferState.active, formatBytes(float64(c.bufferState.bytes)))
	line := dashboardLine(content)
	if c.color {
		line = strings.Replace(line, "BUF", c.paint("BUF", "1;35"), 1)
	}
	return line
}

func compressionLine(direction string, uncompressed, compressed uint64) string {
	savings := 0.0
	if uncompressed > 0 {
		savings = (1 - float64(compressed)/float64(uncompressed)) * 100
	}
	return fmt.Sprintf(
		"%s  uncompressed %-10s compressed %-10s saved %6.1f%%",
		direction,
		formatBytes(float64(uncompressed)),
		formatBytes(float64(compressed)),
		savings,
	)
}

func (c *runtimeConsole) rateBar(rate float64) string {
	filled := 0
	if rate > 0 && c.peakRate > 0 {
		filled = int(rate/c.peakRate*trafficBarWidth + 0.5)
		if filled < 1 {
			filled = 1
		}
		if filled > trafficBarWidth {
			filled = trafficBarWidth
		}
	}
	return strings.Repeat("█", filled) + strings.Repeat("░", trafficBarWidth-filled)
}

func dashboardTop(state string) string {
	prefix := fmt.Sprintf("─ LightningBNB traffic · %-5s ", state)
	return "╭" + prefix + strings.Repeat("─", trafficContentWidth+2-utf8.RuneCountInString(prefix)) + "╮"
}

func dashboardLine(content string) string {
	padding := trafficContentWidth - utf8.RuneCountInString(content)
	if padding < 0 {
		padding = 0
	}
	return "│ " + content + strings.Repeat(" ", padding) + " │"
}

func (c *runtimeConsole) paint(value, code string) string {
	if !c.color {
		return value
	}
	return "\x1b[" + code + "m" + value + "\x1b[0m"
}

func (c *runtimeConsole) clearLocked() {
	for line := 0; line < len(c.lines); line++ {
		_, _ = io.WriteString(c.output, "\r\x1b[2K")
		if line+1 < len(c.lines) {
			_, _ = io.WriteString(c.output, "\x1b[1A")
		}
	}
}

func (c *runtimeConsole) drawLocked() error {
	_, err := io.WriteString(c.output, strings.Join(c.lines, "\n"))
	return err
}

func trafficRates(current, previous traffic.Snapshot, elapsed time.Duration) (float64, float64) {
	seconds := elapsed.Seconds()
	if seconds <= 0 {
		seconds = 1
	}
	return float64(delta(current.TX, previous.TX)) / seconds,
		float64(delta(current.RX, previous.RX)) / seconds
}
