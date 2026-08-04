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

	"github.com/Sygmei/LightningBNB/internal/traffic"
)

const (
	trafficBarWidth     = 14
	trafficContentWidth = 58
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
	closed           bool
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
	txLine := dashboardLine(fmt.Sprintf("TX  total %-10s now %-11s %s", formatBytes(float64(current.TX)), formatBytes(txRate)+"/s", txBar))
	rxLine := dashboardLine(fmt.Sprintf("RX  total %-10s now %-11s %s", formatBytes(float64(current.RX)), formatBytes(rxRate)+"/s", rxBar))
	if c.color {
		top = c.paint(top, "1;36")
		txLine = strings.Replace(txLine, "TX", c.paint("TX", "1;33"), 1)
		txLine = strings.Replace(txLine, txBar, c.paint(txBar, "33"), 1)
		rxLine = strings.Replace(rxLine, "RX", c.paint("RX", "1;32"), 1)
		rxLine = strings.Replace(rxLine, rxBar, c.paint(rxBar, "32"), 1)
	}
	return []string{
		top,
		txLine,
		rxLine,
		dashboardLine(fmt.Sprintf("⇅   total %-10s now %-11s peak %-9s", formatBytes(float64(current.TX+current.RX)), formatBytes(combinedRate)+"/s", formatBytes(c.peakCombinedRate)+"/s")),
		c.paint("╰"+strings.Repeat("─", trafficContentWidth+2)+"╯", "1;36"),
	}
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
