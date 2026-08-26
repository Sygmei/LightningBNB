package app

import (
	"bytes"
	"strings"
	"testing"

	"github.com/Sygmei/LightningBNB/internal/ble"
)

func TestRenderPickerShowsScanningStateAndSelection(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	var output bytes.Buffer
	devices := map[string]ble.Device{
		"platform-a": {
			ID:       "platform-a",
			Name:     "Living room",
			RSSI:     -63,
			ServerID: "lbnb:99439dd9-6255-4cc3-a891-8ba1b0f5c0f5",
		},
		"platform-b": {
			ID:   "platform-b",
			Name: "Office",
			RSSI: -71,
		},
	}

	if err := renderPicker(&output, []string{"platform-a", "platform-b"}, devices, 1, true, 3, ""); err != nil {
		t.Fatal(err)
	}
	view := output.String()
	for _, expected := range []string{
		"◆ LightningBNB",
		"Select a nearby server",
		"Scanning ⠸ · 2 server(s) found",
		"↑/↓ or j/k move · Enter connect · q cancel",
		"Living room",
		"Office",
		"❯  Office",
		"ID platform-b",
		"lbnb:99439dd9-6255-4cc3-a891-8ba1b0f5c0f5",
	} {
		if !strings.Contains(view, expected) {
			t.Errorf("picker view does not contain %q:\n%s", expected, view)
		}
	}
	if strings.Contains(view, "\x1b[2J") || strings.Contains(view, "\x1b[H") {
		t.Fatalf("picker view clears the terminal: %q", view)
	}
}

func TestRenderPickerShowsCompletedEmptyScan(t *testing.T) {
	var output bytes.Buffer
	if err := renderPicker(&output, nil, nil, 0, false, 0, ""); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "No nearby LightningBNB servers found.") {
		t.Fatalf("picker view does not show completed empty scan:\n%s", output.String())
	}
}
