package app

import (
	"bytes"
	"strings"
	"testing"

	"github.com/Sygmei/LightningBNB/internal/ble"
	"github.com/Sygmei/LightningBNB/internal/mux"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
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
		"↑/↓ or j/k move · hover to inspect · Enter connect · q cancel",
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

func TestPickerMouseHoverSelectsServer(t *testing.T) {
	model := pickerModel{
		orderedIDs: []string{"platform-a", "platform-b"},
		devices: map[string]ble.Device{
			"platform-a": {ID: "platform-a", Name: "Living room"},
			"platform-b": {ID: "platform-b", Name: "Office"},
		},
		serviceState: map[string]pickerServicesState{
			"platform-b": {services: []mux.Service{{Name: "http", Port: 1180}}},
		},
	}

	updated, _ := model.Update(tea.MouseMsg{X: 10, Y: pickerDeviceFirstRowY + 2, Action: tea.MouseActionMotion})
	picker := updated.(pickerModel)
	if picker.selected != 1 {
		t.Fatalf("selected row = %d, want 1", picker.selected)
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

func TestRenderPickerShowsExposedServicesForSelectedServer(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	var output bytes.Buffer
	devices := map[string]ble.Device{
		"platform-a": {ID: "platform-a", Name: "Living room", RSSI: -63},
	}
	services := map[string]pickerServicesState{
		"platform-a": {
			services: []mux.Service{{Name: "http", Port: 1180}, {Name: "https", Port: 11443}},
		},
	}

	view := renderPickerViewWithServices(&output, []string{"platform-a"}, devices, 0, false, 0, "", services)
	for _, expected := range []string{
		"Nearby servers",
		"Exposed services",
		"Living room",
		"http           :1180",
		"https          :11443",
	} {
		if !strings.Contains(view, expected) {
			t.Errorf("picker view does not contain %q:\n%s", expected, view)
		}
	}
}

func TestRenderPickerKeepsPanelBordersAligned(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	devices := map[string]ble.Device{
		"platform-a": {ID: "platform-a", Name: "Living room"},
	}
	services := map[string]pickerServicesState{
		"platform-a": {
			services: []mux.Service{
				{Name: "ninfer", Port: 63000},
				{Name: "searxng", Port: 63001},
				{Name: "signal-cli", Port: 63002},
				{Name: "network-gateway", Port: 63003},
				{Name: "aistack", Port: 8990},
			},
		},
	}

	view := renderPickerViewWithServicesSize(&bytes.Buffer{}, []string{"platform-a"}, devices, 0, false, 0, "", services, 168)
	width := lipgloss.Width(view)
	if width > 116 {
		t.Fatalf("picker width = %d, want compact card no wider than 116:\n%s", width, view)
	}
	for lineNumber, line := range strings.Split(view, "\n") {
		if got := lipgloss.Width(line); got != width {
			t.Fatalf("line %d width = %d, want %d; borders are misaligned:\n%s", lineNumber+1, got, width, view)
		}
	}
}
