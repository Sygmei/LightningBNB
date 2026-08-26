package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"golang.org/x/term"

	"github.com/Sygmei/LightningBNB/internal/ble"
	"github.com/Sygmei/LightningBNB/internal/link"
	"github.com/Sygmei/LightningBNB/internal/mux"
)

const pickerScanFrameInterval = 120 * time.Millisecond

const pickerServicesTimeout = 20 * time.Second

// The first device row follows the outer frame, title, scan status, and the
// two panel headers. Rows occupy two lines: the name and its server ID.
const pickerDeviceFirstRowY = 11

var pickerSpinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

type pickerScanEvent struct {
	device  *ble.Device
	devices []ble.Device
	err     error
	done    bool
}

type pickerScanTickMsg struct{}

type pickerContextMsg struct{ err error }

type pickerServicesEvent struct {
	deviceID string
	request  uint64
	services []mux.Service
	err      error
}

type pickerServicesState struct {
	loading  bool
	request  uint64
	services []mux.Service
	err      error
}

func terminalPickerFiles(input io.Reader, output io.Writer) (*os.File, *os.File, bool) {
	inputFile, inputOK := input.(*os.File)
	outputFile, outputOK := output.(*os.File)
	if !inputOK || !outputOK {
		return nil, nil, false
	}
	inputFD, outputFD := int(inputFile.Fd()), int(outputFile.Fd())
	if !term.IsTerminal(inputFD) || !term.IsTerminal(outputFD) {
		return nil, nil, false
	}
	return inputFile, outputFile, true
}

type pickerModel struct {
	parentCtx context.Context
	output    io.Writer

	events   <-chan pickerScanEvent
	scanDone <-chan struct{}
	cancel   context.CancelFunc

	orderedIDs          []string
	devices             map[string]ble.Device
	selected            int
	adapter             *ble.Adapter
	serviceState        map[string]pickerServicesState
	serviceProbeCancel  context.CancelFunc
	serviceProbeID      string
	serviceProbeRequest uint64
	windowWidth         int
	frame               int
	scanning            bool
	scanErr             error
	selectionErr        error
	canceled            bool
	result              *ble.Device
}

func newPickerModel(ctx context.Context, adapter *ble.Adapter, timeout time.Duration, output io.Writer) pickerModel {
	scanCtx, cancel := context.WithCancel(ctx)
	events := make(chan pickerScanEvent, 32)
	scanDone := make(chan struct{})
	go func() {
		defer close(scanDone)
		devices, scanErr := adapter.DiscoverWithCallback(scanCtx, timeout, func(device ble.Device) {
			copyOfDevice := device
			select {
			case events <- pickerScanEvent{device: &copyOfDevice}:
			case <-scanCtx.Done():
			}
		})
		select {
		case events <- pickerScanEvent{devices: devices, err: scanErr, done: true}:
		case <-scanCtx.Done():
		}
	}()
	return pickerModel{
		parentCtx:    ctx,
		output:       output,
		adapter:      adapter,
		events:       events,
		scanDone:     scanDone,
		cancel:       cancel,
		devices:      make(map[string]ble.Device),
		serviceState: make(map[string]pickerServicesState),
		scanning:     true,
	}
}

func chooseDeviceTUI(ctx context.Context, adapter *ble.Adapter, timeout time.Duration, input *os.File, output io.Writer) (ble.Device, error) {
	model := newPickerModel(ctx, adapter, timeout, output)
	program := tea.NewProgram(model, tea.WithInput(input), tea.WithOutput(output), tea.WithMouseAllMotion())
	finalModel, runErr := program.Run()
	model.stop()
	if runErr != nil {
		return ble.Device{}, runErr
	}
	picker, ok := finalModel.(pickerModel)
	if !ok {
		return ble.Device{}, errors.New("server picker returned an unexpected model")
	}
	picker.stop()
	if picker.scanErr != nil && !errors.Is(picker.scanErr, context.Canceled) {
		return ble.Device{}, picker.scanErr
	}
	if picker.selectionErr != nil {
		return ble.Device{}, picker.selectionErr
	}
	if picker.canceled || picker.result == nil {
		return ble.Device{}, errors.New("server selection canceled")
	}
	return *picker.result, nil
}

func (m pickerModel) Init() tea.Cmd {
	return tea.Batch(m.nextScanEvent(), m.nextScanTick(), m.waitForContext())
}

func (m pickerModel) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	switch message := message.(type) {
	case pickerScanEvent:
		if message.device != nil {
			if _, exists := m.devices[message.device.ID]; !exists {
				m.orderedIDs = append(m.orderedIDs, message.device.ID)
			}
			m.devices[message.device.ID] = *message.device
		}
		if message.done {
			m.scanning = false
			m.scanErr = message.err
			for _, device := range message.devices {
				if _, exists := m.devices[device.ID]; !exists {
					m.orderedIDs = append(m.orderedIDs, device.ID)
				}
				m.devices[device.ID] = device
			}
			if m.scanErr != nil && !errors.Is(m.scanErr, context.Canceled) {
				return m, tea.Quit
			}
			m, servicesCmd := m.ensureSelectedServices()
			return m, servicesCmd
		}
		if m.selected >= len(m.orderedIDs) {
			m.selected = len(m.orderedIDs) - 1
		}
		m, servicesCmd := m.ensureSelectedServices()
		return m, tea.Batch(m.nextScanEvent(), servicesCmd)

	case pickerScanTickMsg:
		if !m.scanning {
			return m, nil
		}
		m.frame = (m.frame + 1) % len(pickerSpinnerFrames)
		return m, m.nextScanTick()

	case tea.WindowSizeMsg:
		m.windowWidth = message.Width
		return m, nil

	case pickerContextMsg:
		m.selectionErr = message.err
		return m, tea.Quit

	case pickerServicesEvent:
		state, exists := m.serviceState[message.deviceID]
		if !exists || state.request != message.request {
			return m, nil
		}
		state.loading = false
		state.services = message.services
		state.err = message.err
		m.serviceState[message.deviceID] = state
		if m.serviceProbeRequest == message.request {
			if m.serviceProbeCancel != nil {
				m.serviceProbeCancel()
			}
			m.serviceProbeCancel = nil
			m.serviceProbeID = ""
		}
		return m, nil

	case tea.MouseMsg:
		mouse := tea.MouseMsg(message)
		if mouse.Action != tea.MouseActionMotion {
			return m, nil
		}
		index, ok := pickerDeviceIndexAtY(mouse.Y)
		if !ok || index >= len(m.orderedIDs) || index == m.selected {
			return m, nil
		}
		m.selected = index
		m, servicesCmd := m.ensureSelectedServices()
		return m, servicesCmd

	case tea.KeyMsg:
		switch message.String() {
		case "ctrl+c", "esc", "q", "Q":
			m.canceled = true
			m.cancel()
			return m, tea.Quit
		case "up", "k":
			if len(m.orderedIDs) > 0 {
				m.selected = (m.selected + len(m.orderedIDs) - 1) % len(m.orderedIDs)
				m, servicesCmd := m.ensureSelectedServices()
				return m, servicesCmd
			}
		case "down", "j":
			if len(m.orderedIDs) > 0 {
				m.selected = (m.selected + 1) % len(m.orderedIDs)
				m, servicesCmd := m.ensureSelectedServices()
				return m, servicesCmd
			}
		case "enter":
			if len(m.orderedIDs) == 0 {
				if !m.scanning {
					m.selectionErr = errors.New("no LightningBNB servers found")
					return m, tea.Quit
				}
				return m, nil
			}
			device := m.devices[m.orderedIDs[m.selected]]
			m.result = &device
			m.cancelServiceProbe()
			m.cancel()
			return m, tea.Quit
		}
	}
	return m, nil
}

func (m pickerModel) View() string {
	return renderPickerViewWithServicesSize(m.output, m.orderedIDs, m.devices, m.selected, m.scanning, m.frame, errorString(m.scanErr), m.serviceState, m.windowWidth)
}

func (m pickerModel) ensureSelectedServices() (pickerModel, tea.Cmd) {
	if len(m.orderedIDs) == 0 || m.selected < 0 || m.selected >= len(m.orderedIDs) {
		return m, nil
	}
	if m.serviceState == nil {
		m.serviceState = make(map[string]pickerServicesState)
	}
	deviceID := m.orderedIDs[m.selected]
	if state, exists := m.serviceState[deviceID]; exists && !state.loading {
		return m, nil
	}
	if m.serviceProbeID == deviceID && m.serviceProbeCancel != nil {
		return m, nil
	}
	m.cancelServiceProbe()
	device := m.devices[deviceID]
	probeCtx, cancel := context.WithTimeout(m.parentCtx, pickerServicesTimeout)
	m.serviceProbeCancel = cancel
	m.serviceProbeID = deviceID
	m.serviceProbeRequest++
	request := m.serviceProbeRequest
	m.serviceState[deviceID] = pickerServicesState{loading: true, request: request}
	return m, func() tea.Msg {
		services, err := discoverPickerServices(probeCtx, m.adapter, device)
		return pickerServicesEvent{deviceID: deviceID, request: request, services: services, err: err}
	}
}

func (m *pickerModel) cancelServiceProbe() {
	if m.serviceProbeCancel != nil {
		m.serviceProbeCancel()
	}
	m.serviceProbeCancel = nil
	m.serviceProbeID = ""
}

func (m pickerModel) nextScanEvent() tea.Cmd {
	return func() tea.Msg {
		return <-m.events
	}
}

func (m pickerModel) nextScanTick() tea.Cmd {
	return tea.Tick(pickerScanFrameInterval, func(time.Time) tea.Msg { return pickerScanTickMsg{} })
}

func (m pickerModel) waitForContext() tea.Cmd {
	return func() tea.Msg {
		<-m.parentCtx.Done()
		return pickerContextMsg{err: m.parentCtx.Err()}
	}
}

func (m pickerModel) stop() {
	if m.cancel == nil {
		return
	}
	m.cancelServiceProbe()
	m.cancel()
	select {
	case <-m.scanDone:
	case <-time.After(2 * time.Second):
	}
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

type pickerStyles struct {
	frame      lipgloss.Style
	panelLeft  lipgloss.Style
	panelRight lipgloss.Style
	title      lipgloss.Style
	heading    lipgloss.Style
	accent     lipgloss.Style
	checked    lipgloss.Style
	selected   lipgloss.Style
	body       lipgloss.Style
	muted      lipgloss.Style
	error      lipgloss.Style
	colored    bool
}

type pickerLayout struct {
	frameWidth int
	leftWidth  int
	rightWidth int
	split      bool
}

func pickerLayoutFor(windowWidth int) pickerLayout {
	if windowWidth <= 0 {
		windowWidth = 120
	}
	frameWidth := windowWidth - 10 // outer border and padding, plus a little air
	if frameWidth > 110 {
		frameWidth = 110
	}
	if frameWidth < 40 {
		frameWidth = 40
	}
	layout := pickerLayout{frameWidth: frameWidth}
	if frameWidth < 82 {
		layout.leftWidth = frameWidth - 6
		return layout
	}
	layout.split = true
	panelContentWidth := frameWidth - 13 // two borders, padding, and the one-column gap
	layout.leftWidth = panelContentWidth * 3 / 5
	layout.rightWidth = panelContentWidth - layout.leftWidth
	return layout
}

func newPickerStyles(output io.Writer, layout pickerLayout) pickerStyles {
	renderer := lipgloss.NewRenderer(output)
	colored := os.Getenv("NO_COLOR") == ""
	styles := pickerStyles{
		frame:      renderer.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("#334155")).Padding(1, 2).Width(layout.frameWidth),
		panelLeft:  renderer.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("#334155")).Padding(1, 2).Width(layout.leftWidth),
		panelRight: renderer.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("#334155")).Padding(1, 2).Width(layout.rightWidth),
		title:      renderer.NewStyle().Bold(true).Foreground(lipgloss.Color("#FF7A90")),
		heading:    renderer.NewStyle().Bold(true).Foreground(lipgloss.Color("#F8FAFC")),
		accent:     renderer.NewStyle().Bold(true).Foreground(lipgloss.Color("#7DD3FC")),
		checked:    renderer.NewStyle().Bold(true).Foreground(lipgloss.Color("#86EFAC")),
		selected:   renderer.NewStyle().Bold(true).Foreground(lipgloss.Color("#FDE68A")),
		body:       renderer.NewStyle().Foreground(lipgloss.Color("#CBD5E1")),
		muted:      renderer.NewStyle().Foreground(lipgloss.Color("#64748B")),
		error:      renderer.NewStyle().Bold(true).Foreground(lipgloss.Color("#FDA4AF")),
		colored:    colored,
	}
	if !colored {
		styles.title = renderer.NewStyle()
		styles.heading = renderer.NewStyle()
		styles.accent = renderer.NewStyle()
		styles.checked = renderer.NewStyle()
		styles.selected = renderer.NewStyle()
		styles.body = renderer.NewStyle()
		styles.muted = renderer.NewStyle()
		styles.error = renderer.NewStyle()
		styles.panelLeft = renderer.NewStyle().Border(lipgloss.RoundedBorder()).Padding(1, 2).Width(layout.leftWidth)
		styles.panelRight = renderer.NewStyle().Border(lipgloss.RoundedBorder()).Padding(1, 2).Width(layout.rightWidth)
	}
	return styles
}

func (s pickerStyles) render(style lipgloss.Style, value string) string {
	if !s.colored {
		return value
	}
	return style.Render(value)
}

func renderPicker(output io.Writer, orderedIDs []string, devices map[string]ble.Device, selected int, scanning bool, frame int, errorText string) error {
	_, err := io.WriteString(output, renderPickerView(output, orderedIDs, devices, selected, scanning, frame, errorText))
	return err
}

func renderPickerView(output io.Writer, orderedIDs []string, devices map[string]ble.Device, selected int, scanning bool, frame int, errorText string) string {
	return renderPickerViewWithServices(output, orderedIDs, devices, selected, scanning, frame, errorText, nil)
}

func renderPickerViewWithServices(output io.Writer, orderedIDs []string, devices map[string]ble.Device, selected int, scanning bool, frame int, errorText string, serviceState map[string]pickerServicesState) string {
	return renderPickerViewWithServicesSize(output, orderedIDs, devices, selected, scanning, frame, errorText, serviceState, 0)
}

func renderPickerViewWithServicesSize(output io.Writer, orderedIDs []string, devices map[string]ble.Device, selected int, scanning bool, frame int, errorText string, serviceState map[string]pickerServicesState, windowWidth int) string {
	layout := pickerLayoutFor(windowWidth)
	styles := newPickerStyles(output, layout)
	var result strings.Builder
	result.WriteString(styles.render(styles.title, "◆ LightningBNB") + styles.render(styles.muted, "  server picker") + "\n")
	result.WriteString(styles.render(styles.heading, "Select a nearby server") + "\n\n")
	if scanning {
		result.WriteString(styles.render(styles.accent, fmt.Sprintf("Scanning %s · %d server(s) found", pickerSpinnerFrames[frame], len(orderedIDs))))
	} else {
		result.WriteString(styles.render(styles.muted, fmt.Sprintf("Scan complete · %d server(s) found", len(orderedIDs))))
	}
	result.WriteString("\n\n")
	if errorText != "" {
		result.WriteString(styles.render(styles.error, "! "+errorText))
	} else if len(orderedIDs) == 0 {
		if scanning {
			result.WriteString(styles.render(styles.muted, "Waiting for nearby LightningBNB servers…"))
		} else {
			result.WriteString(styles.render(styles.muted, "No nearby LightningBNB servers found."))
		}
	} else {
		leftContent := renderPickerDeviceContent(styles, orderedIDs, devices, selected)
		rightContent := renderPickerServicesContent(styles, orderedIDs, devices, selected, serviceState)
		panelHeight := max(lipgloss.Height(leftContent), lipgloss.Height(rightContent))
		leftPanel := styles.panelLeft.Height(panelHeight).Render(leftContent)
		if layout.split {
			rightPanel := styles.panelRight.Height(panelHeight).Render(rightContent)
			result.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, leftPanel, " ", rightPanel))
		} else {
			result.WriteString(lipgloss.JoinVertical(lipgloss.Left, leftPanel, styles.panelRight.Width(layout.leftWidth).Height(panelHeight).Render(rightContent)))
		}
	}
	result.WriteString("\n" + styles.render(styles.muted, "↑/↓ or j/k move · hover to inspect · Enter connect · q cancel"))
	return styles.frame.Render(result.String())
}

func pickerDeviceIndexAtY(y int) (int, bool) {
	if y < pickerDeviceFirstRowY {
		return 0, false
	}
	return (y - pickerDeviceFirstRowY) / 2, true
}

func renderPickerDeviceContent(styles pickerStyles, orderedIDs []string, devices map[string]ble.Device, selected int) string {
	var panel strings.Builder
	panel.WriteString(styles.render(styles.heading, "Nearby servers") + "\n\n")
	for index, id := range orderedIDs {
		device := devices[id]
		marker := styles.render(styles.muted, "○")
		nameStyle := styles.body
		if index == selected {
			marker = styles.render(styles.accent, "❯")
			nameStyle = styles.selected
		}
		panel.WriteString(fmt.Sprintf("%s  %s  %s\n", marker, styles.render(nameStyle, deviceDisplayName(device)), styles.render(styles.body, fmt.Sprintf("RSSI=%d", device.RSSI))))
		serverID := device.ServerID
		if serverID == "" {
			serverID = device.ID
		}
		panel.WriteString("   " + styles.render(styles.muted, "ID "+serverID))
		if index < len(orderedIDs)-1 {
			panel.WriteString("\n")
		}
	}
	return panel.String()
}

func renderPickerServicesContent(styles pickerStyles, orderedIDs []string, devices map[string]ble.Device, selected int, serviceState map[string]pickerServicesState) string {
	var panel strings.Builder
	panel.WriteString(styles.render(styles.heading, "Exposed services") + "\n\n")
	if selected < 0 || selected >= len(orderedIDs) {
		panel.WriteString(styles.render(styles.muted, "Select a server to view\nits exposed services."))
		return panel.String()
	}
	deviceID := orderedIDs[selected]
	device := devices[deviceID]
	panel.WriteString(styles.render(styles.accent, deviceDisplayName(device)) + "\n\n")
	state, exists := serviceState[deviceID]
	if !exists || state.loading {
		panel.WriteString(styles.render(styles.muted, "Connecting to server…\nReading service list…"))
		return panel.String()
	}
	if state.err != nil {
		panel.WriteString(styles.render(styles.error, "! Unable to read services") + "\n")
		panel.WriteString(styles.render(styles.muted, state.err.Error()))
		return panel.String()
	}
	if len(state.services) == 0 {
		panel.WriteString(styles.render(styles.muted, "No exposed services."))
		return panel.String()
	}
	for index, service := range state.services {
		name := service.Name
		if name == "" {
			name = "(default)"
		}
		if index > 0 {
			panel.WriteString("\n")
		}
		panel.WriteString(styles.render(styles.body, fmt.Sprintf("%-14s :%d", name, service.Port)))
	}
	return panel.String()
}

func discoverPickerServices(ctx context.Context, adapter *ble.Adapter, device ble.Device) ([]mux.Service, error) {
	connectCtx, cancel := context.WithTimeout(ctx, ble.ConnectAttemptTimeout)
	packetConn, err := adapter.Connect(connectCtx, device)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer packetConn.Close()

	linkSession, err := link.NewSession(link.Config{
		ResumeTimeout:  link.DefaultResumeTimeout,
		ReplayWindow:   link.DefaultReplayWindow,
		MaxConnections: link.DefaultMaxConnections,
	})
	if err != nil {
		return nil, err
	}
	defer linkSession.Close()
	handshakeCtx, cancelHandshake := context.WithTimeout(ctx, 15*time.Second)
	err = linkSession.BindClient(handshakeCtx, packetConn)
	cancelHandshake()
	if err != nil {
		return nil, fmt.Errorf("handshake: %w", err)
	}
	muxSession := mux.NewClient(linkSession, link.DefaultMaxConnections)
	defer muxSession.Close()
	servicesCtx, cancelServices := context.WithTimeout(ctx, 10*time.Second)
	services, err := muxSession.Services(servicesCtx)
	cancelServices()
	if err != nil {
		return nil, fmt.Errorf("read service list: %w", err)
	}
	return services, nil
}
