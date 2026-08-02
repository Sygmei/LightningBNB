package app

import (
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/Sygmei/LightningBNB/internal/ble"
)

func Scan(ctx context.Context, timeout time.Duration, output io.Writer) error {
	adapter := ble.NewAdapter()
	devices, err := adapter.Scan(ctx, timeout)
	if err != nil {
		return err
	}
	sortDevices(devices)
	if len(devices) == 0 {
		_, _ = fmt.Fprintln(output, "No LightningBNB servers found.")
		return nil
	}
	_, _ = fmt.Fprintln(output, "ID\tRSSI\tNAME")
	for _, device := range devices {
		name := device.Name
		if name == "" {
			name = "(unnamed)"
		}
		_, _ = fmt.Fprintf(output, "%s\t%d\t%s\n", device.ID, device.RSSI, name)
	}
	return nil
}

func sortDevices(devices []ble.Device) {
	sort.Slice(devices, func(i, j int) bool {
		if devices[i].RSSI != devices[j].RSSI {
			return devices[i].RSSI > devices[j].RSSI
		}
		return devices[i].ID < devices[j].ID
	})
}
