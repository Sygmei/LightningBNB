package app

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/Sygmei/LightningBNB/internal/ble"
)

func Scan(ctx context.Context, timeout time.Duration, all bool, output io.Writer) error {
	adapter := ble.NewAdapter()
	var devices []ble.Device
	var err error
	if all {
		devices, err = adapter.ScanAll(ctx, timeout)
	} else {
		devices, err = adapter.Scan(ctx, timeout)
	}
	if err != nil {
		return err
	}
	sortDevices(devices)
	if len(devices) == 0 {
		if all {
			_, _ = fmt.Fprintln(output, "No BLE advertisements found.")
		} else {
			_, _ = fmt.Fprintln(output, "No LightningBNB servers found.")
		}
		return nil
	}
	if all {
		_, _ = fmt.Fprintln(output, "ID\tRSSI\tLIGHTNINGBNB\tNAME\tSERVICE_UUIDS\tSERVICE_DATA\tMANUFACTURER_DATA")
		for _, device := range devices {
			name := strings.ReplaceAll(device.Name, "\t", " ")
			if name == "" {
				name = "(unnamed)"
			}
			_, _ = fmt.Fprintf(output, "%s\t%d\t%t\t%s\t%s\t%s\t%s\n",
				device.ID,
				device.RSSI,
				device.LightningBNB,
				name,
				strings.Join(device.ServiceUUIDs, ","),
				strings.Join(device.ServiceData, ","),
				strings.Join(device.ManufacturerData, ","),
			)
		}
		return nil
	}
	_, _ = fmt.Fprintln(output, "SERVER_ID\tPLATFORM_ID\tRSSI\tNAME")
	for _, device := range devices {
		name := device.Name
		if name == "" {
			name = "(unnamed)"
		}
		serverID := device.ServerID
		if serverID == "" {
			serverID = "(unavailable)"
		}
		_, _ = fmt.Fprintf(output, "%s\t%s\t%d\t%s\n", serverID, device.ID, device.RSSI, name)
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
