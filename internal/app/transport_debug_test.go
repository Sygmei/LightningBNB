package app

import (
	"strings"
	"testing"
	"time"

	"github.com/Sygmei/LightningBNB/internal/link"
)

func TestFormatTransportDebugReportsIntervalAndQueueState(t *testing.T) {
	previous := link.TransportSnapshot{
		DataTXPackets: 2,
		DataTXBytes:   470,
		SendCalls:     2,
		SendDuration:  4 * time.Millisecond,
	}
	current := link.TransportSnapshot{
		DataTXPackets:         10,
		DataTXBytes:           2350,
		DataRXPackets:         1,
		DataRXBytes:           9,
		ACKRXPackets:          1,
		Retransmissions:       2,
		OutOfOrderDataPackets: 4,
		RejectedDataPackets:   3,
		SendCalls:             10,
		SendDuration:          20 * time.Millisecond,
		MaxSendDuration:       8 * time.Millisecond,
		OutstandingBytes:      1880,
		FlightBytes:           1880,
		PacketMTU:             244,
	}

	got := formatTransportDebug(current, previous)
	for _, want := range []string{
		"mtu=244",
		"data-tx=8pkts/1.8 KiB",
		"data-rx=1pkts/9 B",
		"ack-rx=1",
		"rtx=2",
		"ooo=4",
		"rejected=3",
		"send-api-avg=2ms",
		"send-api-max=8ms",
		"outstanding=1.8 KiB",
		"flight=1.8 KiB",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("transport output does not contain %q: %q", want, got)
		}
	}
}
