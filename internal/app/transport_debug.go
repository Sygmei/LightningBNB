package app

import (
	"context"
	"fmt"
	"time"

	"github.com/Sygmei/LightningBNB/internal/link"
)

const transportDebugInterval = time.Second

func startTransportDebugReporter(parent context.Context, session *link.Session, logf func(string, ...any)) {
	if session == nil || logf == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(transportDebugInterval)
		defer ticker.Stop()
		previous := session.TransportSnapshot()
		for {
			select {
			case <-parent.Done():
				return
			case <-session.Done():
				logf("transport final %s", formatTransportDebug(session.TransportSnapshot(), previous))
				return
			case <-ticker.C:
				current := session.TransportSnapshot()
				logf("transport %s", formatTransportDebug(current, previous))
				previous = current
			}
		}
	}()
}

func formatTransportDebug(current, previous link.TransportSnapshot) string {
	sendCalls := delta(current.SendCalls, previous.SendCalls)
	sendDuration := current.SendDuration - previous.SendDuration
	if sendDuration < 0 {
		sendDuration = 0
	}
	averageSend := time.Duration(0)
	if sendCalls > 0 {
		averageSend = sendDuration / time.Duration(sendCalls)
	}
	return fmt.Sprintf(
		"mtu=%d data-tx=%dpkts/%s data-rx=%dpkts/%s ack-tx=%d ack-rx=%d rtx=%d fast-rtx=%d ooo=%d rejected=%d send-api-avg=%s send-api-max=%s outstanding=%s flight=%s rx-buffer=%s",
		current.PacketMTU,
		delta(current.DataTXPackets, previous.DataTXPackets),
		formatBytes(float64(delta(current.DataTXBytes, previous.DataTXBytes))),
		delta(current.DataRXPackets, previous.DataRXPackets),
		formatBytes(float64(delta(current.DataRXBytes, previous.DataRXBytes))),
		delta(current.ACKTXPackets, previous.ACKTXPackets),
		delta(current.ACKRXPackets, previous.ACKRXPackets),
		delta(current.Retransmissions, previous.Retransmissions),
		delta(current.FastRetransmissions, previous.FastRetransmissions),
		delta(current.OutOfOrderDataPackets, previous.OutOfOrderDataPackets),
		delta(current.RejectedDataPackets, previous.RejectedDataPackets),
		averageSend.Round(time.Microsecond),
		current.MaxSendDuration.Round(time.Microsecond),
		formatBytes(float64(current.OutstandingBytes)),
		formatBytes(float64(current.FlightBytes)),
		formatBytes(float64(current.BufferedRXBytes)),
	)
}
