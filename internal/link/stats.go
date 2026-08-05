package link

import "time"

// TransportSnapshot exposes diagnostic counters for the reliable BLE link.
// Payload traffic accounting remains in internal/traffic; these values include
// link framing and are intended only for troubleshooting transport stalls.
type TransportSnapshot struct {
	Bound                        bool
	HeartbeatPending             bool
	HeartbeatConsecutiveFailures int
	LastReceived                 time.Time
	DataTXPackets                uint64
	DataTXBytes                  uint64
	DataRXPackets                uint64
	DataRXBytes                  uint64
	ACKTXPackets                 uint64
	ACKRXPackets                 uint64
	HeartbeatTX                  uint64
	HeartbeatRX                  uint64
	HeartbeatFailures            uint64
	Retransmissions              uint64
	FastRetransmissions          uint64
	OutOfOrderDataPackets        uint64
	RejectedDataPackets          uint64
	SendCalls                    uint64
	SendDuration                 time.Duration
	MaxSendDuration              time.Duration
	OutstandingBytes             uint64
	FlightBytes                  uint64
	FlightLimitPackets           int
	BufferedRXBytes              uint64
	PacketMTU                    int
}

type transportStats struct {
	dataTXPackets         uint64
	dataTXBytes           uint64
	dataRXPackets         uint64
	dataRXBytes           uint64
	ackTXPackets          uint64
	ackRXPackets          uint64
	heartbeatTX           uint64
	heartbeatRX           uint64
	heartbeatFailures     uint64
	retransmissions       uint64
	fastRetransmissions   uint64
	outOfOrderDataPackets uint64
	rejectedDataPackets   uint64
	sendCalls             uint64
	sendDuration          time.Duration
	maxSendDuration       time.Duration
}

func (s *Session) TransportSnapshot() TransportSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot := TransportSnapshot{
		Bound:                 s.current != nil && !s.closed,
		DataTXPackets:         s.stats.dataTXPackets,
		DataTXBytes:           s.stats.dataTXBytes,
		DataRXPackets:         s.stats.dataRXPackets,
		DataRXBytes:           s.stats.dataRXBytes,
		ACKTXPackets:          s.stats.ackTXPackets,
		ACKRXPackets:          s.stats.ackRXPackets,
		HeartbeatTX:           s.stats.heartbeatTX,
		HeartbeatRX:           s.stats.heartbeatRX,
		HeartbeatFailures:     s.stats.heartbeatFailures,
		Retransmissions:       s.stats.retransmissions,
		FastRetransmissions:   s.stats.fastRetransmissions,
		OutOfOrderDataPackets: s.stats.outOfOrderDataPackets,
		RejectedDataPackets:   s.stats.rejectedDataPackets,
		SendCalls:             s.stats.sendCalls,
		SendDuration:          s.stats.sendDuration,
		MaxSendDuration:       s.stats.maxSendDuration,
		OutstandingBytes:      s.txNext - s.txBase,
		BufferedRXBytes:       uint64(len(s.rxBuf) + s.rxPendingBytes),
	}
	if s.current != nil {
		snapshot.HeartbeatPending = s.current.heartbeatPending
		snapshot.HeartbeatConsecutiveFailures = s.current.heartbeatFailures
		snapshot.LastReceived = s.current.lastRX
		snapshot.PacketMTU = s.current.mtu
		snapshot.FlightLimitPackets = max(initialFlightWindowPackets, s.current.flightPackets)
		if s.current.sendNext > s.txBase {
			snapshot.FlightBytes = s.current.sendNext - s.txBase
		}
	}
	return snapshot
}
