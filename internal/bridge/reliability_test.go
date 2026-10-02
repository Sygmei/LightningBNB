package bridge

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Sygmei/LightningBNB/internal/mux"
)

func TestOneOpenTimeoutDoesNotResetOtherConnections(t *testing.T) {
	a, b := net.Pipe()
	cm, sm := mux.NewClient(a, 2), mux.NewServer(b, 2)
	defer cm.Close()
	defer sm.Close()
	var resets atomic.Int32
	c := NewClient(time.Second, 2, nil)
	c.SetEndpoint(&Endpoint{Link: &testAvailability{bound: true}, Mux: cm, Reset: func() { resets.Add(1) }})
	tcp, peer := net.Pipe()
	defer peer.Close()
	c.limit <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	c.handle(ctx, tcp, "ninfer") // Peer deliberately leaves OPEN unapproved.
	if resets.Load() != 0 {
		t.Fatal("one timed out OPEN reset the entire session")
	}
	select {
	case <-cm.Done():
		t.Fatalf("session ended: %v", cm.Err())
	default:
	}
}
