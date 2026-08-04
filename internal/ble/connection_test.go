package ble

import (
	"runtime"
	"testing"
	"time"

	"tinygo.org/x/bluetooth"
)

func TestApplicationConnectDeadlinesOutliveDarwinBackendCleanup(t *testing.T) {
	// tinygo.org/x/bluetooth uses a ten-second Connect timeout and this
	// repository's Darwin patch allows two more seconds for asynchronous
	// cancellation. Application deadlines must not abandon that call first.
	const backendWorstCase = 12 * time.Second
	if identityProbeTimeout <= backendWorstCase {
		t.Fatalf("identity probe timeout %s does not outlive backend cleanup", identityProbeTimeout)
	}
	if connectAttemptTimeout <= backendWorstCase {
		t.Fatalf("connect attempt timeout %s does not outlive backend cleanup", connectAttemptTimeout)
	}
}

func TestPendingConnectionCanOnlyBeTakenOnce(t *testing.T) {
	native := bluetooth.Device{}
	pending := newPendingConnection(&native)
	if _, ok := pending.take(); !ok {
		t.Fatal("first take did not return the pending connection")
	}
	if _, ok := pending.take(); ok {
		t.Fatal("pending connection was returned more than once")
	}
}

func TestValidateConnectedDeviceRejectsEmptyBackendResult(t *testing.T) {
	var address bluetooth.Address
	if runtime.GOOS == "darwin" {
		address.Set("8ae2e0e5-a073-5403-6327-327ff30c885b")
	} else {
		address.Set("01:02:03:04:05:06")
	}
	if err := validateConnectedDevice(bluetooth.Device{}, address); err == nil {
		t.Fatal("empty backend device was accepted")
	}
	if err := validateConnectedDevice(bluetooth.Device{Address: address}, address); err != nil {
		t.Fatalf("connected device was rejected: %v", err)
	}
}

func TestConnectedServerIDUsesTransportIdentity(t *testing.T) {
	const id = "lbnb:3f09e76b-5583-4888-a4cc-f6d64e180d58"
	if got := ConnectedServerID(&clientPacketConn{serverID: id}); got != id {
		t.Fatalf("connected server ID = %q", got)
	}
	if got := ConnectedServerID(nil); got != "" {
		t.Fatalf("nil transport server ID = %q", got)
	}
}
