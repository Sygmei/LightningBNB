//go:build linux && !baremetal

package bluetooth

import "testing"

func TestBluezAdvertisementTypeDefaultsToConnectable(t *testing.T) {
	if got := bluezAdvertisementType(AdvertisementOptions{}); got != "peripheral" {
		t.Fatalf("default BlueZ advertisement type = %q, want peripheral", got)
	}
	if got := bluezAdvertisementType(AdvertisementOptions{AdvertisementType: AdvertisingTypeNonConnInd}); got != "broadcast" {
		t.Fatalf("non-connectable BlueZ advertisement type = %q, want broadcast", got)
	}
}
