//go:build linux

package bluetooth

import "testing"

func TestLinuxAdvertisementType(t *testing.T) {
	if got := linuxAdvertisementType(AdvertisingTypeInd); got != "peripheral" {
		t.Fatalf("connectable advertisement type = %q", got)
	}
	if got := linuxAdvertisementType(AdvertisingTypeNonConnInd); got != "broadcast" {
		t.Fatalf("non-connectable advertisement type = %q", got)
	}
}
