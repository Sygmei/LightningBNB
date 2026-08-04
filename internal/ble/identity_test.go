package ble

import "testing"

func TestServerIDRoundTrip(t *testing.T) {
	id, err := NewServerID()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseServerID(id.String())
	if err != nil {
		t.Fatal(err)
	}
	if parsed != id {
		t.Fatalf("parsed ID = %s, want %s", parsed, id)
	}
	if id[6]>>4 != 4 || id[8]>>6 != 2 {
		t.Fatalf("server ID does not use UUID v4 bits: %x", id)
	}
}

func TestServerIDRejectsPlatformIdentifier(t *testing.T) {
	if _, err := ParseServerID("8c360084-be91-5b58-c05b-52c416657fbf"); err == nil {
		t.Fatal("platform UUID was accepted as a stable server ID")
	}
}
