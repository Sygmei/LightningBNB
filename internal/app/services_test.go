package app

import (
	"reflect"
	"strings"
	"testing"

	"github.com/Sygmei/LightningBNB/internal/mux"
)

func TestParseServerServices(t *testing.T) {
	services, err := ParseServerServices([]string{"http:1180", "google:google.com:443", "https:[::1]:11443"})
	if err != nil {
		t.Fatal(err)
	}
	if len(services) != 3 || services[0].Name != "http" || services[0].Host != "" || services[0].Port != 1180 {
		t.Fatalf("services = %+v", services)
	}
	if services[1].Name != "google" || services[1].Host != "google.com" || services[1].Port != 443 {
		t.Fatalf("non-local service = %+v", services[1])
	}
	if services[2].Name != "https" || services[2].Host != "::1" || services[2].Port != 11443 {
		t.Fatalf("IPv6 service = %+v", services[2])
	}
}

func TestParseServerServicesRejectsNumericAliasAndDuplicate(t *testing.T) {
	for _, values := range [][]string{{"1180:1180"}, {"http:1180", "http:11443"}} {
		if _, err := ParseServerServices(values); err == nil {
			t.Fatalf("ParseServerServices(%q) succeeded", values)
		}
	}
}

func TestParseServerServicesRejectsMalformedTarget(t *testing.T) {
	for _, value := range []string{"google:google.com", "google:google.com:not-a-port", "google:::443"} {
		if _, err := ParseServerServices([]string{value}); err == nil {
			t.Fatalf("ParseServerServices(%q) succeeded", value)
		}
	}
}

func TestValidateClientServices(t *testing.T) {
	if err := ValidateClientServices([]ClientService{{LocalPort: 1180, Remote: "http"}, {LocalPort: 11443, Remote: "11443"}}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateClientServices([]ClientService{{LocalPort: 1180, Remote: ""}}); err == nil || !strings.Contains(err.Error(), "target") {
		t.Fatalf("invalid client service error = %v", err)
	}
}

func TestClientServicesForAdvertised(t *testing.T) {
	services, err := ClientServicesForAdvertised([]mux.Service{{Name: "http", Port: 1180}, {Port: 11443}})
	if err != nil {
		t.Fatal(err)
	}
	want := []ClientService{{LocalPort: 1180, Remote: "http"}, {LocalPort: 11443, Remote: "11443"}}
	if !reflect.DeepEqual(services, want) {
		t.Fatalf("services = %+v, want %+v", services, want)
	}
}

func TestClientServicesForAdvertisedRejectsDuplicatePorts(t *testing.T) {
	_, err := ClientServicesForAdvertised([]mux.Service{{Name: "http", Port: 1180}, {Name: "admin", Port: 1180}})
	if err == nil || !strings.Contains(err.Error(), "duplicate port") {
		t.Fatalf("duplicate port error = %v", err)
	}
}
