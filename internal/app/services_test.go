package app

import (
	"reflect"
	"strings"
	"testing"

	"github.com/Sygmei/LightningBNB/internal/mux"
)

func TestParseServerServices(t *testing.T) {
	services, err := ParseServerServices([]string{"http:1180", "https:11443"})
	if err != nil {
		t.Fatal(err)
	}
	if len(services) != 2 || services[0].Name != "http" || services[0].Port != 1180 {
		t.Fatalf("services = %+v", services)
	}
}

func TestParseServerServicesRejectsNumericAliasAndDuplicate(t *testing.T) {
	for _, values := range [][]string{{"1180:1180"}, {"http:1180", "http:11443"}} {
		if _, err := ParseServerServices(values); err == nil {
			t.Fatalf("ParseServerServices(%q) succeeded", values)
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
