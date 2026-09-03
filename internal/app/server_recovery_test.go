package app

import "testing"

func TestSelectBLERecovery(t *testing.T) {
	tests := []struct {
		name                 string
		goos                 string
		disabled             bool
		contextActive        bool
		advertisementAborted bool
		radioAttempted       bool
		servicesAttempted    bool
		want                 bleRecoveryAction
	}{
		{name: "first Windows abort uses radio", goos: "windows", contextActive: true, advertisementAborted: true, want: bleRecoveryRadio},
		{name: "persistent abort uses services", goos: "windows", contextActive: true, advertisementAborted: true, radioAttempted: true, want: bleRecoveryServices},
		{name: "both recovery stages exhausted", goos: "windows", contextActive: true, advertisementAborted: true, radioAttempted: true, servicesAttempted: true},
		{name: "disabled", goos: "windows", disabled: true, contextActive: true, advertisementAborted: true},
		{name: "canceled", goos: "windows", advertisementAborted: true},
		{name: "different error", goos: "windows", contextActive: true},
		{name: "non-Windows", goos: "linux", contextActive: true, advertisementAborted: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := selectBLERecovery(test.goos, test.disabled, test.contextActive, test.advertisementAborted, test.radioAttempted, test.servicesAttempted)
			if got != test.want {
				t.Fatalf("selectBLERecovery() = %d, want %d", got, test.want)
			}
		})
	}
}
