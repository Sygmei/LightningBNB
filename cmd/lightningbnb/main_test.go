package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestClientRequiresDeviceWithoutTerminal(t *testing.T) {
	var output bytes.Buffer
	err := run(context.Background(), []string{"client"}, strings.NewReader(""), &output, &output, false)
	if err == nil || !strings.Contains(err.Error(), "--device is required") {
		t.Fatalf("run error = %v", err)
	}
}

func TestServerRequiresTargetPort(t *testing.T) {
	var output bytes.Buffer
	err := run(context.Background(), []string{"server"}, strings.NewReader(""), &output, &output, false)
	if err == nil || !strings.Contains(err.Error(), "--target-port") {
		t.Fatalf("run error = %v", err)
	}
}

func TestClientRejectsNegativeStatsInterval(t *testing.T) {
	var output bytes.Buffer
	err := run(
		context.Background(),
		[]string{"client", "--device", "test", "--stats-interval", "-1s"},
		strings.NewReader(""),
		&output,
		&output,
		false,
	)
	if err == nil || !strings.Contains(err.Error(), "--stats-interval") {
		t.Fatalf("run error = %v", err)
	}
}

func TestBenchmarkRequiresModeAndAddress(t *testing.T) {
	var output bytes.Buffer
	if err := run(context.Background(), []string{"benchmark"}, strings.NewReader(""), &output, &output, false); err == nil || !strings.Contains(err.Error(), "client or server") {
		t.Fatalf("benchmark error = %v", err)
	}
	output.Reset()
	if err := run(context.Background(), []string{"benchmark", "client"}, strings.NewReader(""), &output, &output, false); err == nil || !strings.Contains(err.Error(), "--address") {
		t.Fatalf("benchmark client error = %v", err)
	}
}

func TestVersion(t *testing.T) {
	var output bytes.Buffer
	if err := run(context.Background(), []string{"version"}, strings.NewReader(""), &output, &output, false); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(output.String()) != version {
		t.Fatalf("version output = %q", output.String())
	}
}
