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

func TestBenchmarkRequiresDeviceWithoutTerminal(t *testing.T) {
	var output bytes.Buffer
	if err := run(context.Background(), []string{"benchmark"}, strings.NewReader(""), &output, &output, false); err == nil || !strings.Contains(err.Error(), "--device") {
		t.Fatalf("benchmark error = %v", err)
	}
}

func TestBenchmarkRejectsInvalidDirectionBeforeConnecting(t *testing.T) {
	var output bytes.Buffer
	err := run(context.Background(), []string{"benchmark", "--device", "test", "--direction", "sideways"}, strings.NewReader(""), &output, &output, false)
	if err == nil || !strings.Contains(err.Error(), "--direction") {
		t.Fatalf("benchmark error = %v", err)
	}
}

func TestBenchmarkServerRejectsTargetPort(t *testing.T) {
	var output bytes.Buffer
	err := run(context.Background(), []string{"server", "--benchmark", "--target-port", "1234"}, strings.NewReader(""), &output, &output, false)
	if err == nil || !strings.Contains(err.Error(), "cannot be combined") {
		t.Fatalf("server error = %v", err)
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
