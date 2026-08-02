package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Sygmei/LightningBNB/internal/app"
)

var version = "dev"

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr, isTerminal(os.Stdin)); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "lightningbnb: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, input io.Reader, output, errorOutput io.Writer, interactive bool) error {
	if len(args) == 0 {
		printUsage(errorOutput)
		return errors.New("a subcommand is required")
	}
	switch args[0] {
	case "scan":
		flags := newFlagSet("scan", errorOutput)
		timeout := flags.Duration("timeout", 5*time.Second, "how long to scan for LightningBNB servers")
		if err := flags.Parse(args[1:]); err != nil {
			return flagError(err)
		}
		if *timeout <= 0 {
			return errors.New("--timeout must be greater than zero")
		}
		return app.Scan(ctx, *timeout, output)

	case "client":
		flags := newFlagSet("client", errorOutput)
		listenHost := flags.String("listen-host", "127.0.0.1", "local TCP listen host")
		listenPort := flags.Int("listen-port", 0, "local TCP listen port; 0 selects a random port")
		device := flags.String("device", "", "Bluetooth server identifier from the scan command")
		scanTimeout := flags.Duration("scan-timeout", 5*time.Second, "duration of each Bluetooth scan")
		resumeTimeout := flags.Duration("resume-timeout", 60*time.Second, "time active TCP sessions wait for BLE reconnection")
		maxConnections := flags.Int("max-connections", 32, "maximum active and queued TCP connections")
		if err := flags.Parse(args[1:]); err != nil {
			return flagError(err)
		}
		if *listenHost == "" {
			return errors.New("--listen-host must not be empty")
		}
		if *listenPort < 0 || *listenPort > 65535 {
			return errors.New("--listen-port must be between 0 and 65535")
		}
		if *scanTimeout <= 0 || *resumeTimeout <= 0 {
			return errors.New("scan and resume timeouts must be greater than zero")
		}
		if *maxConnections <= 0 || *maxConnections > 65535 {
			return errors.New("--max-connections must be between 1 and 65535")
		}
		if *device == "" && !interactive {
			return errors.New("--device is required when stdin is not an interactive terminal")
		}
		return app.RunClient(ctx, app.ClientConfig{
			ListenHost:     *listenHost,
			ListenPort:     *listenPort,
			DeviceID:       *device,
			ScanTimeout:    *scanTimeout,
			ResumeTimeout:  *resumeTimeout,
			MaxConnections: *maxConnections,
			Interactive:    interactive,
			Input:          input,
			Output:         output,
			ErrorOutput:    errorOutput,
		})

	case "server":
		flags := newFlagSet("server", errorOutput)
		targetHost := flags.String("target-host", "localhost", "fixed TCP target hostname or IP address")
		targetPort := flags.Int("target-port", 0, "fixed TCP target port (required)")
		name := flags.String("name", "LightningBNB", "Bluetooth advertisement name")
		dialTimeout := flags.Duration("dial-timeout", 10*time.Second, "target TCP connection timeout")
		resumeTimeout := flags.Duration("resume-timeout", 60*time.Second, "time active TCP sessions wait for BLE reconnection")
		maxConnections := flags.Int("max-connections", 32, "maximum multiplexed TCP connections")
		if err := flags.Parse(args[1:]); err != nil {
			return flagError(err)
		}
		if *targetHost == "" {
			return errors.New("--target-host must not be empty")
		}
		if *targetPort < 1 || *targetPort > 65535 {
			return errors.New("--target-port is required and must be between 1 and 65535")
		}
		if *name == "" {
			return errors.New("--name must not be empty")
		}
		if *dialTimeout <= 0 || *resumeTimeout <= 0 {
			return errors.New("dial and resume timeouts must be greater than zero")
		}
		if *maxConnections <= 0 || *maxConnections > 65535 {
			return errors.New("--max-connections must be between 1 and 65535")
		}
		return app.RunServer(ctx, app.ServerConfig{
			TargetHost:     *targetHost,
			TargetPort:     *targetPort,
			Name:           *name,
			DialTimeout:    *dialTimeout,
			ResumeTimeout:  *resumeTimeout,
			MaxConnections: *maxConnections,
			ErrorOutput:    errorOutput,
		})

	case "version", "--version", "-version":
		_, _ = fmt.Fprintln(output, version)
		return nil
	case "help", "--help", "-h":
		printUsage(output)
		return nil
	default:
		printUsage(errorOutput)
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func newFlagSet(name string, output io.Writer) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(output)
	return flags
}

func flagError(err error) error {
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	return err
}

func printUsage(output io.Writer) {
	_, _ = fmt.Fprintln(output, `Usage:
  lightningbnb scan [--timeout 5s]
  lightningbnb client [--listen-host 127.0.0.1] [--listen-port 0] [--device ID]
  lightningbnb server --target-port PORT [--target-host localhost]
  lightningbnb version`)
}

func isTerminal(file *os.File) bool {
	info, err := file.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}
