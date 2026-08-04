//go:build linux

package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/godbus/dbus/v5"
)

const (
	loginManagerDestination = "org.freedesktop.login1"
	loginManagerPath        = dbus.ObjectPath("/org/freedesktop/login1")
	loginManagerInhibit     = "org.freedesktop.login1.Manager.Inhibit"
)

func acquireSleepInhibitor(ctx context.Context) (io.Closer, error) {
	connection, err := dbus.SystemBus()
	if err != nil {
		return nil, fmt.Errorf("connect to system D-Bus: %w", err)
	}
	if !connection.SupportsUnixFDs() {
		return nil, errors.New("system D-Bus does not support file descriptor passing")
	}

	var descriptor dbus.UnixFD
	err = connection.Object(loginManagerDestination, loginManagerPath).
		CallWithContext(
			ctx,
			loginManagerInhibit,
			0,
			"sleep:idle",
			"LightningBNB",
			"Bluetooth bridge server is running",
			"block",
		).
		Store(&descriptor)
	if err != nil {
		return nil, fmt.Errorf("acquire systemd-logind inhibitor: %w", err)
	}
	if descriptor < 0 {
		return nil, errors.New("systemd-logind returned an invalid inhibitor descriptor")
	}

	file := os.NewFile(uintptr(descriptor), "lightningbnb-sleep-inhibitor")
	if file == nil {
		return nil, errors.New("open systemd-logind inhibitor descriptor")
	}
	return file, nil
}
