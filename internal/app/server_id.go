package app

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Sygmei/LightningBNB/internal/ble"
)

func loadOrCreateServerID(configuredPath string) (ble.ServerID, string, error) {
	path := configuredPath
	if path == "" {
		configDir, err := os.UserConfigDir()
		if err != nil {
			return ble.ServerID{}, "", fmt.Errorf("locate user configuration directory: %w", err)
		}
		path = filepath.Join(configDir, "LightningBNB", "server-id")
	}
	if data, err := os.ReadFile(path); err == nil {
		id, parseErr := ble.ParseServerID(strings.TrimSpace(string(data)))
		if parseErr != nil {
			return ble.ServerID{}, path, fmt.Errorf("parse server ID file %s: %w", path, parseErr)
		}
		return id, path, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return ble.ServerID{}, path, fmt.Errorf("read server ID file %s: %w", path, err)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return ble.ServerID{}, path, fmt.Errorf("create server ID directory: %w", err)
	}
	id, err := ble.NewServerID()
	if err != nil {
		return ble.ServerID{}, path, fmt.Errorf("generate server ID: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		return loadOrCreateServerID(path)
	}
	if err != nil {
		return ble.ServerID{}, path, fmt.Errorf("create server ID file %s: %w", path, err)
	}
	if _, err := fmt.Fprintln(file, id.String()); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return ble.ServerID{}, path, fmt.Errorf("write server ID file %s: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return ble.ServerID{}, path, fmt.Errorf("close server ID file %s: %w", path, err)
	}
	return id, path, nil
}
