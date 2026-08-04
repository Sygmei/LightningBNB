package app

import (
	"os"
	"path/filepath"
	"testing"
)

func TestServerIDPersistsAcrossLoads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "server-id")
	first, resolvedPath, err := loadOrCreateServerID(path)
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := loadOrCreateServerID(path)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || resolvedPath != path {
		t.Fatalf("server identity changed: first=%s second=%s path=%s", first, second, resolvedPath)
	}
}

func TestServerIDRejectsInvalidPersistedValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server-id")
	if err := os.WriteFile(path, []byte("not-an-id\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadOrCreateServerID(path); err == nil {
		t.Fatal("invalid persisted server ID was accepted")
	}
}
