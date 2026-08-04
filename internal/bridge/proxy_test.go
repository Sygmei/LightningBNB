package bridge

import (
	"errors"
	"testing"
)

func TestCleanProxyErrorPreservesUnexpectedFailure(t *testing.T) {
	want := errors.New("unexpected copy failure")
	if got := cleanProxyError(want); !errors.Is(got, want) {
		t.Fatalf("cleanProxyError() = %v, want %v", got, want)
	}
}
