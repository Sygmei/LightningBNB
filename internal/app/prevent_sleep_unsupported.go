//go:build !linux && !windows

package app

import (
	"context"
	"errors"
	"io"
)

func acquireSleepInhibitor(context.Context) (io.Closer, error) {
	return nil, errors.New("sleep prevention is unsupported on this platform")
}
