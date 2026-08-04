# LightningBNB patch

This directory is a complete copy of `tinygo.org/x/bluetooth` v0.15.0. The
root module pins it with a `replace` directive because the dependency's API is
unstable and LightningBNB needs one Darwin transport fix that has not appeared
in an upstream release.

LightningBNB changes only the macOS central write-without-response path:

- `gap_darwin.go` forwards CoreBluetooth's
  `peripheralIsReadyToSendWriteWithoutResponse` delegate callback to a
  coalescing channel on the connected device.
- `gattc_darwin.go` waits for that signal when CoreBluetooth's transmit queue is
  full and uses a 1 ms readiness probe as a fallback for delayed or missed
  delegate callbacks, instead of relying on the upstream 15 ms polling cycle.
- `gattc_darwin_test.go` covers readiness notification, missed-callback
  recovery, and timeout behavior.

The upstream license is retained in `LICENSE`. When a released upstream
version provides equivalent callback-driven behavior, remove this patch and
the root `replace` directive after native Windows, Linux, and macOS validation.
