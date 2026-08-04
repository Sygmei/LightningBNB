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
  full, instead of polling readiness every 15 ms.
- `gattc_darwin_test.go` covers readiness notification and timeout behavior.

The upstream license is retained in `LICENSE`. When a released upstream
version provides equivalent callback-driven behavior, remove this patch and
the root `replace` directive after native Windows, Linux, and macOS validation.
