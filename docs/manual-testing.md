# Manual Bluetooth test checklist

Run this checklist with physical BLE adapters before declaring a release supported. Pair the computers with their operating-system tools first and record adapter models, OS versions, BlueZ version where applicable, and the LightningBNB commit.

## Platform matrix

- Windows client → Windows server
- Windows client → Linux server
- Linux client → Windows server
- Linux client → Linux server
- macOS client → Windows server
- macOS client → Linux server

For every pairing:

1. Start TCP echo targets bound to server-side `127.0.0.1` and, where available, `[::1]`.
2. Run `lightningbnb server --target-port PORT`, then confirm `lightningbnb scan` shows the server name or identifier and RSSI.
3. Start the client once through the interactive picker and once with `--device ID`. Confirm the printed `LISTEN_ADDR` uses a random port unless an explicit port is supplied.
4. Open at least four simultaneous local TCP connections. Send distinguishable bidirectional payloads and verify no cross-stream data or starvation.
5. Exercise a protocol that uses TCP half-close: send a request, close only the client write direction, and verify the complete response still arrives.
6. Stop the target and verify a new local connection closes with a target error in the server/client diagnostics without disrupting existing streams.

## Recovery scenarios

With one or more streams actively exchanging numbered messages:

1. Disable Bluetooth for less than 60 seconds, continue writing within the 1 MiB replay bound, re-enable Bluetooth, and verify every byte arrives exactly once and in order.
2. Move a device out of range and back before 60 seconds; verify the same result.
3. Connect a new local socket during the outage and verify it waits without being read, then opens after recovery.
4. Exceed `--max-connections` during the outage and verify excess sockets close immediately.
5. Leave Bluetooth unavailable beyond 60 seconds and verify all active and waiting TCP sockets close. Restore Bluetooth and verify a new session accepts new sockets.
6. Restart either process during an outage and verify old streams close rather than attaching to the new process.
7. Attempt to connect a second bridge client while the first session is active or resumable and verify it receives a busy rejection.

## Throughput benchmark

Run `lightningbnb server --benchmark` on the BLE server computer and `lightningbnb benchmark --device ID` on the client computer. Exercise upload, download, and both directions with one and four streams per direction. Repeat with `--compression` on both processes. Confirm both processes report matching receiver-confirmed totals in the expected directions without an initial buffer-sized spike, and record the adapter models, MTU, direction, compression setting, and stream count with the observed result.

For a normal forwarding target, verify a compressed client transfers repetitive text and incompressible binary data exactly. Confirm a compressed client receives an explicit rejection from a server started without `--compression`, and an uncompressed client still works against a server that allows compression.

## Platform-specific checks

- Linux: run without root under the documented D-Bus permissions, restart `bluetoothd`, and verify the client/server recover or report actionable errors.
- Windows server: verify the adapter supports the peripheral role and the connectable GATT service is discoverable. The configured name may appear as `(unnamed)` because the WinRT service advertisement can omit it.
- macOS client: test both Intel and Apple Silicon when available, grant and revoke terminal Bluetooth permission, and confirm server mode returns the documented unsupported error.

Record observed throughput only as diagnostic information. It is not a compatibility gate beyond carrying interactive/modest-rate traffic without corruption.
