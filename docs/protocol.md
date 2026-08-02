# LightningBNB protocol version 1

All multi-byte integers use network byte order (big endian). Receivers reject malformed lengths, invalid stream state, unavailable replay offsets, and unsupported protocol versions.

## GATT service

| Purpose | UUID | Properties |
| --- | --- | --- |
| Service | `13f0b6a0-4746-4c42-8e2f-1f62e4a0b1a0` | Primary service |
| RX (client → server) | `13f0b6a1-4746-4c42-8e2f-1f62e4a0b1a0` | Write with response |
| TX (server → client) | `13f0b6a2-4746-4c42-8e2f-1f62e4a0b1a0` | Notify |

Linux advertises the service UUID and local name. Windows advertises the connectable GATT service directly through WinRT and attaches the `LBNB1` discovery marker as service data for that same UUID. Windows may omit the configured local name, in which case scanners display `(unnamed)` and the platform-specific device identifier remains the selection key. LightningBNB deliberately does not publish a second manufacturer-data advertisement because WinRT's generic advertisement publisher is not the connectable GATT service.

The negotiated packet size is the smaller peer limit, capped at 244 bytes. Bootstrap packets are no larger than the 20-byte minimum ATT value size. Empty and oversized packets are invalid where a payload is required.

## Session handshake

The client creates a random 128-bit session ID with `crypto/rand`. State exists only in memory.

| Type | Value | Body |
| --- | --- | --- |
| `HELLO_ID` | `0x01` | protocol version `u8`, session ID `[16]byte` |
| `HELLO` | `0x02` | next server byte expected `u64`, resume timeout milliseconds `u32`, maximum streams `u16`, packet MTU `u16` |
| `HELLO_ACK` | `0x03` | protocol version `u8`, next client byte expected `u64`, effective timeout milliseconds `u32`, effective maximum streams `u16`, effective packet MTU `u16` |
| `REJECT` | `0x09` | short UTF-8 diagnostic |

A fresh session starts both byte offsets at zero. A resumable server accepts only the same session ID. `HELLO` and `HELLO_ACK` exchange the next byte each receiver needs, and a sender may advance only within its retained replay interval. A request outside that interval fails rather than silently losing or duplicating bytes.

The effective timeout and stream limit are the smaller values offered by the peers. A server rejects a different session ID as busy until the current session closes or its recovery deadline expires.

## Reliable byte stream

| Type | Value | Body |
| --- | --- | --- |
| `DATA` | `0x04` | starting byte offset `u64`, payload |
| `ACK` | `0x05` | next contiguous byte expected `u64` |
| `PING` | `0x06` | none |
| `PONG` | `0x07` | none |
| `CLOSE` | `0x08` | none |

Each direction is independent. A receiver accepts `DATA` only at its next expected offset, drops duplicate/out-of-order data, and returns a cumulative `ACK`. The sender retains unacknowledged bytes in a 1 MiB replay window and retransmits the oldest outstanding fragment after one second. When either replay or receive storage is full, higher layers block and propagate TCP backpressure.

An idle peer emits `PING` after five seconds. Fifteen seconds without a received packet marks the physical link detached; the resume deadline is measured from the last received packet. BLE connection callbacks and read/write errors may detect detachment sooner.

`CLOSE` ends the logical session. A detached transport does not emit `CLOSE`; it waits for reattachment until the negotiated timeout.

## TCP multiplexing

The resumable byte stream contains frames with this nine-byte header:

```text
TYPE u8 | STREAM_ID u32 | PAYLOAD_LENGTH u32 | PAYLOAD
```

Client-created stream IDs are non-zero odd integers. Payloads are limited to 16 KiB.

| Type | Value | Payload |
| --- | --- | --- |
| `OPEN` | `0x01` | none |
| `OPEN_OK` | `0x02` | none |
| `OPEN_ERROR` | `0x03` | UTF-8 diagnostic, at most 256 bytes |
| `DATA` | `0x04` | TCP bytes, 1–16384 bytes |
| `WINDOW_UPDATE` | `0x05` | consumed byte count `u32` |
| `FIN` | `0x06` | none |
| `RESET` | `0x07` | UTF-8 diagnostic, at most 256 bytes |

`OPEN` asks the server to dial its fixed configured target. The client does not forward data until `OPEN_OK`; target DNS, refusal, and timeout failures produce `OPEN_ERROR`.

Each stream starts with a 64 KiB receive window. `DATA` consumes window space, and application reads return it through `WINDOW_UPDATE`. The sender splits writes into at most 16 KiB frames and schedules at most one frame per ready stream before rotating to another ready stream.

`FIN` closes only the sender's write direction and maps to TCP `CloseWrite`, allowing reverse traffic to continue. `RESET` terminates both directions after an error. Session failure resets every remaining stream.

## Compatibility and security

Protocol changes that alter these fields or state transitions require a new version. Unknown versions are rejected during `HELLO_ID`/`HELLO_ACK`; there is no downgrade negotiation in v1.

The session ID prevents accidental attachment to the wrong retained state but is not an authentication credential. Pairing is not initiated or enforced by this protocol, and the stream is not encrypted at the application layer.
