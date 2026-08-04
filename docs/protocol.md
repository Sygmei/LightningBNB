# LightningBNB protocol version 1

All multi-byte integers use network byte order (big endian). Receivers reject malformed lengths, invalid stream state, unavailable replay offsets, and unsupported protocol versions.

## GATT service

| Purpose | UUID | Properties |
| --- | --- | --- |
| Service | `13f0b6a0-4746-4c42-8e2f-1f62e4a0b1a0` | Primary service |
| RX (client → server) | `13f0b6a1-4746-4c42-8e2f-1f62e4a0b1a0` | Write without response; write with response is also exposed for compatibility |
| TX (server → client) | `13f0b6a2-4746-4c42-8e2f-1f62e4a0b1a0` | Notify |
| Server identity | `13f0b6a3-4746-4c42-8e2f-1f62e4a0b1a0` | Read |

Linux advertises the service UUID and local name. Windows advertises the connectable GATT service directly through WinRT. Windows may omit the configured local name, in which case scanners display `(unnamed)` and the platform-specific device identifier remains the selection key. LightningBNB deliberately does not publish a second manufacturer-data advertisement because WinRT's generic advertisement publisher is not the connectable GATT service.

The reliable link protocol supplies offsets, cumulative acknowledgements, replay, and retransmission above GATT. Clients therefore use ATT write commands on RX to keep multiple packets moving instead of waiting for an ATT response after every packet.

The negotiated packet size is the smaller peer limit, capped at 244 bytes. Bootstrap packets are no larger than the 20-byte minimum ATT value size. Empty and oversized packets are invalid where a payload is required.

The server identity characteristic contains a persistent random 128-bit application identifier. CLIs render it as `lbnb:<uuid>` and resolve it to the platform's current scan identifier before connecting. It is deliberately public and provides stable discovery, not authentication. Older servers without this characteristic remain reachable through their platform identifier.

## Session handshake

The client creates a random 128-bit session ID with `crypto/rand`. State exists only in memory.

| Type | Value | Body |
| --- | --- | --- |
| `HELLO_ID` | `0x01` | protocol version `u8`, session ID `[16]byte` |
| `HELLO` | `0x02` | next server byte expected `u64`, resume timeout milliseconds `u32`, maximum streams `u16`, packet MTU `u16`, optional capabilities `u8` |
| `HELLO_ACK` | `0x03` | protocol version `u8`, next client byte expected `u64`, effective timeout milliseconds `u32`, effective maximum streams `u16`, effective packet MTU `u16`, optional capabilities `u8` |
| `REJECT` | `0x09` | short UTF-8 diagnostic |

A fresh session starts both byte offsets at zero. A resumable server accepts only the same session ID. `HELLO` and `HELLO_ACK` exchange the next byte each receiver needs, and a sender may advance only within its retained replay interval. A request outside that interval fails rather than silently losing or duplicating bytes.

The effective timeout and stream limit are the smaller values offered by the peers. A server rejects a different session ID as busy until the current session closes or its recovery deadline expires.

The optional capability byte is omitted when zero, preserving the original bootstrap lengths. Bit `0x01` selects per-frame DEFLATE compression. A client includes it only with `--compression`; a server echoes it only when compression was enabled and otherwise rejects the request. Resumption requires the same compression setting as the original session.

## Reliable byte stream

| Type | Value | Body |
| --- | --- | --- |
| `DATA` | `0x04` | starting byte offset `u64`, payload |
| `ACK` | `0x05` | next contiguous byte expected `u64` |
| `PING` | `0x06` | none |
| `PONG` | `0x07` | none |
| `CLOSE` | `0x08` | none |

Each direction is independent. A receiver accepts `DATA` only at its next expected offset, drops duplicate/out-of-order data, and returns a cumulative `ACK`. Under continuous traffic, ACKs are coalesced after up to eight packets or 40 milliseconds; gaps, duplicates, and receive pressure trigger an immediate ACK. A sender keeps at most the same eight MTU-sized `DATA` packets in flight before waiting for acknowledgement, ensuring the reverse GATT path gets airtime. It retains unacknowledged data in a 1 MiB replay window and restarts from the oldest outstanding fragment after one second or three duplicate acknowledgements. While connected, new writes are limited to a 16 KiB live window so multiplexing control frames cannot sit behind a long bulk-data train. Offline buffering can still use the full replay window.

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

`OPEN` normally asks the server to dial its fixed configured target. The client does not forward data until `OPEN_OK`; target DNS, refusal, and timeout failures produce `OPEN_ERROR`. A server started with `--benchmark` instead approves each `OPEN` as an in-memory benchmark stream and never dials a TCP target.

Each stream starts with a 64 KiB receive window. `DATA` consumes window space, and application reads return it through `WINDOW_UPDATE`. The sender splits writes into at most 16 KiB frames and schedules at most one frame per ready stream before rotating to another ready stream.

With compression negotiated, each `DATA` payload starts with an encoding byte: `0` carries raw bytes and `1` carries a DEFLATE stream. Uncompressed frame chunks are limited to 16383 bytes so the marker still fits the 16 KiB frame bound. Senders keep data raw unless compression makes the frame smaller. Receivers bound decompression to 16383 bytes and apply stream windows to the uncompressed size. `WINDOW_UPDATE` frames are accumulated in 8 KiB increments.

`FIN` closes only the sender's write direction and maps to TCP `CloseWrite`, allowing reverse traffic to continue. `RESET` terminates both directions after an error. Session failure resets every remaining stream.

## Compatibility and security

Protocol changes that alter these fields or state transitions require a new version. Unknown versions are rejected during `HELLO_ID`/`HELLO_ACK`; there is no downgrade negotiation in v1.

The session ID prevents accidental attachment to the wrong retained state but is not an authentication credential. Pairing is not initiated or enforced by this protocol, and the stream is not encrypted at the application layer.

## Benchmark TCP protocol

The diagnostic `benchmark` client and a server running with `--benchmark` communicate through ordinary multiplexed streams. The server handles these streams in memory instead of dialing a TCP target. Each stream starts with:

1. Eight ASCII bytes `LBNBBEN1`.
2. One direction byte: `1` for upload or `2` for download, relative to the benchmark client. Bidirectional tests open separate upload and download streams.
3. One readiness byte with value `1`, returned by the benchmark server after it accepts the header.
4. One start byte with value `1`, sent after every parallel stream returns readiness.
5. Unframed payload in 16 KiB blocks in the selected direction.
6. Eight-byte big-endian cumulative acknowledgements in the reverse direction.

Each sender limits unacknowledged benchmark payload to 256 KiB. TX counters advance as soon as cumulative acknowledgements arrive, while RX counters advance as payload is delivered. Acknowledgements cover 16 KiB blocks. At an instantaneous sample or at the benchmark deadline, the receiver may therefore lead the sender by the unconfirmed portion of the window; the two processes also sample peaks on independent clocks. Benchmark payload is deterministic high-entropy data so compression does not distort the physical transport measurement; counters exclude handshake and acknowledgement bytes.
