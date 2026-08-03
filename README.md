# LightningBNB

LightningBNB is a resumable TCP bridge over Bluetooth Low Energy (BLE). A client listens on a local TCP port, multiplexes incoming connections over one BLE GATT link, and a server forwards every stream to one operator-selected TCP target.

## Platform support

| Operating system | Client | Server |
| --- | --- | --- |
| Windows 10 22H2 / Windows 11 | Yes | Yes, when the adapter supports the BLE peripheral role |
| Linux with BlueZ 5.55+ | Yes | Yes |
| macOS 13+ | Yes | No |

The program is intended for interactive and modest-rate traffic. BLE throughput depends heavily on the two adapters, negotiated ATT MTU, connection interval, and operating systems; LightningBNB does not promise multi-megabit throughput.

## Security warning

LightningBNB v1 has no application-level authentication or encryption. Pairing is an out-of-band operator prerequisite, but the program does **not** initiate, verify, or enforce pairing and its GATT characteristics do not require an authenticated link.

Use it only between devices you control in a trusted physical environment. The safer defaults bind the client to `127.0.0.1` and let only the server operator select a fixed target, but they do not authenticate a nearby Bluetooth peer.

## Build from source

LightningBNB requires Go 1.25 or newer.

```sh
git clone https://github.com/Sygmei/LightningBNB.git
cd LightningBNB
go build -o lightningbnb ./cmd/lightningbnb
```

Build on the operating system where the binary will run:

- Linux: install BlueZ and ensure the user can access the system D-Bus Bluetooth interfaces. On Debian/Ubuntu, start with `sudo apt install bluez` and verify `bluetoothctl show` works.
- macOS: install Xcode command-line tools with `xcode-select --install`. The terminal application running LightningBNB must have Bluetooth permission under System Settings → Privacy & Security → Bluetooth.
- Windows: use a current Go toolchain. Server mode additionally requires a Bluetooth adapter and driver supporting the BLE peripheral/GATT server role.

## Usage

Pair the two computers with the operating-system Bluetooth tools first, then start the server on the computer that can reach the target.

```sh
# Forward every bridged stream to localhost:8080 on the server.
./lightningbnb server --target-port 8080

# Find the server from the client computer.
./lightningbnb scan --timeout 5s

# Start a client with an interactive server picker.
./lightningbnb client

# Or use the identifier printed by scan (required without an interactive TTY).
./lightningbnb client --device DEVICE_ID
```

For compressible traffic such as HTTP, JSON, and LLM token streams, allow compression on the server and request it on the client:

```sh
./lightningbnb server --target-port 8080 --compression
./lightningbnb client --device DEVICE_ID --compression
```

An uncompressed client can still use a server started with `--compression`. A client requesting compression is rejected explicitly when the server did not enable it.

The client prints its selected endpoint on stdout:

```text
LISTEN_ADDR=127.0.0.1:54321
```

Connect an ordinary TCP application to that address. The port is selected by the operating system unless `--listen-port` is provided.

### Client flags

```text
--listen-host       local listen host (default 127.0.0.1)
--listen-port       local listen port; 0 selects a random port (default 0)
--device            server identifier from `scan`
--scan-timeout      duration of each scan (default 5s)
--resume-timeout    BLE recovery window (default 60s)
--max-connections   active plus waiting TCP connection limit (default 32)
--stats-interval    live traffic stats interval; 0 disables stats (default 1s)
--compression       compress TCP payloads; the server must allow compression
```

### Server flags

```text
--target-host       fixed target hostname, IPv4, or IPv6 address (default localhost)
--target-port       fixed target TCP port (required unless --benchmark)
--name              advertised bridge name (default LightningBNB; Windows may omit it)
--dial-timeout      target connection timeout (default 10s)
--resume-timeout    BLE recovery window (default 60s)
--max-connections   multiplexed TCP connection limit (default 32)
--stats-interval    live traffic stats interval; 0 disables stats (default 1s)
--benchmark         handle in-memory throughput streams instead of a TCP target
--compression       allow clients to negotiate compressed TCP payloads
```

Client and server print process-relative live traffic totals and rates to stderr. `tx` is forwarded TCP payload sent across Bluetooth and `rx` is payload received from Bluetooth; protocol overhead is excluded. For example:

```text
lightningbnb: 2026/08/03 12:00:00 traffic tx=1.5 MiB rx=8.2 MiB tx-rate=24.0 KiB/s rx-rate=96.4 KiB/s
```

## Throughput benchmark

The built-in benchmark saturates the complete TCP-over-BLE path with multiple streams and reports live totals and rates. It needs only one process on each computer.

Start the regular server command in benchmark mode; no TCP target is needed:

```sh
./lightningbnb server --benchmark
```

Then run the benchmark subcommand on the client computer:

```sh
./lightningbnb benchmark --device DEVICE_ID --duration 30s
```

To exercise the compression code path, add `--compression` to both commands. Benchmark data is deliberately high entropy, so compression cannot inflate the reported physical transport rate:

```sh
./lightningbnb server --benchmark --compression
./lightningbnb benchmark --device DEVICE_ID --duration 30s --compression
```

The benchmark uses one stream per direction by default. `--direction upload`, `--direction download`, and `--direction both` measure traffic relative to the benchmark client. `--connections N` sets the number of streams per direction (up to 32 for one-way tests or 16 in each direction for bidirectional tests), and `--stats-interval` changes the live reporting interval. BLE session resumption remains active during the run.

Benchmark totals are receiver-confirmed payload, not bytes merely accepted into local TCP, multiplexing, or replay buffers. Each stream keeps at most 256 KiB of unconfirmed benchmark data in flight and exchanges cumulative acknowledgements every 16 KiB without counting them as payload.

The client prints one-second live rates followed by a final whole-run average, for example:

```text
lightningbnb: 2026/08/03 12:00:30 benchmark result duration=30s tx=3.2 MiB rx=3.0 MiB tx-rate=109.2 KiB/s rx-rate=102.4 KiB/s
```

Use `Ctrl+C` to close the listener, BLE resources, active streams, and target sockets cleanly.

## Reconnection behavior

When BLE disappears, active local and target TCP sockets remain open. New local sockets are accepted up to the connection limit but are not read, so the operating-system TCP buffer supplies backpressure. Both sides retain at most 1 MiB of unacknowledged link data per direction.

If the same in-memory session reconnects before the effective resume timeout (the lower value configured by the two peers), acknowledged offsets are reconciled and only missing bytes are replayed. If recovery expires or either process restarts, all sockets in that session close and the client starts looking for a fresh session.

One server accepts one BLE client at a time. That client may carry up to 32 TCP streams by default.

## Troubleshooting

- No scan results: confirm Bluetooth is enabled, the server is advertising, and the adapter supports BLE. Windows uses the connectable GATT service advertisement directly and may show the server as `(unnamed)` because WinRT does not let this application attach a local name to that advertisement.
- Advertisement diagnosis: run `lightningbnb scan --timeout 15s --all` to list unfiltered BLE advertisements, service UUIDs, service data, and manufacturer data. A Windows LightningBNB server should include `13f0b6a0-4746-4c42-8e2f-1f62e4a0b1a0` in `SERVICE_UUIDS`.
- Linux permission errors: verify BlueZ is running and that the account can use `org.bluez` through the system D-Bus. Distribution policies vary.
- macOS abort or permission errors: grant Bluetooth access to the terminal application, then restart it.
- Server reports unsupported mode on macOS: this is intentional in v1; run the server on Windows or Linux.
- Target connection closes immediately: verify `--target-host`, `--target-port`, firewall rules, and whether the target is listening from the server computer.

The complete version-1 framing and resumption contract is in [docs/protocol.md](docs/protocol.md). Hardware releases should follow the [manual platform checklist](docs/manual-testing.md).

## Development

```sh
go test ./...
go test -race ./...
go vet ./...
```

The test suite uses deterministic fake BLE transports and loopback TCP listeners; Bluetooth hardware is required only for the manual platform matrix.

UDP, a macOS server, GUI/service installation, durable resumption across process restarts, and packaged release artifacts are outside v1.
