# LightningBNB

LightningBNB is a resumable TCP bridge over Bluetooth Low Energy (BLE). A client listens on one or more local TCP ports, multiplexes incoming connections over one BLE GATT link, and a server forwards each stream to an operator-selected TCP service.

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

The checked-in `third_party/bluetooth-v0.15.0` directory is intentional. It is
the pinned upstream v0.15.0 source plus a documented macOS readiness-callback
fix; the root `go.mod` uses it through a local `replace`, so native builds are
reproducible without silently picking up an unstable Bluetooth API revision.

Build on the operating system where the binary will run:

- Linux: install BlueZ and ensure the user can access the system D-Bus Bluetooth interfaces. On Debian/Ubuntu, start with `sudo apt install bluez` and verify `bluetoothctl show` works. `--prevent-sleep` additionally requires systemd-logind and permission to acquire its sleep and idle inhibitor locks.
- macOS: install Xcode command-line tools with `xcode-select --install`. The terminal application running LightningBNB must have Bluetooth permission under System Settings → Privacy & Security → Bluetooth.
- Windows: use a current Go toolchain. Server mode additionally requires a Bluetooth adapter and driver supporting the BLE peripheral/GATT server role.

## Releases and containers

Pushing a version tag such as `1.0.0` starts the release workflow. It publishes native binaries for Linux, macOS, and Windows to a GitHub Release, and publishes a multi-architecture Linux container image for `amd64` and `arm64` at `ghcr.io/sygmei/lightningbnb`.

The container uses the host Linux Bluetooth stack through system D-Bus. A typical server invocation is:

```sh
docker run --rm --network host \
  -v /run/dbus:/run/dbus:ro \
  ghcr.io/sygmei/lightningbnb:1.0.0 \
  server --target-port 8080
```

## Usage

Pair the two computers with the operating-system Bluetooth tools first, then start the server on the computer that can reach the target.

```sh
# Forward every bridged stream to localhost:8080 on the server.
./lightningbnb server --target-port 8080

# Advertise several server-side TCP services.
./lightningbnb server --service http:1180 --service https:11443

# A service may target any host reachable from the server computer.
./lightningbnb server --service google:google.com:443

# Find the server from the client computer.
./lightningbnb scan --timeout 30s

# Start a client with an interactive server picker.
./lightningbnb client

# In a terminal, use ↑/↓ to select a server, Enter to connect, or q to cancel.
# Results appear in the picker as soon as advertisements are received.

# Or use the identifier printed by scan (required without an interactive TTY).
./lightningbnb client --device DEVICE_ID

# Map local ports to named services. The client may use only a subset.
./lightningbnb client --device DEVICE_ID --service 1180:http --service 11443:https

# A service's server port can be used directly when it was advertised.
./lightningbnb client --device DEVICE_ID --service 1180:1180

# Automatically forward every advertised service on the same local port.
./lightningbnb client --device DEVICE_ID --all-services

# List the services advertised by one server.
./lightningbnb services --device DEVICE_ID
```

For compressible traffic such as HTTP, JSON, and LLM token streams, allow compression on the server and request it on the client:

```sh
./lightningbnb server --target-port 8080 --compression
./lightningbnb client --device DEVICE_ID --compression
```

To keep the server computer awake while the bridge is running:

```sh
./lightningbnb server --target-port 8080 --prevent-sleep
```

An uncompressed client can still use a server started with `--compression`. A client requesting compression is rejected explicitly when the server did not enable it.

The client prints its selected endpoint on stdout:

```text
LISTEN_ADDR=127.0.0.1:54321
```

Connect an ordinary TCP application to that address. The port is selected by the operating system unless `--listen-port` is provided.
With `--service`, the client prints one `LISTEN_ADDR[service]=...` line per local listener. With `--all-services`, it prints one such line for each advertised service and listens on the advertised port.

### Client flags

```text
--listen-host       local listen host (default 127.0.0.1)
--listen-port       local listen port; 0 selects a random port (default 0)
--device            server identifier from `scan`
--scan-timeout      duration of each scan (default 30s)
--resume-timeout    BLE recovery window (default 60s)
--max-connections   active plus waiting TCP connection limit (default 32)
--stats-interval    live traffic stats interval; 0 disables stats (default 1s)
--compression       compress TCP payloads; the server must allow compression
--transport-debug   log reliable-link packet, ACK, retransmission, and send-latency diagnostics
--service           local-port:server-service; repeat for multiple local listeners
--all-services      forward every advertised service on its advertised local port
```

`--all-services` discovers the server's `SERVICE_LIST` after connecting, then
listens on each advertised port locally. Named services are selected by alias;
unnamed services are selected by their advertised numeric port. It
cannot be combined with `--service` or an explicit `--listen-port`.

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
--prevent-sleep     prevent automatic system sleep while the server is running
--skip-ble-checks   skip native BLE adapter capability checks
--no-ble-recovery   disable automatic Windows Bluetooth radio and service recovery
--server-id-file    persistent server ID file (default: OS user configuration directory)
--transport-debug   log reliable-link packet, ACK, retransmission, and send-latency diagnostics
--service           service-name:server-port or service-name:host:port; repeat to advertise multiple targets
```

`NAME:PORT` is the local-target form and forwards to `--target-host` (which
defaults to `localhost`). `NAME:HOST:PORT` overrides the host for that service,
so the server can forward to a non-local target such as
`google:google.com:443`. The host is resolved and contacted by the server;
clients see the service name and advertised target, including the host for
remote targets.

`--prevent-sleep` keeps the system awake for the lifetime of the server process without forcing the display to remain on. The native inhibitor is released on clean shutdown. On Windows, explicit user actions such as selecting Sleep or closing a laptop lid can still suspend the computer.

`--skip-ble-checks` bypasses the Windows adapter capability preflight only. It
does not force advertising when Windows rejects the actual GATT advertisement;
leave it off unless diagnosing a capability-query failure.

On Windows, a GATT advertisement that remains aborted first triggers one
automatic Bluetooth radio off/on cycle. If the next attempt is still aborted,
LightningBNB requests administrator permission once and restarts the Windows
Bluetooth services that own the stale GATT reservation. Both operations briefly
disconnect other Bluetooth devices. Use `--no-ble-recovery` to disable them.

On its first start, the server generates an application-level ID such as `lbnb:6ea4c3db-41bd-4ebf-9712-a8c01ddba387` and stores it in the OS user configuration directory. `scan` reports this stable ID alongside the transient platform Bluetooth ID, and `client --device` accepts either form. Prefer saving the `lbnb:` ID: the client resolves it to the current Bluetooth address after server or Bluetooth restarts. Use `--server-id-file` when running the server under another account or when its configuration must live at a fixed path.

The stable ID is a routing label, not an authentication credential; nearby software can copy it. A server process restart still closes existing TCP streams because resumption state lives only in memory, but a running client can discover the restarted server and establish a fresh bridge session automatically.

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

When stderr is an interactive terminal, live traffic statistics appear in an in-place dashboard instead of adding one log line per interval. Diagnostics are printed above the dashboard without disrupting it. Redirected stderr retains the line-oriented format for logs and scripts; set `NO_COLOR=1` to disable dashboard colors.

The dashboard includes a link-health dot: green `● link HEALTHY` means the BLE
binding is receiving traffic or heartbeat responses, amber `● link CHECKING`
means a heartbeat is awaiting confirmation, and red `● link OFFLINE` means the
session is currently being rebound or has not connected yet. The same row shows
the local time of the last received heartbeat (`hb last HH:MM:SS`), or `--`
before the first heartbeat response.

Its `BUF` row reports queued requests, active requests/streams, and the bytes
currently retained in the resumable link buffers. Queued client sockets are not
read until the link is ready, so this row makes backpressure visible.

When compression is negotiated, the dashboard adds TX and RX rows comparing the original DATA payload totals with their encoded compressed totals and the resulting percentage saved. These compression totals include the one-byte DATA encoding marker, but exclude multiplexing, link, GATT, Bluetooth, and retransmission overhead.

Benchmark totals are receiver-confirmed payload, not bytes merely accepted into local TCP, multiplexing, or replay buffers. Each stream keeps at most 256 KiB of unconfirmed benchmark data in flight and exchanges cumulative acknowledgements every 16 KiB without counting them as payload. Because the two dashboards use independent one-second sampling clocks, a receiver can briefly show a higher peak or lead its sender by the still-unconfirmed portion of that window; compare the whole-run average for throughput.

The benchmark also prints a final whole-run result; redirected logs retain that line along with the interval reports:

```text
lightningbnb: 2026/08/03 12:00:30 benchmark result duration=30s tx=3.2 MiB rx=3.0 MiB tx-rate=109.2 KiB/s rx-rate=102.4 KiB/s
```

Use `Ctrl+C` to close the listener, BLE resources, active streams, and target sockets cleanly.

## Reconnection behavior

When BLE disappears, active local and target TCP sockets remain open. New local sockets are accepted up to the connection limit but are not read, so the operating-system TCP buffer supplies backpressure. Both sides retain at most 1 MiB of unacknowledged link data per direction.

If the same in-memory session reconnects before the effective resume timeout (the lower value configured by the two peers), acknowledged offsets are reconciled and only missing bytes are replayed. If recovery expires or either process restarts, all sockets in that session close and the client starts looking for a fresh session.

The link performs an active liveness check independently of application traffic:
a `PING` is sent every three seconds, each probe allows two seconds for a
response, and two consecutive missed probes detach the BLE binding. A blocked
heartbeat write uses the same two-second deadline. The client and server then
reuse their normal discovery/advertising loops to rebind the same session.
This is a transport rebind, not a process restart;
the configured resume timeout still controls how long queued and active TCP
connections are retained. Transport diagnostics expose `hb-tx`, `hb-rx`, and
`hb-fail` counters when `--transport-debug` is enabled.

One server accepts one BLE client at a time. That client may carry up to 32 TCP streams by default.

## Troubleshooting

- No scan results: confirm Bluetooth is enabled, the server is advertising, and the adapter supports BLE. Windows uses the connectable GATT service advertisement directly and may show the server as `(unnamed)` because WinRT does not let this application attach a local name to that advertisement.
- Advertisement diagnosis: run `lightningbnb scan --timeout 15s --all` to list unfiltered BLE advertisements, service UUIDs, service data, and manufacturer data. A Windows LightningBNB server should include `13f0b6a0-4746-4c42-8e2f-1f62e4a0b1a0` in `SERVICE_UUIDS`. Interactive client discovery prints each recognized server as soon as it arrives.
- Windows reports that GATT advertising remained aborted: close other Bluetooth peripheral/advertising applications and retry. LightningBNB waits through WinRT's transient `Aborted` state and retries genuine startup failures automatically; `--skip-ble-checks` does not bypass this native failure.
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

UDP, a macOS server, GUI/service installation, and durable resumption across process restarts are outside v1.
