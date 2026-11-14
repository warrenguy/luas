# Luas

[![Go Reference](https://pkg.go.dev/badge/github.com/warrenguy/luas.svg)](https://pkg.go.dev/github.com/warrenguy/luas)

### Pure Go clients for Aeron transport, archive, and cluster

Luas implements [Aeron](https://github.com/aeron-io/aeron)'s shared memory and
wire protocols directly against an external Aeron media driver (C or Java),
targeting behavioural parity with the Java stack. It also includes an
experimental cluster consensus module and service container.

## Status

Measured against the Java stack at the Aeron release pinned in
[`specs/versions.yaml`](specs/versions.yaml).

| Component | Package | Status | Main gaps |
|-----------|---------|--------|-----------|
| Transport client | `aeron` | Functional | Counters, publication MDC destinations, `Image.reject`, block and raw poll, `ChannelUri` builder |
| Archive client | `archive` | Partial | Recording listing, position queries, `ReplayMerge` |
| Cluster client | `cluster/client` | Functional | Controlled egress dispatch, egress listener extensions |
| Consensus module | `cluster` | Experimental | Counters, mark file, backup and standby, `ClusterTool` |
| Service container | `cluster/service` | Experimental | Service messages to the log, mark file, counters |

Detail per component: [STATUS.md](STATUS.md).

## Performance

[luas-benchmarks](https://github.com/warrenguy/luas-benchmarks) compares
Luas with Aeron (Java) and aeron-go against the same C media driver.

- [RESULTS.md](https://github.com/warrenguy/luas-benchmarks/blob/main/RESULTS.md):
  the tables of the latest run.
- [Benchmark report](https://github.com/warrenguy/luas-benchmarks/releases/download/run-20261111T123648Z/luas-benchmarks-20261111T123648Z.pdf)
  (PDF): the method, the latency curves, and the limits.

## Supported platforms

Linux with kernel 5.14 or later, on amd64 or arm64.

Other Unix systems may work, but CI tests only Linux. Other architectures do
not build.

## Getting started

```bash
go get github.com/warrenguy/luas
```

A media driver must be running. Build the pinned C driver and start it:

```bash
make aeronmd                 # requires Docker; output bin/aeronmd
export AERON_DIR=/tmp/aeron
mkdir -p "$AERON_DIR"
bin/aeronmd
```

The Aeron release is pinned in [`specs/versions.yaml`](specs/versions.yaml).

Publish and receive a message over IPC (error handling omitted):

```go
import "github.com/warrenguy/luas/aeron"

ctx := context.Background()
client, err := aeron.Connect(ctx, aeron.Config{AeronDir: "/tmp/aeron"})
if err != nil { /* ... */ }
defer client.Close()

pub, err := client.AddPublication(1001, "aeron:ipc")
if err != nil { /* ... */ }
sub, err := client.AddSubscription(1001, "aeron:ipc")
if err != nil { /* ... */ }

if err := pub.WaitUntilConnected(ctx); err != nil { /* ... */ }
if err := sub.WaitUntilImageCount(ctx, 1); err != nil { /* ... */ }

if _, err := pub.OfferWithRetry(ctx, []byte("hello")); err != nil { /* ... */ }
msg, err := sub.PollOneMessage(ctx)
if err != nil { /* ... */ }
log.Printf("received %q", msg)
```

Connect starts a conductor goroutine. That goroutine loops on `DoWork` and
idles between calls. The sample above uses that default. Resource helpers
call `DoWork` only in invoker mode. Runnable one-shot sample:
[`examples/ipc-echo`](examples/ipc-echo/).

A fatal conductor error, such as a driver timeout, terminates the client.
Set `Config.ErrorHandler` or check `Client.IsTerminating` to see it.

Set `UseConductorInvoker` to true when the application runs the conductor.
One goroutine then calls `DoWork` and the poll methods. `Publication.Offer`
may run from other goroutines.

```go
handler := aeron.AdaptFragmentHandlerBytes(func(payload []byte, _ *aeron.Header) {
	log.Printf("received %q", payload)
})

for {
	client.DoWork()
	sub.Poll(handler, 10)
}
```

Runnable invoker sample (paced offer, idle, signal stop):
[`examples/ipc-duty-cycle`](examples/ipc-duty-cycle/).

## Cluster client

[`cluster/client`](cluster/client/) connects to an Aeron Cluster:
`Connect`/`AsyncConnect` handle authentication challenges and member redirects;
sessions offer and claim ingress, poll egress with fragment assembly, and
recover across leader changes. On a background transport, Connect,
`SpinOnce`, and the offer helpers do not call `DoWork`. On an invoker
transport, the session owner calls `DoWork` or `SpinOnce`. An external
cluster stack is required:

- [`examples/cluster-echo-client`](examples/cluster-echo-client/) - IPC echo
  against `test/integration/cluster_java/docker`
- [`examples/cluster-auction-client`](examples/cluster-auction-client/) - UDP
  auction duty cycle against the upstream Java cluster

## Archive client

[`archive`](archive/) connects to Aeron Archive for recording control and
replay. Pass an existing transport client in `Config.Aeron` to share it.
On a background transport the session owner polls and does not call `DoWork`.
On an invoker transport that owner also calls `DoWork`:

```go
import (
	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/archive"
)

transport, err := aeron.Connect(context.Background(), aeron.Config{AeronDir: "/tmp/aeron"})
// ...
session, err := archive.Connect(context.Background(), archive.Config{Aeron: transport})
// ...
defer session.Close()

for {
	session.Poll()
}
```

Blocking connect and recording awaits call `DoWork` only on an invoker
transport. `Poll` never calls `DoWork`. Set a recording signal handler.
Otherwise the client drops those signals when it polls control events.

## Cluster member

[`cluster`](cluster/) and [`cluster/service`](cluster/service/) run one
member and its services on one goroutine. The consensus module runs its
transport client in invoker mode, as upstream does. Only the runner drives
it.

[`examples/cluster-member-service`](examples/cluster-member-service/) runs
one member and an echo service:

1. `cluster.OpenConsensusModule` opens the consensus module.
2. `service.Open` opens a service container on the module's transport
   client.
3. `cluster.NewRunner` builds a runner from the module's parts.
4. `Runner.RunWithServices` runs the module and the container until the
   member stops.
5. Close the container, then the module.

## Examples

Runnable end-to-end demos live under [`examples/`](examples/), each with its
own README covering prerequisites and run steps.

## Contributing

Build, test, SBE codec, and benchmark workflows: [CONTRIBUTING.md](CONTRIBUTING.md)

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Luas includes material derived from Aeron under the Apache License 2.0; see
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
