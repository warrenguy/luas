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
| Consensus module | `cluster` | Experimental | Snapshots, termination, cluster control, admin requests |
| Service container | `cluster/service` | Experimental | Egress to clients, snapshots, recovery |

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

Resource poll/offer helpers above call client `DoWork` internally. Runnable one-shot
sample: [`examples/ipc-echo`](examples/ipc-echo/).

A long-running application drives the client directly: one goroutine per
`*aeron.Client` calls `DoWork` and the poll methods, while `Publication.Offer`
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

Runnable long-running sample (paced offer, idle, signal stop):
[`examples/ipc-duty-cycle`](examples/ipc-duty-cycle/).

## Cluster client

[`cluster/client`](cluster/client/) connects to an Aeron Cluster:
`Connect`/`AsyncConnect` handle authentication challenges and member redirects;
sessions offer and claim ingress, poll egress with fragment assembly, and
recover across leader changes. An external cluster stack is required:

- [`examples/cluster-echo-client`](examples/cluster-echo-client/) - IPC echo
  against `test/integration/cluster_java/docker`
- [`examples/cluster-auction-client`](examples/cluster-auction-client/) - UDP
  auction duty cycle against the upstream Java cluster

## Archive client

[`archive`](archive/) connects to Aeron Archive for recording control and
replay. Pass an existing transport client in `Config.Aeron` to share one duty
cycle:

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
	transport.DoWork()
	session.Poll()
}
```

Blocking `Connect` and recording APIs drive `DoWork` during awaits; otherwise pump
`transport.DoWork()` then `session.Poll()` (`Poll` never calls `DoWork`). Configure a
recording signal handler or signals drop when control events are polled.

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
