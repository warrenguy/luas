# ipc-duty-cycle

Long-running IPC pub/sub driven by an explicit invoker loop: `Client.DoWork`,
`Publication.Offer`, and `Subscription.Poll` (no resource wait/offer helpers).
`Config.UseConductorInvoker` makes `DoWork` run the conductor on the loop goroutine.
Offers a sequenced payload about once per second until Ctrl-C / SIGTERM or
media-driver loss (`IsTerminating` after `DoWork`; `ErrorHandler` logs faults).

For the one-shot helper path, see [`../ipc-echo`](../ipc-echo/).

## Prerequisites

- Go toolchain matching `go.mod`
- Docker (for `make aeronmd`) or an existing `bin/aeronmd`

## Run

From the repo root:

```bash
export AERON_DIR=/tmp/aeron
mkdir -p "$AERON_DIR"
make aeronmd
```

Skip `make aeronmd` if `bin/aeronmd` already exists from prerequisites.

```bash
export AERON_DIR=/tmp/aeron
mkdir -p "$AERON_DIR"
bin/aeronmd
```

In another terminal, from the repo root:

```bash
export AERON_DIR=/tmp/aeron
go run -tags=example ./examples/ipc-duty-cycle/
```

Or compile and run:

```bash
export AERON_DIR=/tmp/aeron
make build-examples
bin/ipc-duty-cycle
```

Stop with Ctrl-C (or `SIGTERM`), or by shutting down `aeronmd` (the sample
cancels the loop; IPC Offer/Poll can still succeed on mapped buffers until then).

## Expected output

Status (stderr; `log` adds a timestamp prefix). Offers and receives continue
until stop:

```
2026/07/06 15:04:05 ipc-duty-cycle: running (Ctrl-C or driver loss to stop)
2026/07/06 15:04:05 ipc-duty-cycle: offered "hello-0" at position <n>
2026/07/06 15:04:05 ipc-duty-cycle: received "hello-0" (n=1)
2026/07/06 15:04:06 ipc-duty-cycle: offered "hello-1" at position <n>
2026/07/06 15:04:06 ipc-duty-cycle: received "hello-1" (n=2)
...
2026/07/06 15:04:10 ipc-duty-cycle: stop (context canceled); offered=5 received=5
```

## Env

| Var | Default | Notes |
|-----|---------|-------|
| `AERON_DIR` | `/tmp/aeron` | Must match running `aeronmd` |
