# Examples

Entrypoints are tagged `//go:build example`: run with
`go run -tags=example ./examples/<name>/` or build all with `make build-examples`.

Upstream Java sources live in the Aeron checkout at the pin in [`specs/versions.yaml`](../specs/versions.yaml).

| Example | Purpose |
|---------|---------|
| [`ipc-echo`](ipc-echo/) | IPC pub/sub with Publication/Subscription wait and offer helpers |
| [`ipc-duty-cycle`](ipc-duty-cycle/) | Long-running IPC invoker: `DoWork` / `Offer` / `Poll` until signal |
| [`cluster-echo-client`](cluster-echo-client/) | IPC cluster echo via `client.Connect` and `SpinOnce` |
| [`cluster-member-service`](cluster-member-service/) | UDP cluster member and echo service on one goroutine via `Runner.RunWithServices` |
| [`cluster-auction-client`](cluster-auction-client/) | UDP auction duty cycle against upstream Java cluster |
