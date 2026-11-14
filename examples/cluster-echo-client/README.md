# cluster-echo-client

Minimal cluster client: `client.Connect`, ingress offer, `SpinOnce` until Java
`EchoService` echoes the message. Default topology is IPC against a single-node
`test/integration/cluster_java/docker` cluster.

## Prerequisites

- Docker (build cluster image from this repo)
- Go toolchain matching `go.mod`
- Ports free if also running other cluster stacks (default member 0 uses port base 9000)

## Driver model (IPC default)

Java `ClusterNode` embeds a MediaDriver at `AERON_DIR`. The Go client joins that
driver; do not start a separate `bin/aeronmd` for IPC. Both processes must share
the same absolute path (e.g. `/dev/shm/aeron` on host and in the container).

IPC term length is `64k` in channel URIs (`aeron:ipc?term-length=64k`). Set
`AERON_IPC_TERM_BUFFER_LENGTH=65536` on the Java cluster container (`entrypoint-cluster.sh`).
The Go client does not read this env var; term length comes from the channel URI.

For UDP multi-node clusters with a separate Go `aeronmd`, see
[`examples/cluster-auction-client`](../cluster-auction-client/).

## Run end-to-end

### 1. Start Java cluster (host-visible driver dir)

From repo root:

```bash
make bin/luas-cluster-java.jar
LUAS_COMPOSE_UID=$(id -u) LUAS_COMPOSE_GID=$(id -g) \
docker compose \
  -f test/integration/cluster_java/docker/compose.yaml \
  -f test/integration/cluster_java/docker/compose.host.yaml \
  up --build -d --wait cluster
```

### 2. Run Go client

From repo root:

```bash
export AERON_DIR=/dev/shm/aeron
go run -tags=example ./examples/cluster-echo-client/
```

Or compile and run:

```bash
export AERON_DIR=/dev/shm/aeron
make build-examples
bin/cluster-echo-client
```

`main.go` defaults to `/dev/shm/aeron` when `AERON_DIR` is unset; the host path must
match the container.

### 3. Stop cluster

```bash
docker compose \
  -f test/integration/cluster_java/docker/compose.yaml \
  -f test/integration/cluster_java/docker/compose.host.yaml \
  down
```

## Expected output

Status (stderr; `log` adds a timestamp prefix):

```
2026/07/06 15:04:05 cluster-echo-client: offered ping at position <n>
```

Payload (stdout):

```
cluster-echo-client: echo ok ("luas-cluster-echo-ping")
```

## Env

| Var | Default | Notes |
|-----|---------|-------|
| `AERON_DIR` | `/dev/shm/aeron` | Same absolute path on host and Java container |
| `AERON_TERM_BUFFER_LENGTH` | `65536` on cluster container | Required on Java cluster (`entrypoint-cluster.sh`) |
| `AERON_IPC_TERM_BUFFER_LENGTH` | `65536` on cluster container | Required on Java cluster; not read by Go client |

## Cluster channels

| Field | Value |
|-------|-------|
| Ingress | `aeron:ipc?term-length=64k` |
| Egress | `aeron:ipc?term-length=64k` |
| Ingress endpoints | *(empty; IPC passthrough)* |
| Ingress stream | 101 |
| Egress stream | 102 |
