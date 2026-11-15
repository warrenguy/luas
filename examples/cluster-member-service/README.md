# cluster-member-service

Runs a one-member cluster with an echo service. `Runner.RunWithServices`
drives the member and the service on one goroutine.

The service sends each message back to the client that sent it. When the reply
fails, the service logs the result and drops the reply.

## Prerequisites

- `bin/aeronmd` (`make aeronmd` needs Docker) or an existing media driver
- Java 26 or later, to run the Archive node
- Docker, to build `bin/luas-archive-java.jar` (`make bin/luas-archive-java.jar`)
- Go toolchain matching `go.mod`

## Run

Run steps 1 to 3 in separate terminals from the repo root. Run the echo client
in a fourth.

### 1. Media driver

```bash
export AERON_DIR=/tmp/aeron
mkdir -p "$AERON_DIR"
bin/aeronmd
```

### 2. Archive

The member reaches the Archive over IPC, on
`archive.DefaultLocalControlChannel` (`aeron:ipc?term-length=64k`). The
Archive node always listens on that channel.

```bash
export AERON_DIR=/tmp/aeron
export LUAS_ARCHIVE_DIR=/tmp/luas-archive
mkdir -p "$LUAS_ARCHIVE_DIR"
java \
  --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
  -cp bin/luas-archive-java.jar luas.archivejava.ArchiveNode
```

### 3. Member

```bash
export AERON_DIR=/tmp/aeron
go run -tags=example ./examples/cluster-member-service/ \
  -port-base 9000 \
  -cluster-dir /tmp/luas-cluster-member
```

The member takes client ingress on `-port-base` plus 2
(`cluster.ClientFacingPortOffset`). With port base 9000, that is
`localhost:9002`.

Press Ctrl-C or send `SIGTERM` to stop the member. A second signal ends the
process if the stop hangs. A signal stops this member only. To stop the whole
cluster, call `ConsensusModule.RequestClusterShutdown` on the leader.

Run again with the same `-cluster-dir` to resume from the log. Leave a gap
first. The driver holds the member's ports for a moment after it stops, and an
early restart fails with `Address already in use`. Wait 2 seconds after a
clean stop, or 15 seconds after `kill -9`.

### 4. Echo client

Start the client once the member logs `role LEADER`. The client uses the
member's `bin/aeronmd` through `AERON_DIR`.

```bash
export AERON_DIR=/tmp/aeron
export CLUSTER_INGRESS_CHANNEL=aeron:udp
export CLUSTER_INGRESS_ENDPOINTS=0=localhost:9002
export CLUSTER_EGRESS_CHANNEL='aeron:udp?endpoint=localhost:0'
go run -tags=example ./examples/cluster-echo-client/
```

## Expected output

The member logs to stderr, with a timestamp prefix from `log`.

```
cluster-member-service: member 0 ingress localhost:9002 cluster-dir /tmp/luas-cluster-member
cluster-member-service: running
cluster-member-service: start
cluster-member-service: role LEADER
cluster-member-service: echo "luas-cluster-echo-ping"
cluster-member-service: stopped: context canceled
cluster-member-service: terminate
```

The member logs each role change, so `role FOLLOWER` can come before
`role LEADER`. The echo client resends the ping every 250 ms until the echo
arrives, so the member can log `echo` more than once.

The echo client prints to stdout:

```
cluster-echo-client: echo ok ("luas-cluster-echo-ping")
```

## Start fresh

Stop the member and the Archive, and delete both directories. Then start the
Archive and the member again.

```bash
rm -rf /tmp/luas-cluster-member /tmp/luas-archive
```

## Flags

| Flag | Default | Notes |
|------|---------|-------|
| `-port-base` | `9000` | First port of member 0. Ingress is this value plus 2. |
| `-cluster-dir` | `/tmp/luas-cluster-member` | Recording log and node state. Kept across runs. |

## Env

| Var | Default | Notes |
|-----|---------|-------|
| `AERON_DIR` | `/tmp/aeron` | Must match `bin/aeronmd`, the Archive, and the echo client |
