# Contributing

Development workflows for this repository. User-facing overview and usage examples live in
[README.md](README.md).

## Building blocks

Importable packages under `github.com/warrenguy/luas/...`.

| Package | Detail |
|---------|--------|
| `atomicbuffer` | `AtomicBuffer` over byte slices; bit and ASCII string helpers |
| `broadcast` | One-to-many broadcast buffer: fixed Agrona wire layout for driver/CnC |
| `idlestrategy` | Agent idle pacing (`IdleStrategy`, backoff, spin/yield/sleep) |
| `mmap` | Unix mmap for driver/CnC shared-memory files |
| `record` | Shared record header constants for ring and broadcast buffers |
| `ringbuffer` | `ManyToOneRingBuffer`: fixed Agrona wire layout for CnC/driver rings |
| `timerwheel` | Agrona `DeadlineTimerWheel`: hashed deadline wheel (O(1) cancel) |
| `wire` | Wire primitives: length checks, fixed-width int64 and bool-byte put/get |
| `cluster` | Consensus module member assembly: configuration, recovery, channel topology, log streams, duty cycle runner |
| `cluster/client` | Cluster client: connect, ingress, egress polling, leader failover |
| `cluster/codecs` | SBE flyweights in `clusterwire/`; see **SBE codecs** below |
| `cluster/consensus` | Consensus agent: log publication and adaptation, client sessions, timers, service acks, catchup and truncation |
| `cluster/election` | Election state machine, cluster members, `node-state.dat`; SBE flyweights in `nodewire/` |
| `cluster/recordinglog` | Recording log (`recording.log`) access and recovery planning |
| `cluster/service` | Clustered service container: service callbacks, log consumption, service agent |
| `archive`, `archive/codecs` | Archive client: connect, recording and replay control, recording signals; SBE flyweights in `archivewire/` - see [archive client](README.md#archive-client) |
| `aeron` | Transport client: driver connection, publications, subscriptions, images |
| `aeron/{conductor,cnc,logbuffer}` | Transport internals: conductor, CnC, logbuffer |

Low-level layout packages (`atomicbuffer`, `broadcast`, `record`, `ringbuffer`, `wire`)
follow Agrona/C driver memory layouts. `ringbuffer` and `broadcast` are layout-coupled
ports, not generic queues; see package godoc for layout and the `ManyToOneRingBuffer`,
`BroadcastTransmitter` and `BroadcastReceiver` godoc for concurrency constraints.

## Examples

Runnable examples live under `examples/`. Importable subpackages (e.g. `auctionwire`) are
included in `make check`; `package main` entrypoints use the `example` build tag. See
[`examples/README.md`](examples/README.md) for run commands.

## Pinned dependencies

SBE tool and Aeron C media driver are pinned in [`specs/versions.yaml`](specs/versions.yaml).

`aeron.commit` is the commit the `aeron.ref` tag resolves to.
`sbe_tool.sha256` is the sha256 of the sbe-all JAR; a fetch with a stale pin prints the actual hash.

Dev tools live in `tools/go.mod`. Dependabot version updates skip them.
`make tools-update` upgrades every tool to latest.
To add a tool or pin a version, run:

```bash
go -C tools get [-tool] <package>@<version>
go -C tools mod tidy
```

Pass `-tool` for a tool package; omit it for any other requirement.

## Tests

`make help` lists every target with a one-line description, including the
contract suites and Docker Compose smokes.

**Contract suites** (`make contract-oracles` and the `*-contract` targets below): run `mvn test`
(Java goldens) then matching Go oracle tests. No aeronmd or Docker.

| Target | Live smoke | What it checks (examples) |
|--------|------------|---------------------------|
| `test-java-interop-contract` | `smoke-java-interop` | streams 1100/1101; 32x32-byte `java-N` / `go-N` payloads |
| `test-cluster-java-contract` | `smoke-cluster-java` / `smoke-cluster-mixed-java-leader` | IPC channels; streams 101/102; ports 9000/9002; failover and mixed topology goldens |
| `test-archive-java-contract` | `smoke-archive-java` | record/replay streams 1200/1201; IPC channels; archive dir layout |
| `test-cluster-sbe-contract` | (none; wire oracle only) | cluster SBE bytes vs `catalog.json` |
| `test-archive-sbe-contract` | (none; wire oracle only) | archive control SBE bytes vs `catalog.json` |

**Compose smokes** (Docker Compose). Image tag: `java-interop-aeronmd`.

| Make target | Script | Media driver |
|-------------|--------|--------------|
| `smoke-java-interop` | `scripts/java-interop-smoke.sh` | Compose image (built by make target) |
| `smoke-cluster-java-failover` | `scripts/cluster-java-failover-smoke.sh` | Compose image (built by make target) |
| `smoke-archive-java` | `scripts/archive-java-smoke.sh` | Compose image (built by make target) |
| `smoke-cluster-mixed-java-leader` | `scripts/cluster-mixed-smoke.sh java-leader` | Compose image (built by make target) |
| `smoke-cluster-mixed-go-leader` | `scripts/cluster-mixed-smoke.sh go-leader` | Compose image (built by make target) |
| `smoke-cluster-mixed-go-leader-unicast` | `scripts/cluster-mixed-smoke.sh go-leader-unicast` | Compose image (built by make target) |
| `smoke-cluster-mixed-ex-leader-rejoin` | `scripts/cluster-mixed-smoke.sh ex-leader-rejoin` | Compose image (built by make target) |
| `smoke-cluster-mixed-java-leader-snapshot` | `scripts/cluster-mixed-smoke.sh java-leader-snapshot` | Compose image (built by make target) |
| `smoke-cluster-mixed-go-leader-snapshot` | `scripts/cluster-mixed-smoke.sh go-leader-snapshot` | Compose image (built by make target) |
| `smoke-cluster-mixed-unappointed-java-majority` | `scripts/cluster-mixed-smoke.sh unappointed-java-majority` | Compose image (built by make target) |
| `smoke-cluster-mixed-unappointed-go-majority` | `scripts/cluster-mixed-smoke.sh unappointed-go-majority` | Compose image (built by make target) |
| `smoke-cluster-java` | `scripts/cluster-java-smoke.sh` | Embedded (no image step) |
| `smoke-cluster-mixed-service` | `scripts/cluster-mixed-service-smoke.sh` | Embedded (no image step) |

Scripts do not build the image or the Java jars. Run `make aeronmd-compose-image java-jars`
first, or the smoke's make target.

**Integration:** `make test-integration` builds `bin/aeronmd` when needed; the harness sets
`AERON_DIR` per test.

**Build tags**

| Kind | Tags / location |
|------|-----------------|
| Unit | `bench`, `testhooks` |
| Property | `rapid` (`*_rapid_test.go`) |
| IDE / gopls (`go.buildTags`) | `testhooks,rapid,stress,integration,example,java_interop,cluster_sbe,cluster_java,archive_java,archive_live,cluster_soak,cluster_mixed,electionsim,replicationsim,bench` |
| Test helper packages | under an internal directory, or tagged `testhooks` |

```bash
make test                                             # unit only
go test -race -tags=bench,testhooks ./...             # ad-hoc unit
make test-rapid                                       # property only
```

Prefer Makefile targets over bare `go test`. `make check-public-packages` compares a default-tag build with `scripts/public-packages.txt`.

## SBE codecs (cluster + archive + node-state)

Pinned Aeron ref: `aeron.ref` in [`specs/versions.yaml`](specs/versions.yaml).

| Area | Spec XML | Flyweights | Upstream (at pin) | Port notes |
|------|----------|------------|-------------------|------------|
| Cluster wire | `specs/cluster-codecs-wire.xml` | `cluster/codecs/clusterwire/` | `aeron-cluster/.../aeron-cluster-codecs.xml` | Copy verbatim |
| Archive control | `specs/archive-codecs-client.xml` | `archive/codecs/archivewire/` | `aeron-archive/.../aeron-archive-codecs.xml` | Subset: control request, response, descriptor, recording-signal, shared types |
| Node-state file | `specs/node-state-codecs.xml` | `cluster/election/nodewire/` | `aeron-cluster/.../aeron-cluster-node-state-codecs.xml` | Keep trimmed message set unless PR expands scope |

Upstream path prefixes under the matching Aeron module:

- Cluster / node-state: `src/main/resources/cluster/`
- Archive: `src/main/resources/archive/`

**Fixture regen**

| Target | What it refreshes | Needs |
|--------|-------------------|-------|
| `make cluster-sbe-catalog-regen` | `hex` from `fields` in `test/integration/cluster_sbe/clustersbe/fixtures/catalog.json` | (none) |
| `make archive-sbe-catalog-regen` | `hex` in `test/integration/archive_sbe/archivesbe/fixtures/catalog.json` from Java encoders | mvn + java |
| `make node-state-fixtures-regen` | committed node-state oracle bytes | mvn + java |
| `make recordinglog-fixtures-regen` | committed recording-log oracle bytes | mvn + java |
| `make recovery-state-key-fixtures-regen` | committed recovery state key oracle bytes | mvn + java |

Java golden fixture layout:
[`test/integration/golden_fixtures/README.md`](test/integration/golden_fixtures/README.md).

**Pin-bump workflow**

1. Bump `aeron.ref` and `aeron.commit` in `specs/versions.yaml`.
2. Diff each upstream XML against the matching Spec XML row above; update per Port notes.
3. Generate flyweights:
   - `make sbe-generate`
   - `make sbe-generate-archive`
   - `make sbe-generate-nodestate`
4. Commit spec XML and flyweight diffs if any.
5. If port notes or subset changed, update the matching entries in
   [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).
6. `make check` (needs java + mvn for SBE stale and contract-oracles) and `make test-rapid`.
7. `make test-cluster-sbe-contract` for cluster wire and
   `make test-archive-sbe-contract` for archive control. Fix failures under:
   - `cluster/codecs/`
   - `test/integration/cluster_sbe/clustersbe/`
   - `archive/codecs/`
   - `test/integration/archive_sbe/archivesbe/`
8. Node-state failures from step 6: fix under
   - `cluster/election/`
   - `cluster/election/nodewire/`
   Use `make check` / `make test-rapid` (not `test-cluster-sbe-contract`).

## Benchmarks

| Target | Tags | Packages |
|--------|------|----------|
| `make bench` (+ baseline, regression) | `bench` | `./test/bench/...` |
| `make bench-micro` | `testhooks` | `BENCH_MICRO_PKGS` in Makefile |

```bash
make bench             # contract suite (quick)
make bench-baseline    # .bench/baseline.txt (gitignored, per machine)
make bench-regression  # vs baseline via benchstat; exit 1 on alloc regression (CI)
make bench-micro       # in-package (informational)
```

Re-baseline after pulling `main` or when perf changes are intentional.

Optional env:

| Variable | Role | Defaults |
|----------|------|----------|
| `BENCH_COUNT` | bench iterations | Makefile: 10 |
| `BENCH_PAYLOAD_SIZES` | comma-separated payload filter | CI: `32,1400` |
| `BENCH_THRESHOLD` | time regression threshold in percent (warning only) | script: 10; CI (`bench.yml`): 30 |
| `SKIP_BENCH_REGRESSION` | skip alloc regression check | unset |

## Devcontainer

VS Code / GitHub Codespaces configs live under `.devcontainer/`. Default (check) covers
`make check`; docker adds Docker-outside-of-Docker for integration and compose smokes. See
[.devcontainer/README.md](.devcontainer/README.md).

## License

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Luas, as defined in the Apache License 2.0, is dual licensed
under the MIT license and the Apache License 2.0, as described in the
[README](README.md#license), without any additional terms or conditions.

This clause is the contribution policy. Contributions do not need a DCO sign-off or a CLA.
