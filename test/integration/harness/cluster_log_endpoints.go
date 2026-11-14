//go:build integration

package harness

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/warrenguy/luas/cluster"
)

// Multicast endpoints for live cluster-log tests.
//
// Each Test* that opens a member set must own a distinct PortBase. A set on a
// multicast log must also own a distinct log multicast endpoint. Two tests
// on one group address and port see each other's images. The driver joins
// the group per interface, so only the port separates them. Add a constant
// rather than reusing one.
//
// Port formula: PortBase + memberID*100 + offset (offsets 1..5). Three members
// reserve base+1 .. base+205. Adjacent three-member bases sit 300 apart so
// ranges never overlap. Unique bases make t.Parallel safe inside the package.
// Register each block in multicastPortLedger or unicastPortLedger.
// TestClusterPortBlocks_doNotOverlap asserts the ranges stay disjoint.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing it and rejects an even address at
// publication add.
const (
	ClusterLogMulticastGroup   = "239.255.12.1"
	ClusterLogNLTMulticastPort = 24404
)

// Unicast PortBase values for test/integration. Each live Test* that binds
// unicast owns one block. Two tests must not share a PortBase under t.Parallel.
//
// Member counts:
//   - ConsensusPublications: three members (base+1 .. base+205)
//   - ClusterStreams*: two members (base+1 .. base+105)
//
// Single-endpoint UDP pubs and subs (channel status, multi-destination) call
// FreeUDPPort instead of fixed ports so parallel hosts do not collide.
const (
	ConsensusPublicationsPortBase   = 24000
	ClusterStreamsMulticastPortBase = 23700
	ClusterStreamsCommitPortBase    = 24500
	ClusterStreamsUnicastPortBase   = 27800
)

// ClusterStreamsMulticastLogEndpoint is derived from the cluster-log multicast
// constants so the port ledger and the live bind cannot drift.
var ClusterStreamsMulticastLogEndpoint = ClusterLogMulticastGroup + ":" +
	strconv.Itoa(ClusterLogNLTMulticastPort)

// ClusterJavaPortBase is the default port base every cluster_java fixture
// binds (single node, failover, mixed). Restated in harness so the port matrix
// builds under the integration tag alone, where the clusterjava package does
// not compile. The clusterjava package asserts these against its own topology
// constants.
const (
	ClusterJavaPortBase = cluster.DefaultPortBase

	// ClusterJavaMemberCount is the widest cluster_java fixture. Failover and
	// mixed both run three members. The single-node fixture runs one.
	ClusterJavaMemberCount = 3
)

// ClusterJavaMixedLogEndpoint is derived from ClusterJavaPortBase so the
// ledger and the live bind cannot drift. Only the group address is
// cluster_java's own. Only the mixed go-leader fixture binds this endpoint.
var ClusterJavaMixedLogEndpoint = "239.255.12.15:" + strconv.Itoa(
	cluster.CalculatePort(0, ClusterJavaPortBase, cluster.LogPortOffset),
)

// Three-node module-test endpoints. Each Test* owns one block so the suite may
// run those tests in parallel. Fixtures that never bind (config reject, hook
// stubs) reuse the elect block.
//
// The final octet of the group address must be odd.
const (
	ClusterModuleElectPortBase           = 25000
	ClusterModuleElectLogEndpoint        = "239.255.12.7:25004"
	ClusterModuleCommitPortBase          = 28200
	ClusterModuleCommitLogEndpoint       = "239.255.12.31:28204"
	ClusterModuleRejoinPortBase          = 28500
	ClusterModuleRejoinLogEndpoint       = "239.255.12.33:28504"
	ClusterModuleReElectPortBase         = 28800
	ClusterModuleReElectLogEndpoint      = "239.255.12.35:28804"
	ClusterModuleFollowerStopPortBase    = 29100
	ClusterModuleFollowerStopLogEndpoint = "239.255.12.37:29104"
	ClusterModuleImageLossPortBase       = 21300
	ClusterModuleImageLossLogEndpoint    = "239.255.12.73:21304"
)

// One-member cluster-log replay restart endpoints. Reserves the consensus port
// block ClusterReplayPortBase+1 .. ClusterReplayPortBase+(memberCount-1)*100+5.
// The shared cluster log uses the multicast endpoint below.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterReplayPortBase    = 25700
	ClusterReplayLogEndpoint = "239.255.12.17:25704"
)

// Durable live-Archive scenario endpoints. Each pair owns one consensus port
// block base+1 .. base+(memberCount-1)*100+5 and one shared cluster-log
// multicast endpoint. Default durable sets use three members
// (base+1 .. base+205). DurableConcurrentCatchup uses five
// (base+1 .. base+405).
//
// DurableRecord and DurableRank are separate so the two record/rank tests may
// run in parallel. DurableRestart, DurableRestartLeader, and
// DurableRestartRunning are separate for the three restart tests.
// DurableCatchup and DurableConcurrentCatchup are separate for the
// single-follower and concurrent-follower catchup tests.
// DurableDivergence and DurableExLeaderReElect are separate for the truncate
// and ex-leader re-election tests. DurableReplicate and
// DurableReplicateTruncate are separate for the two ex-leader replication
// tests.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	DurableRecordPortBase               = 26000
	DurableRecordLogEndpoint            = "239.255.12.19:26004"
	DurableRankPortBase                 = 29400
	DurableRankLogEndpoint              = "239.255.12.39:29404"
	DurableRestartPortBase              = 26300
	DurableRestartLogEndpoint           = "239.255.12.21:26304"
	DurableRestartLeaderPortBase        = 29700
	DurableRestartLeaderLogEndpoint     = "239.255.12.41:29704"
	DurableRestartRunningPortBase       = 22200
	DurableRestartRunningLogEndpoint    = "239.255.12.65:22204"
	DurableReElectionPortBase           = 26600
	DurableReElectionLogEndpoint        = "239.255.12.23:26604"
	DurableCatchupPortBase              = 26900
	DurableCatchupLogEndpoint           = "239.255.12.25:26904"
	DurableDivergencePortBase           = 27200
	DurableDivergenceLogEndpoint        = "239.255.12.27:27204"
	DurableExLeaderReElectPortBase      = 22500
	DurableExLeaderReElectLogEndpoint   = "239.255.12.63:22504"
	DurableReplicatePortBase            = 19800
	DurableReplicateLogEndpoint         = "239.255.12.79:19804"
	DurableReplicateTruncatePortBase    = 19500
	DurableReplicateTruncateLogEndpoint = "239.255.12.81:19504"
	DurableConcurrentCatchupPortBase    = 30400
	DurableConcurrentCatchupLogEndpoint = "239.255.12.43:30404"
)

// Durable unicast-log scenario port bases. Each member receives the log at
// its own log port inside the three-member block.
const (
	DurableUnicastCatchupPortBase  = 15000
	DurableUnicastExLeaderPortBase = 15300
)

// Process cluster smoke endpoints. The pair reserves one three-member
// consensus port block and one shared cluster-log multicast endpoint.
//
// The final octet of the group address must be odd.
const (
	ProcessClusterSmokePortBase    = 15600
	ProcessClusterSmokeLogEndpoint = "239.255.12.107:15604"
)

// Service-stop endpoints. The pair reserves one three-member consensus port
// block and one shared cluster-log multicast endpoint.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterServiceStopPortBase    = 16200
	ClusterServiceStopLogEndpoint = "239.255.12.105:16204"
)

// Live soak endpoints. The pair reserves one three-member consensus port
// block and one shared cluster-log multicast endpoint.
//
// The final octet of the group address must be odd.
const (
	LiveSoakPortBase    = 16800
	LiveSoakLogEndpoint = "239.255.12.103:16804"
)

// Cluster termination endpoints. The pair reserves one three-member consensus
// port block and one shared cluster-log multicast endpoint.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterTerminationPortBase    = 17100
	ClusterTerminationLogEndpoint = "239.255.12.101:17104"
)

// Linearizability endpoints. The pair reserves one three-member consensus
// port block and one shared cluster-log multicast endpoint.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterLinearizabilityPortBase    = 17400
	ClusterLinearizabilityLogEndpoint = "239.255.12.97:17404"
)

// Crash restart endpoints. Each pair reserves one three-member consensus port
// block and one shared cluster-log multicast endpoint.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	CrashRestartLeaderPortBase      = 17700
	CrashRestartLeaderLogEndpoint   = "239.255.12.95:17704"
	CrashRestartFollowerPortBase    = 18000
	CrashRestartFollowerLogEndpoint = "239.255.12.93:18004"
)

// Cluster control endpoints. The pair reserves one three-member consensus
// port block and one shared cluster-log multicast endpoint.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterControlPortBase    = 18300
	ClusterControlLogEndpoint = "239.255.12.91:18304"
)

// Multi-service snapshot retention endpoints. The pair reserves one
// three-member consensus port block and one shared cluster-log multicast
// endpoint.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterMultiServiceSnapshotRetentionPortBase    = 18600
	ClusterMultiServiceSnapshotRetentionLogEndpoint = "239.255.12.89:18604"
)

// Java cluster client endpoints. The pair reserves one three-member consensus
// port block and one shared cluster-log multicast endpoint.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterJavaClientPortBase    = 18900
	ClusterJavaClientLogEndpoint = "239.255.12.5:18904"
)

// Session replication ex-leader endpoints. The pair reserves one three-member
// consensus port block and one shared cluster-log multicast endpoint.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterSessionReplicateExLeaderPortBase    = 19200
	ClusterSessionReplicateExLeaderLogEndpoint = "239.255.12.83:19204"
)

// Session-client endpoints. Each pair owns one three-member consensus port
// block and one shared cluster-log multicast endpoint. Smoke and reject are
// separate so the two session tests may run in parallel.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterSessionSmokePortBase     = 30900
	ClusterSessionSmokeLogEndpoint  = "239.255.12.45:30904"
	ClusterSessionRejectPortBase    = 31200
	ClusterSessionRejectLogEndpoint = "239.255.12.47:31204"
)

// Stop/restart amplifier endpoints. Two members reserve one consensus port
// block and one shared cluster-log multicast endpoint.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterMemberAmplifierPortBase    = 31500
	ClusterMemberAmplifierLogEndpoint = "239.255.12.49:31504"
)

// Live service-ack endpoints. Three members reserve one consensus port block
// and one shared cluster-log multicast endpoint.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterServiceAckPortBase    = 31800
	ClusterServiceAckLogEndpoint = "239.255.12.51:31804"
)

// Session-extend live-scenario endpoints. Each pair reserves one three-member
// consensus port block and one shared cluster-log multicast endpoint. Restart
// and re-elect are separate so the two scenarios may run in parallel.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterSessionExtendRestartPortBase    = 32100
	ClusterSessionExtendRestartLogEndpoint = "239.255.12.53:32104"
	ClusterSessionExtendReElectPortBase    = 32400
	ClusterSessionExtendReElectLogEndpoint = "239.255.12.55:32404"
)

// Multi-service, session-transfer, and service-transfer live-scenario
// endpoints. Each pair reserves one three-member consensus port block and one
// shared cluster-log multicast endpoint. The three scenarios are separate so
// they may run in parallel.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterMultiServicePortBase       = 22800
	ClusterMultiServiceLogEndpoint    = "239.255.12.57:22804"
	ClusterSessionTransferPortBase    = 23100
	ClusterSessionTransferLogEndpoint = "239.255.12.59:23104"
	ClusterServiceTransferPortBase    = 23400
	ClusterServiceTransferLogEndpoint = "239.255.12.61:23404"
)

// Service restart live-scenario endpoints. Each pair reserves one three-member
// consensus port block and one shared cluster-log multicast endpoint. The two
// scenarios are separate so they may run in parallel.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterServiceRestartPortBase    = 21600
	ClusterServiceRestartLogEndpoint = "239.255.12.67:21604"
	ClusterServiceRebuildPortBase    = 21900
	ClusterServiceRebuildLogEndpoint = "239.255.12.69:21904"
)

// Service egress leader-change endpoints. The pair reserves one three-member
// consensus port block and one shared cluster-log multicast endpoint.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterServiceEgressPortBase    = 20100
	ClusterServiceEgressLogEndpoint = "239.255.12.77:20104"
)

// Service snapshot restart endpoints. The pair reserves one three-member
// consensus port block and one shared cluster-log multicast endpoint.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterServiceSnapshotRestartPortBase    = 20400
	ClusterServiceSnapshotRestartLogEndpoint = "239.255.12.83:20404"
)

// Multi-service snapshot restart and restored-session egress each reserve one
// three-member consensus port block and one shared cluster-log multicast
// endpoint.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterMultiServiceSnapshotRestartPortBase    = 20700
	ClusterMultiServiceSnapshotRestartLogEndpoint = "239.255.12.85:20704"
	ClusterServiceRestoredEgressPortBase          = 21000
	ClusterServiceRestoredEgressLogEndpoint       = "239.255.12.87:21004"
)

// cluster_archive one-off and smoke endpoints. Harness holds one matrix for
// every PortBase and log multicast the cluster_archive package uses under
// t.Parallel. Sibling packages register their blocks in the same ledgers.
// Those packages may run in parallel go test processes.
//
// TestClusterPortBlocks_doNotOverlap registers every reservation. New
// cluster_archive bases must end below linuxEphemeralPortFloor.
//
// Member counts for reserved unicast ranges:
//   - consensus, runner, runner archive loss, runner member service,
//     runner service start failure, service snapshot, and module snapshot:
//     one member (base+1..+5)
//   - member-set smoke and restart affordance: two members (base+1..+105)
//   - truncate: three members (base+1..+205)
//
// Archive-node control URIs are single UDP listens, not consensus port
// blocks.
const (
	ConsensusModulePortBase    = 24700
	ConsensusModuleLogEndpoint = "239.255.12.3:24704"

	RunnerPortBase    = 24800
	RunnerLogEndpoint = "239.255.12.13:24804"

	RunnerArchiveLossPortBase    = 24900
	RunnerArchiveLossLogEndpoint = "239.255.12.99:24904"

	RunnerMemberServicePortBase    = 30200
	RunnerMemberServiceLogEndpoint = "239.255.12.109:30204"

	RunnerServiceStartFailurePortBase    = 30300
	RunnerServiceStartFailureLogEndpoint = "239.255.12.111:30304"

	ClusterMemberSmokePortBase    = 25300
	ClusterMemberSmokeLogEndpoint = "239.255.12.9:25304"

	ClusterMemberRestartPortBase    = 25500
	ClusterMemberRestartLogEndpoint = "239.255.12.11:25504"

	ClusterServiceSnapshotPortBase    = 25800
	ClusterServiceSnapshotLogEndpoint = "239.255.12.71:25804"

	ClusterModuleSnapshotPortBase    = 25900
	ClusterModuleSnapshotLogEndpoint = "239.255.12.75:25904"

	ClusterTruncatePortBase    = 27500
	ClusterTruncateLogEndpoint = "239.255.12.29:27504"

	// Archive-node launcher smoke and archive client. Single control binds;
	// not PortBase blocks.
	ArchiveNodeSmokeControlPort0           = 28001
	ArchiveClientControlPort0              = 28101
	ArchiveReplicateSourceControlPort      = 28111
	ArchiveReplicateDestinationControlPort = 28121
	ArchiveClientFaultControlPort0         = 28151
	ArchiveNodePairControlPort0            = 30001
	ArchiveNodePairControlPort1            = 30101
)

// Archive control URIs. Each derives from a port constant above so the
// non-overlap matrix and live binds cannot drift.
var (
	ArchiveNodeSmokeControl0           = ArchiveControlChannel(ArchiveNodeSmokeControlPort0)
	ArchiveClientControl0              = ArchiveControlChannel(ArchiveClientControlPort0)
	ArchiveReplicateSourceControl      = ArchiveControlChannel(ArchiveReplicateSourceControlPort)
	ArchiveReplicateDestinationControl = ArchiveControlChannel(ArchiveReplicateDestinationControlPort)
	ArchiveClientFaultControl0         = ArchiveControlChannel(ArchiveClientFaultControlPort0)
	ArchiveNodePairControl0            = ArchiveControlChannel(ArchiveNodePairControlPort0)
	ArchiveNodePairControl1            = ArchiveControlChannel(ArchiveNodePairControlPort1)
)

// ArchiveControlChannel returns the loopback UDP Archive control URI for port.
func ArchiveControlChannel(port int) string {
	return "aeron:udp?endpoint=" + LoopbackUDPEndpoint(port)
}

// LoopbackUDPEndpoint returns host:port for a loopback UDP endpoint.
func LoopbackUDPEndpoint(port int) string {
	return "127.0.0.1:" + strconv.Itoa(port)
}

const (
	// linuxEphemeralPortFloor is the default first Linux ephemeral port
	// (net.ipv4.ip_local_port_range 32768-60999). Reserved unicast blocks must
	// end below this floor.
	linuxEphemeralPortFloor = 32768

	// ianaDynamicPortFloor is the first IANA dynamic port. Hosts without
	// the Linux sysctl default to 49152-65535.
	ianaDynamicPortFloor = 49152

	maxUDPPort = 65535

	// freeUDPFirst sits above the 9000-9205 sibling block. The ledger does
	// not hold that block.
	freeUDPFirst = 16000

	freeUDPWidth = maxUDPPort - freeUDPFirst + 1

	// freeUDPClaimDir holds one flock file per claimed port. Ports are
	// host-wide, so every go test binary shares one fixed path. Files stay
	// after unlock so the next opener locks the same inode.
	freeUDPClaimDir = "/tmp/luas-udp-claim"
)

// ClientEgressOffset is the unused slot after the last topology offset.
const ClientEgressOffset = cluster.TransferPortOffset + 1

// ClientEgressPort returns portBase plus ClientEgressOffset.
//
// The reservation must cover at least two members. A one-member block ends at
// portBase plus TransferPortOffset.
func ClientEgressPort(portBase int) int {
	return portBase + ClientEgressOffset
}

// FreeUDPPort returns one loopback UDP port. See FreeUDPPorts.
func FreeUDPPort(t testing.TB) int {
	t.Helper()
	return FreeUDPPorts(t, 1)[0]
}

// FreeUDPPorts returns n distinct loopback UDP ports for live UDP channels.
//
// Candidates sit at or above 16000 and outside the live kernel ephemeral
// range, so a port-0 bind never takes one. An exclusive flock under
// /tmp/luas-udp-claim holds each port until t finishes. Parallel tests,
// including other go test binaries, skip a held port. The probe socket
// closes before return so the media driver can bind.
func FreeUDPPorts(t testing.TB, n int) []int {
	t.Helper()
	require.Positive(t, n)

	ephemeral, err := freeUDPEphemeralRange()
	require.NoError(t, err)

	ports := make([]int, 0, n)
	for attempts := 0; len(ports) < n && attempts < freeUDPWidth; attempts++ {
		port := nextFreeUDPCandidate()
		ephemeralPort := port >= ephemeral.first && port <= ephemeral.last
		if ephemeralPort || isReservedUDPPort(port) {
			continue
		}
		if claimFreeUDPPort(t, freeUDPClaimDir, port) {
			ports = append(ports, port)
		}
	}
	if len(ports) < n {
		require.FailNow(t, fmt.Sprintf(
			"FreeUDPPorts: claimed %d of %d ports in %d-%d",
			len(ports), n, freeUDPFirst, maxUDPPort,
		))
	}
	return ports
}

// freeUDPEphemeralRange reads the live ephemeral range once and creates
// freeUDPClaimDir. Returns an error when the range covers every candidate.
var freeUDPEphemeralRange = sync.OnceValues(func() (portRange, error) {
	if err := os.MkdirAll(freeUDPClaimDir, 0o755); err != nil {
		return portRange{}, fmt.Errorf("FreeUDPPorts: claim dir: %w", err)
	}
	floor, ceil, err := liveEphemeralPortRange()
	if err != nil {
		return portRange{}, fmt.Errorf("FreeUDPPorts: read ip_local_port_range: %w", err)
	}
	if floor <= freeUDPFirst && ceil >= maxUDPPort {
		return portRange{}, fmt.Errorf(
			"FreeUDPPorts: ephemeral range %d-%d covers %d-%d",
			floor, ceil, freeUDPFirst, maxUDPPort,
		)
	}
	return portRange{first: floor, last: ceil}, nil
})

// liveEphemeralPortRange reads net.ipv4.ip_local_port_range. A missing
// sysctl returns the IANA dynamic range.
func liveEphemeralPortRange() (int, int, error) {
	data, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
	if errors.Is(err, os.ErrNotExist) {
		return ianaDynamicPortFloor, maxUDPPort, nil
	}
	if err != nil {
		return 0, 0, err
	}
	var floor, ceil int
	if _, err := fmt.Sscanf(string(data), "%d %d", &floor, &ceil); err != nil {
		return 0, 0, fmt.Errorf("parse %q: %w", data, err)
	}
	if floor <= 0 || ceil < floor || ceil > maxUDPPort {
		return 0, 0, fmt.Errorf("invalid range %d-%d", floor, ceil)
	}
	return floor, ceil, nil
}

var (
	freeUDPMu     sync.Mutex
	freeUDPCursor = os.Getpid()
)

func nextFreeUDPCandidate() int {
	freeUDPMu.Lock()
	defer freeUDPMu.Unlock()
	port := freeUDPFirst + freeUDPCursor%freeUDPWidth
	freeUDPCursor++
	return port
}

// claimFreeUDPPort locks the claim file for port under dir. It then probes a
// loopback bind. The claim lasts until t finishes. Returns false when another
// caller holds the port or the bind fails.
func claimFreeUDPPort(t testing.TB, dir string, port int) bool {
	t.Helper()
	claim, ok := lockFreeUDPPort(t, dir, port)
	if !ok {
		return false
	}
	conn, err := net.ListenPacket("udp4", LoopbackUDPEndpoint(port))
	if err != nil {
		_ = claim.Close()
		return false
	}
	_ = conn.Close()
	t.Cleanup(func() { _ = claim.Close() })
	return true
}

// lockFreeUDPPort takes a non-blocking exclusive flock on the claim file for
// port under dir. The caller closes the file to drop the claim. Returns false
// when another caller holds the port.
func lockFreeUDPPort(t testing.TB, dir string, port int) (*os.File, bool) {
	t.Helper()
	path := filepath.Join(dir, strconv.Itoa(port))
	claim, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	require.NoError(t, err)
	err = unix.Flock(int(claim.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if err != nil {
		_ = claim.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, false
		}
		require.FailNow(t, fmt.Sprintf("FreeUDPPorts: flock %s: %v", path, err))
	}
	return claim, true
}

type portLedgerBlock struct {
	name        string
	portBase    int
	memberCount int
	logEndpoint string
}

type portLedgerSingle struct {
	name string
	port int
}

// multicastPortLedger returns every reserved consensus block that has a
// shared cluster-log multicast endpoint.
func multicastPortLedger() []portLedgerBlock {
	return []portLedgerBlock{
		{
			name:        "consensus-module",
			portBase:    ConsensusModulePortBase,
			memberCount: 1,
			logEndpoint: ConsensusModuleLogEndpoint,
		},
		{
			name:        "runner",
			portBase:    RunnerPortBase,
			memberCount: 1,
			logEndpoint: RunnerLogEndpoint,
		},
		{
			name:        "runner-archive-loss",
			portBase:    RunnerArchiveLossPortBase,
			memberCount: 1,
			logEndpoint: RunnerArchiveLossLogEndpoint,
		},
		{
			name:        "runner-member-service",
			portBase:    RunnerMemberServicePortBase,
			memberCount: 1,
			logEndpoint: RunnerMemberServiceLogEndpoint,
		},
		{
			name:        "runner-service-start-failure",
			portBase:    RunnerServiceStartFailurePortBase,
			memberCount: 1,
			logEndpoint: RunnerServiceStartFailureLogEndpoint,
		},
		{
			name:        "module-elect",
			portBase:    ClusterModuleElectPortBase,
			memberCount: 3,
			logEndpoint: ClusterModuleElectLogEndpoint,
		},
		{
			name:        "member-smoke",
			portBase:    ClusterMemberSmokePortBase,
			memberCount: 2,
			logEndpoint: ClusterMemberSmokeLogEndpoint,
		},
		{
			name:        "member-restart",
			portBase:    ClusterMemberRestartPortBase,
			memberCount: 2,
			logEndpoint: ClusterMemberRestartLogEndpoint,
		},
		{
			name:        "replay",
			portBase:    ClusterReplayPortBase,
			memberCount: 1,
			logEndpoint: ClusterReplayLogEndpoint,
		},
		{
			name:        "service-snapshot",
			portBase:    ClusterServiceSnapshotPortBase,
			memberCount: 1,
			logEndpoint: ClusterServiceSnapshotLogEndpoint,
		},
		{
			name:        "module-snapshot",
			portBase:    ClusterModuleSnapshotPortBase,
			memberCount: 1,
			logEndpoint: ClusterModuleSnapshotLogEndpoint,
		},
		{
			name:        "durable-record",
			portBase:    DurableRecordPortBase,
			memberCount: 3,
			logEndpoint: DurableRecordLogEndpoint,
		},
		{
			name:        "durable-restart",
			portBase:    DurableRestartPortBase,
			memberCount: 3,
			logEndpoint: DurableRestartLogEndpoint,
		},
		{
			name:        "durable-reelection",
			portBase:    DurableReElectionPortBase,
			memberCount: 3,
			logEndpoint: DurableReElectionLogEndpoint,
		},
		{
			name:        "durable-catchup",
			portBase:    DurableCatchupPortBase,
			memberCount: 3,
			logEndpoint: DurableCatchupLogEndpoint,
		},
		{
			name:        "durable-divergence",
			portBase:    DurableDivergencePortBase,
			memberCount: 3,
			logEndpoint: DurableDivergenceLogEndpoint,
		},
		{
			name:        "durable-ex-leader-reelect",
			portBase:    DurableExLeaderReElectPortBase,
			memberCount: 3,
			logEndpoint: DurableExLeaderReElectLogEndpoint,
		},
		{
			name:        "durable-replicate",
			portBase:    DurableReplicatePortBase,
			memberCount: 3,
			logEndpoint: DurableReplicateLogEndpoint,
		},
		{
			name:        "durable-replicate-truncate",
			portBase:    DurableReplicateTruncatePortBase,
			memberCount: 3,
			logEndpoint: DurableReplicateTruncateLogEndpoint,
		},
		{
			name:        "cluster-control",
			portBase:    ClusterControlPortBase,
			memberCount: 3,
			logEndpoint: ClusterControlLogEndpoint,
		},
		{
			name:        "multi-service-snapshot-retention",
			portBase:    ClusterMultiServiceSnapshotRetentionPortBase,
			memberCount: 3,
			logEndpoint: ClusterMultiServiceSnapshotRetentionLogEndpoint,
		},
		{
			name:        "java-client",
			portBase:    ClusterJavaClientPortBase,
			memberCount: 3,
			logEndpoint: ClusterJavaClientLogEndpoint,
		},
		{
			name:        "session-replicate-ex-leader",
			portBase:    ClusterSessionReplicateExLeaderPortBase,
			memberCount: 3,
			logEndpoint: ClusterSessionReplicateExLeaderLogEndpoint,
		},
		{
			name:        "durable-concurrent-catchup",
			portBase:    DurableConcurrentCatchupPortBase,
			memberCount: 5,
			logEndpoint: DurableConcurrentCatchupLogEndpoint,
		},
		{
			name:        "truncate",
			portBase:    ClusterTruncatePortBase,
			memberCount: 3,
			logEndpoint: ClusterTruncateLogEndpoint,
		},
		{
			name:        "module-commit",
			portBase:    ClusterModuleCommitPortBase,
			memberCount: 3,
			logEndpoint: ClusterModuleCommitLogEndpoint,
		},
		{
			name:        "module-rejoin",
			portBase:    ClusterModuleRejoinPortBase,
			memberCount: 3,
			logEndpoint: ClusterModuleRejoinLogEndpoint,
		},
		{
			name:        "module-reelect",
			portBase:    ClusterModuleReElectPortBase,
			memberCount: 3,
			logEndpoint: ClusterModuleReElectLogEndpoint,
		},
		{
			name:        "module-follower-stop",
			portBase:    ClusterModuleFollowerStopPortBase,
			memberCount: 3,
			logEndpoint: ClusterModuleFollowerStopLogEndpoint,
		},
		{
			name:        "module-image-loss",
			portBase:    ClusterModuleImageLossPortBase,
			memberCount: 3,
			logEndpoint: ClusterModuleImageLossLogEndpoint,
		},
		{
			name:        "durable-rank",
			portBase:    DurableRankPortBase,
			memberCount: 3,
			logEndpoint: DurableRankLogEndpoint,
		},
		{
			name:        "durable-restart-leader",
			portBase:    DurableRestartLeaderPortBase,
			memberCount: 3,
			logEndpoint: DurableRestartLeaderLogEndpoint,
		},
		{
			name:        "durable-restart-running",
			portBase:    DurableRestartRunningPortBase,
			memberCount: 3,
			logEndpoint: DurableRestartRunningLogEndpoint,
		},
		{
			name:        "session-smoke",
			portBase:    ClusterSessionSmokePortBase,
			memberCount: 3,
			logEndpoint: ClusterSessionSmokeLogEndpoint,
		},
		{
			name:        "session-reject",
			portBase:    ClusterSessionRejectPortBase,
			memberCount: 3,
			logEndpoint: ClusterSessionRejectLogEndpoint,
		},
		{
			name:        "member-amplifier",
			portBase:    ClusterMemberAmplifierPortBase,
			memberCount: 2,
			logEndpoint: ClusterMemberAmplifierLogEndpoint,
		},
		{
			name:        "service-ack",
			portBase:    ClusterServiceAckPortBase,
			memberCount: 3,
			logEndpoint: ClusterServiceAckLogEndpoint,
		},
		{
			name:        "session-extend-restart",
			portBase:    ClusterSessionExtendRestartPortBase,
			memberCount: 3,
			logEndpoint: ClusterSessionExtendRestartLogEndpoint,
		},
		{
			name:        "session-extend-reelect",
			portBase:    ClusterSessionExtendReElectPortBase,
			memberCount: 3,
			logEndpoint: ClusterSessionExtendReElectLogEndpoint,
		},
		{
			name:        "multi-service",
			portBase:    ClusterMultiServicePortBase,
			memberCount: 3,
			logEndpoint: ClusterMultiServiceLogEndpoint,
		},
		{
			name:        "session-transfer",
			portBase:    ClusterSessionTransferPortBase,
			memberCount: 3,
			logEndpoint: ClusterSessionTransferLogEndpoint,
		},
		{
			name:        "service-transfer",
			portBase:    ClusterServiceTransferPortBase,
			memberCount: 3,
			logEndpoint: ClusterServiceTransferLogEndpoint,
		},
		{
			name:        "service-restart",
			portBase:    ClusterServiceRestartPortBase,
			memberCount: 3,
			logEndpoint: ClusterServiceRestartLogEndpoint,
		},
		{
			name:        "service-rebuild",
			portBase:    ClusterServiceRebuildPortBase,
			memberCount: 3,
			logEndpoint: ClusterServiceRebuildLogEndpoint,
		},
		{
			name:        "service-egress",
			portBase:    ClusterServiceEgressPortBase,
			memberCount: 3,
			logEndpoint: ClusterServiceEgressLogEndpoint,
		},
		{
			name:        "service-snapshot-restart",
			portBase:    ClusterServiceSnapshotRestartPortBase,
			memberCount: 3,
			logEndpoint: ClusterServiceSnapshotRestartLogEndpoint,
		},
		{
			name:        "multi-service-snapshot-restart",
			portBase:    ClusterMultiServiceSnapshotRestartPortBase,
			memberCount: 3,
			logEndpoint: ClusterMultiServiceSnapshotRestartLogEndpoint,
		},
		{
			name:        "service-restored-egress",
			portBase:    ClusterServiceRestoredEgressPortBase,
			memberCount: 3,
			logEndpoint: ClusterServiceRestoredEgressLogEndpoint,
		},
		{
			name:        "cluster-streams-multicast",
			portBase:    ClusterStreamsMulticastPortBase,
			memberCount: 2,
			logEndpoint: ClusterStreamsMulticastLogEndpoint,
		},
		{
			name:        "cluster-java",
			portBase:    ClusterJavaPortBase,
			memberCount: ClusterJavaMemberCount,
			logEndpoint: ClusterJavaMixedLogEndpoint,
		},
		{
			name:        "process-cluster-smoke",
			portBase:    ProcessClusterSmokePortBase,
			memberCount: 3,
			logEndpoint: ProcessClusterSmokeLogEndpoint,
		},
		{
			name:        "service-stop",
			portBase:    ClusterServiceStopPortBase,
			memberCount: 3,
			logEndpoint: ClusterServiceStopLogEndpoint,
		},
		{
			name:        "live-soak",
			portBase:    LiveSoakPortBase,
			memberCount: 3,
			logEndpoint: LiveSoakLogEndpoint,
		},
		{
			name:        "cluster-termination",
			portBase:    ClusterTerminationPortBase,
			memberCount: 3,
			logEndpoint: ClusterTerminationLogEndpoint,
		},
		{
			name:        "cluster-linearizability",
			portBase:    ClusterLinearizabilityPortBase,
			memberCount: 3,
			logEndpoint: ClusterLinearizabilityLogEndpoint,
		},
		{
			name:        "crash-restart-leader",
			portBase:    CrashRestartLeaderPortBase,
			memberCount: 3,
			logEndpoint: CrashRestartLeaderLogEndpoint,
		},
		{
			name:        "crash-restart-follower",
			portBase:    CrashRestartFollowerPortBase,
			memberCount: 3,
			logEndpoint: CrashRestartFollowerLogEndpoint,
		},
	}
}

// unicastPortLedger returns reserved consensus blocks with no shared
// multicast endpoint.
func unicastPortLedger() []portLedgerBlock {
	return []portLedgerBlock{
		{
			name:        "consensus-publications",
			portBase:    ConsensusPublicationsPortBase,
			memberCount: 3,
		},
		{
			name:        "cluster-streams-commit",
			portBase:    ClusterStreamsCommitPortBase,
			memberCount: 2,
		},
		{
			name:        "cluster-streams-unicast",
			portBase:    ClusterStreamsUnicastPortBase,
			memberCount: 2,
		},
		{
			name:        "durable-unicast-catchup",
			portBase:    DurableUnicastCatchupPortBase,
			memberCount: 3,
		},
		{
			name:        "durable-unicast-ex-leader",
			portBase:    DurableUnicastExLeaderPortBase,
			memberCount: 3,
		},
	}
}

// singlePortLedger returns reserved single UDP listen ports.
func singlePortLedger() []portLedgerSingle {
	return []portLedgerSingle{
		{name: "cluster-streams-multicast-log", port: ClusterLogNLTMulticastPort},
		{name: "archive-smoke-0", port: ArchiveNodeSmokeControlPort0},
		{name: "archive-client-0", port: ArchiveClientControlPort0},
		{name: "archive-replicate-source", port: ArchiveReplicateSourceControlPort},
		{name: "archive-replicate-destination", port: ArchiveReplicateDestinationControlPort},
		{name: "archive-client-fault-0", port: ArchiveClientFaultControlPort0},
		{name: "archive-pair-0", port: ArchiveNodePairControlPort0},
		{name: "archive-pair-1", port: ArchiveNodePairControlPort1},
	}
}

// portBlockLast returns the transfer port of the highest member id.
// memberCount must be >= 1.
func portBlockLast(portBase, memberCount int) int {
	return cluster.CalculatePort(int32(memberCount-1), portBase, cluster.TransferPortOffset)
}

// portRange is one inclusive reserved UDP port range.
type portRange struct {
	first int
	last  int
}

// reservedUDPPortRanges holds every harness reservation as inclusive port
// ranges. The ledgers build once. FreeUDPPorts calls isReservedUDPPort once
// per probed candidate.
var reservedUDPPortRanges = sync.OnceValue(func() []portRange {
	var ranges []portRange
	for _, b := range append(multicastPortLedger(), unicastPortLedger()...) {
		ranges = append(ranges, portRange{
			first: b.portBase + cluster.ArchiveControlPortOffset,
			last:  portBlockLast(b.portBase, b.memberCount),
		})
	}
	for _, s := range singlePortLedger() {
		ranges = append(ranges, portRange{first: s.port, last: s.port})
	}
	return ranges
})

// isReservedUDPPort reports whether port sits in a harness reservation.
func isReservedUDPPort(port int) bool {
	for _, r := range reservedUDPPortRanges() {
		if port >= r.first && port <= r.last {
			return true
		}
	}
	return false
}

func portInBlock(port, portBase, memberCount int) bool {
	first := portBase + cluster.ArchiveControlPortOffset
	last := portBlockLast(portBase, memberCount)
	return port >= first && port <= last
}

// ClusterLogChannelTemplate confines cluster-log multicast to loopback.
//
// Without interface=, the driver joins the group on the default route interface.
// The frames then leave the host. ttl=0 confines traffic to a single host. A
// multi-host cluster needs the cluster interface and a positive ttl.
const ClusterLogChannelTemplate = clusterLogChannelPrefix + "|interface=127.0.0.1|ttl=0"

const clusterLogChannelPrefix = "aeron:udp?term-length=64k"
