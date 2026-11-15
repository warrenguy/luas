package client

import (
	"errors"
	"fmt"

	"github.com/warrenguy/luas/atomicbuffer"
	"github.com/warrenguy/luas/cluster/codecs/clusterwire"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/cluster/codecs"
	"github.com/warrenguy/luas/internal/transportfault"
)

// ErrTransportTerminating reports that the connect-owned transport client has
// terminated. AeronCluster.TransportFault and AsyncConnect.Poll wrap it with
// the conductor cause.
var ErrTransportTerminating = errors.New("cluster client: transport client is terminating")

// transportTerminatingError wraps the monitor termination cause for client
// with ErrTransportTerminating. Returns nil when the monitor reports none.
func transportTerminatingError(monitor *transportfault.Monitor, client *aeron.Client) error {
	cause := monitor.TerminationCause(client)
	if cause == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrTransportTerminating, cause)
}

// AeronCluster is a connected cluster client session. Not safe for concurrent use.
//
// One goroutine runs the session. SpinOnce runs the standard duty cycle. It
// calls DoWork only when the transport uses invoker mode. A caller that polls
// PollEgress directly in invoker mode must call DoWork too. For paced ingress
// retry, use IngressOfferUntilPositive or SendKeepAliveUntilPositive.
//
// Callers must CommitClaim or AbortClaim any in-flight TryClaim before Close.
//
// SpinOnce does not accept context. Wrap the outer duty cycle with context.Context
// and check ctx.Err() between SpinOnce calls.
//
// A fault that terminates the Connect-owned transport client closes the
// session on the next PollEgress. TransportFault then returns the cause. A
// caller-supplied client keeps the session open.
//
// Accessors, SpinOnce, and session ingress-retry helpers tolerate a nil receiver;
// other methods require a non-nil receiver.
type AeronCluster struct {
	cfg       Config
	aeron     *aeron.Client
	ownsAeron bool

	clusterSessionID int64
	leadershipTermID int64
	leaderMemberID   int32

	ingressPublication IngressPublication

	egressSubscription *aeron.Subscription
	egressImage        *aeron.Image
	egressAdapter      *EgressAdapter
	nanoClock          func() int64
	ingressMaxFrameLen int
	ingressScratch     []byte
	controlScratch     []byte
	closed             bool

	memberMap MemberIngressMap

	sessionState    SessionState
	stateDeadlineNs int64

	egressHooks egressDispatchHooks

	// transportFault reads the termination cause of a connect-owned client.
	// It is nil when the caller supplied Config.Aeron.
	transportFault *transportfault.Monitor

	// transportFaultCause is the error that closed the session on a transport
	// client fault. Nil until a fault closes the session.
	transportFaultCause error
}

// IngressClaim holds an in-flight TryClaim until CommitClaim or AbortClaim.
//
// The caller owns the claim between TryClaim success and commit/abort. Close does
// not finalize pending claims. Position is the stream position returned by the
// underlying TryClaim. publication stores the ingress publication used to claim.
// leadershipTermID stores the session term encoded into the claim header.
// CommitClaim and AbortClaim use that publication and term, not the session's
// current leader ingress, so stale claims abort instead of committing on a wrong
// publication or leadership term.
type IngressClaim struct {
	TermIndex        int
	FrameOffset      int
	PayloadLength    int
	Position         int64
	leadershipTermID int64
	publication      IngressPublication
}

// NewAeronCluster constructs a session from a successful ConnectResult.
//
// Runs result.Config.Normalize().Validate() -- requires non-nil EgressHandler.
// Rejects nil result, nil required handles, closed ingress/subscription handles,
// or missing leader ingress publication. Transfers resource ownership from
// ConnectResult into AeronCluster; after success the result's transport handles
// are owned by the session -- do not Close them separately.
func NewAeronCluster(result *ConnectResult) (*AeronCluster, error) {
	if result == nil {
		return nil, errors.New("cluster client: nil connect result")
	}
	if result.IngressPublication == nil {
		return nil, errors.New("cluster client: ingress publication missing")
	}
	if result.EgressSubscription == nil {
		return nil, errors.New("cluster client: egress subscription missing")
	}
	if result.Aeron == nil {
		return nil, errors.New("cluster client: aeron client missing")
	}
	if result.IngressPublication.IsClosed() {
		return nil, errors.New("cluster client: ingress publication is closed")
	}
	if result.EgressSubscription.IsClosed() {
		return nil, errors.New("cluster client: egress subscription is closed")
	}
	if result.EgressImage != nil && result.EgressImage.IsClosed() {
		return nil, errors.New("cluster client: egress image is closed")
	}

	cfg := result.Config.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	memberMap := result.MemberIngressMap
	ingress, err := memberMap.Require(leaderIngressMemberID(memberMap, result.LeaderMemberID))
	if err != nil {
		return nil, err
	}
	if ingress.Publication == nil {
		return nil, errors.New("cluster client: leader ingress publication missing")
	}
	if ingress.Publication != result.IngressPublication {
		return nil, errors.New("cluster client: leader ingress publication mismatch")
	}

	ingressScratch, ingressMaxFrameLen := ingressScratchForPub(result.IngressPublication, nil)

	closeWireLen, closeOK := codecs.ClusterWireMessageLen(
		clusterwire.SessionCloseRequestSbeBlockLength,
	)
	keepAliveWireLen, keepAliveOK := codecs.ClusterWireMessageLen(
		clusterwire.SessionKeepAliveSbeBlockLength,
	)
	adminWireLen, adminOK := codecs.ClusterWireMessageLen(
		clusterwire.AdminRequestSbeBlockLength,
		0,
	)
	if !closeOK || !keepAliveOK || !adminOK {
		return nil, errors.New("cluster client: control frame wire length unavailable")
	}
	controlScratchLen := max(closeWireLen, keepAliveWireLen, adminWireLen)

	nanoClock := resolveNanoClock(cfg.AeronConfig.NanoClock)

	ac := &AeronCluster{
		cfg:                cfg,
		aeron:              result.Aeron,
		ownsAeron:          result.OwnsAeronClient,
		clusterSessionID:   result.ClusterSessionID,
		leadershipTermID:   result.LeadershipTermID,
		leaderMemberID:     result.LeaderMemberID,
		nanoClock:          nanoClock,
		ingressMaxFrameLen: ingressMaxFrameLen,
		ingressScratch:     ingressScratch,
		controlScratch:     make([]byte, controlScratchLen),
		memberMap:          memberMap,
		sessionState:       SessionStateConnected,
		transportFault:     result.transportFault,
	}

	ac.egressAdapter = newEgressAdapterWithHandler(
		cfg.EgressHandler,
		result.ClusterSessionID,
		result.EgressSubscription,
		0,
		cfg.DirectAssemblers,
		ac.dispatchSessionEgressFragment,
	)

	ac.ingressPublication = result.IngressPublication
	ac.egressSubscription = result.EgressSubscription
	ac.egressImage = result.EgressImage
	ac.egressHooks = sessionEgressDispatchHooks(ac)

	result.IngressPublication = nil
	result.EgressSubscription = nil
	result.EgressImage = nil
	// Injected Aeron (OwnsAeronClient false) stays on result; session does not close it.
	if result.OwnsAeronClient {
		result.Aeron = nil
	}

	return ac, nil
}

// PollEgress polls the egress subscription and dispatches to EgressHandler.
//
// When the tracked egress image is already closed at entry, teardown and reattach
// run before the adapter poll so replacement-image fragments are not reassembled
// and then discarded. Otherwise the adapter is polled first (Java parity). When
// the tracked image closes during poll, post-poll teardown frees assembler state
// and counts as work; pollLeaderRecovery and pollStateChanges run on every
// non-fault path until Close.
//
// PollEgress closes the session when the owned transport client has
// terminated, before the adapter poll. TransportFault returns the cause. The
// owned client Close sleeps AeronConfig.ClientCloseLingerDuration.
//
// Concurrency: invoker-only. PollEgress takes the transport client lock after
// an owned client error. Must not run inside a transport client callback.
func (ac *AeronCluster) PollEgress() int {
	if ac.closed || ac.egressAdapter == nil {
		return 0
	}
	if ac.enterPendingCloseOnTransportFault() {
		return ac.pollStateChanges()
	}

	// Snapshot before teardown nils egressImage; drives handleDisconnected when reattach fails.
	closedAtEntry := ac.egressImage != nil && ac.egressImage.IsClosed()
	lostTrackedEgress := closedAtEntry

	var work int
	if closedAtEntry {
		work += ac.pollEgressTeardownClosedImage()
		ac.tryAttachEgressImage()
	}

	// Java polls the subscription every spin; adapter-first matches Java pollEgress work accounting.
	work += ac.egressAdapter.Poll()

	if ac.egressImage != nil && ac.egressImage.IsClosed() {
		work += ac.pollEgressTeardownClosedImage()
		lostTrackedEgress = true
	}

	if ac.egressImage == nil {
		ac.tryAttachEgressImage()
	}

	work += ac.pollTransportDisconnect(lostTrackedEgress)

	work += ac.pollLeaderRecovery()
	work += ac.pollStateChanges()

	return work
}

// Close sends a best-effort SessionCloseRequest and releases session resources.
//
// Closes member ingress publications (including the current leader), egress
// subscription, clears egress adapter state and the tracked egress image, and
// clears the cached leader ingress handle. MemberIngressMap() still returns the
// session map after Close; publications are closed but entries are not removed.
//
// When the session owns the transport client (OwnsAeronClient on ConnectResult),
// Close also closes Aeron. When Aeron was injected via Config.Aeron, the
// transport stays with the caller and Close does not close it.
//
// Idempotent after the first Close invocation.
func (ac *AeronCluster) Close() error {
	if ac.closed {
		return nil
	}
	ac.closed = true
	ac.sendSessionCloseBestEffort()
	ac.memberMap.closeAll(ac.aeron)
	ac.ingressPublication = nil
	ac.teardownEgressImageState()
	if ac.egressSubscription != nil {
		_ = ac.egressSubscription.Close()
		ac.egressSubscription = nil
	}
	if ac.ownsAeron && ac.aeron != nil {
		_ = ac.aeron.Close()
		ac.aeron = nil
	}
	return nil
}

// TransportFault returns the error that closed the session on an owned
// transport client fault. The error wraps ErrTransportTerminating and the
// conductor cause. Returns nil when a fault did not close the session.
func (ac *AeronCluster) TransportFault() error {
	if ac == nil {
		return nil
	}
	return ac.transportFaultCause
}

// IsClosed reports whether Close has been invoked.
func (ac *AeronCluster) IsClosed() bool {
	if ac == nil {
		return true
	}
	return ac.closed
}

// ClusterSessionID returns the connected cluster session id.
func (ac *AeronCluster) ClusterSessionID() int64 {
	if ac == nil {
		return 0
	}
	return ac.clusterSessionID
}

// LeadershipTermID returns the live leadership term id for the session.
//
// Updated on leadership recovery before ingress reconnect completes. May diverge
// from ConnectResult.LeadershipTermID after leader change.
func (ac *AeronCluster) LeadershipTermID() int64 {
	if ac == nil {
		return 0
	}
	return ac.leadershipTermID
}

// LeaderMemberID returns the live leader member id for the session.
//
// Updated on leadership recovery. May diverge from ConnectResult.LeaderMemberID
// after leader change.
func (ac *AeronCluster) LeaderMemberID() int32 {
	if ac == nil {
		return 0
	}
	return ac.leaderMemberID
}

// MemberIngressMap returns the live session member map. Do not mutate entries or
// close publications out of band; on leader change the session replaces the map
// with a newly built one - do not cache the returned map reference across leader
// changes. External mutation breaks session invariants. Tests may read entries;
// use testhooks for injection.
func (ac *AeronCluster) MemberIngressMap() MemberIngressMap {
	if ac == nil {
		return nil
	}
	return ac.memberMap
}

// IngressPublication returns the current leader ingress publication handle.
//
// Nil during AWAIT_NEW_LEADER until a NewLeader event triggers ingress rebuild.
// Nil during AWAIT_NEW_LEADER_CONNECTION until pollLeaderRecovery reconnects via
// keep-alive on the member-map leader publication. Cleared and repointed on leader
// change. Do not cache the returned handle across PollEgress spins or leadership
// recovery.
func (ac *AeronCluster) IngressPublication() IngressPublication {
	if ac == nil {
		return nil
	}
	return ac.ingressPublication
}

// EgressSubscription returns the egress subscription handle.
func (ac *AeronCluster) EgressSubscription() *aeron.Subscription {
	if ac == nil {
		return nil
	}
	return ac.egressSubscription
}

// Config returns the normalized cluster config used by the session.
func (ac *AeronCluster) Config() Config {
	if ac == nil {
		return Config{}
	}
	return ac.cfg
}

// Aeron returns the transport client. Nil after Close when the session owned the client.
func (ac *AeronCluster) Aeron() *aeron.Client {
	if ac == nil {
		return nil
	}
	return ac.aeron
}

func (ac *AeronCluster) dispatchSessionEgressFragment(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	header *aeron.Header,
) {
	dispatchSessionEgressFragment(
		ac,
		ac.cfg.EgressHandler,
		ac.clusterSessionID,
		buffer,
		offset,
		length,
		header,
	)
}

func (ac *AeronCluster) pollEgressTeardownClosedImage() int {
	ac.teardownEgressImageState()
	return 1 // Java counts disconnect teardown as poll work.
}

func (ac *AeronCluster) tryAttachEgressImage() {
	sub := ac.egressSubscription
	if sub == nil || sub.IsClosed() {
		return
	}

	for _, image := range sub.Images() {
		if image == nil || image.IsClosed() {
			continue
		}

		ac.egressImage = image
		return
	}
}

func (ac *AeronCluster) teardownEgressImageState() {
	if ac.egressAdapter == nil {
		return
	}
	// No egressImageTeardownDone flag: egressImage=nil after first pass, so repeat
	// calls only re-Clear adapter state.
	if ac.egressImage != nil {
		ac.egressAdapter.FreeSessionBuffer(ac.egressImage.SessionID())
	}
	ac.egressAdapter.Clear()
	ac.egressImage = nil
}
