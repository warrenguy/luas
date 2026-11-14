package service

import (
	"errors"
	"fmt"
	"iter"
	"math"
	"slices"
	"time"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/archive"
	"github.com/warrenguy/luas/archive/codecs/archivewire"
	"github.com/warrenguy/luas/cluster/codecs"
	"github.com/warrenguy/luas/cluster/codecs/clusterwire"
)

// ErrFailed wraps the first fatal service error.
var ErrFailed = errors.New("service: failed")

// ErrStart wraps a Start error. The error is fatal.
var ErrStart = errors.New("service: start")

// ErrLogSubscriptionAdd wraps a log subscription add error. The error is not
// fatal unless the transport client is closed or terminating.
var ErrLogSubscriptionAdd = errors.New("service: add log subscription")

// ErrLogNotContiguous marks a join whose log position is not contiguous. The
// error is fatal.
var ErrLogNotContiguous = errors.New("service: log not contiguous")

// ErrSessionClash reports a SessionOpen whose id is already open. The error is
// not fatal.
var ErrSessionClash = errors.New("service: session id clash")

// ErrTimerPublish reports a terminal timer offer code. The error is not fatal.
var ErrTimerPublish = errors.New("service: timer publish failed")

// ErrCloseSessionPublish reports a terminal close-session offer code. The
// error is not fatal.
var ErrCloseSessionPublish = errors.New("service: close session publish failed")

// ErrTakeSnapshot wraps a TakeSnapshot error. The error is not fatal unless it
// wraps a fatal cause. See [TakeSnapshotHandler].
var ErrTakeSnapshot = errors.New("service: take snapshot failed")

// ErrActionPositionMismatch marks a cluster action at the wrong log position.
// The error is fatal.
var ErrActionPositionMismatch = errors.New("service: cluster action log position mismatch")

// ErrUnknownClusterAction reports a cluster action the agent does not dispatch.
// The error is not fatal.
var ErrUnknownClusterAction = errors.New("service: unknown cluster action")

// ErrResponsePublicationAdd wraps a response publication add error. The error
// is not fatal unless the transport client is closed or terminating.
var ErrResponsePublicationAdd = errors.New("service: add response publication")

// ErrTransportTerminating reports a transport client that has entered
// termination. The error is fatal.
var ErrTransportTerminating = errors.New("service: transport client is terminating")

// ErrUnknownSessionClose reports a SessionClose for an unknown id. The error is
// not fatal.
var ErrUnknownSessionClose = errors.New("service: unknown session close")

// ErrCloseUnknownSession reports CloseClientSession for an unknown id. The
// error is not fatal.
var ErrCloseUnknownSession = errors.New("service: close unknown client session")

// ErrUnknownTimeUnit reports an unknown NewLeadershipTermEvent time unit. The
// error is not fatal.
var ErrUnknownTimeUnit = errors.New("service: unknown time unit")

// ErrTerminate wraps a Terminate error. The error is not fatal unless it wraps
// a fatal cause. See [TerminateHandler].
var ErrTerminate = errors.New("service: terminate")

// ErrTerminationPositionPassed marks a stop with the log position past the
// termination position. The agent marks the error after Terminate and the
// termination drain. The error is fatal.
var ErrTerminationPositionPassed = errors.New("service: termination position passed")

// ErrTerminationAckPublish reports an unsent termination ack. The agent still
// stops. The error is not fatal.
var ErrTerminationAckPublish = errors.New("service: termination ack publish failed")

// AgentConfig configures NewAgent. Zero-value fields have the defaults stated
// per field.
type AgentConfig struct {
	// ServiceID is this container's service id (upstream default 0).
	ServiceID int32

	// Service receives log-event callbacks. Nil is allowed: dispatch updates
	// agent state and skips callbacks.
	Service ClusteredService

	// CommittedPosition supplies the committed log prefix this member treats
	// as safe for application consumption. Nil is allowed: the log poll is
	// skipped until wired. The service consumes nothing without a bound.
	//
	// With a nil CommittedPosition, the image position stays at the join
	// position. The image unbinds only when it closes, or when the
	// end-of-stream word or MaxLogPosition sits at that position. A pending
	// join waits until then.
	CommittedPosition func() int64

	// Subscriptions adds the log subscription at join time. Nil is allowed: a
	// delivered join stays pending until wired.
	Subscriptions SubscriptionAdder

	// FragmentLimit caps fragments per log poll.
	// Zero or negative takes DefaultLogFragmentLimit.
	FragmentLimit int

	// Control polls module-to-service control frames (stream 104). Nil is
	// allowed: control work polls nothing and delivers no frame.
	Control ControlIngress

	// ModuleProxy sends ServiceAck, timer and CloseSession frames (stream
	// 105). Nil is allowed: ready acks stay queued. Timer and close requests
	// return false. A stop reports ErrTerminationAckPublish when the module
	// asked for the ack. A proxy with no bound publication behaves as nil.
	ModuleProxy *ConsensusModuleProxy

	// StartAck enables the spontaneous start ack a Java consensus module
	// consumes during recovery. Nil disables the start ack. See
	// [StartAckConfig].
	StartAck *StartAckConfig

	// Archive is the archive client DoWork polls. Snapshot takes record
	// through Archive and install its session handler, so Archive must carry
	// no session handler of its own. Nil polls none and fails each take. Agent
	// does not own Archive.
	Archive *archive.Client

	// SnapshotPublications adds each snapshot publication. Nil is allowed: a
	// snapshot action then fails the take. Agent does not own the adder.
	SnapshotPublications SnapshotPublicationAdder

	// SnapshotChannel is the snapshot publication channel. Empty takes
	// DefaultSnapshotChannel. NewAgent does not validate it. [Open] does.
	SnapshotChannel string

	// SnapshotStreamID is the snapshot publication stream. Zero takes
	// DefaultSnapshotStreamID. NewAgent does not validate it. [Open] does.
	SnapshotStreamID int32

	// ResponsePublications adds the response publication of each session a
	// leader opens. A leader join with isStartup false adds one for each open
	// session that holds none. Nil is allowed: ClientSession.Offer then
	// returns aeron.NotConnected. Agent does not own the adder.
	ResponsePublications ResponsePublicationAdder

	// ErrorHandler receives service-path errors.
	//
	// Behaviour:
	//   - A non-fatal error reaches the handler once. The duty cycle keeps
	//     running until a stop at the termination position. See
	//     [Agent.IsTerminated].
	//   - The first fatal error reaches the handler once. The error wraps
	//     ErrFailed. ServiceFailure returns the same error. DoWork returns
	//     0 after the fatal error.
	//   - An error after the first fatal error does not reach the handler.
	//   - A Close teardown error does not reach the handler.
	//   - An error after Close does not reach the handler.
	//   - A nil ErrorHandler drops every report. The agent still records a
	//     fatal error.
	//
	// Concurrency: ErrorHandler runs on the goroutine that runs [Agent.DoWork].
	// ErrorHandler must not:
	//   - panic
	//   - call Agent.DoWork or Agent.Close
	//   - call ScheduleTimer, CancelTimer, or CloseClientSession
	//   - call ClientSession.Offer or ClientSession.Close
	//
	// ErrorHandler may call every other Agent method and every other
	// ClientSession method. Those methods do not change state.
	ErrorHandler func(error)

	// Transport reports whether the transport client has entered termination.
	// DoWork marks ErrTransportTerminating when Transport reports true. A nil
	// Transport skips the check. [Open] sets Transport to Config.Aeron. Agent
	// does not own Transport.
	Transport TerminationReporter
}

// Agent owns the service duty cycle: log join, bounded log consumption,
// session registry, and the pending coordination-ack queue.
//
// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe from
// other goroutines. The harness drives DoWork after runner.DoWork on one
// goroutine. Agent never calls DoWork on aeron.Client. The log
// subscription add, the snapshot publication add poll, and the response
// publication add poll run conductor ticks.
//
// Lifetime: Agent stops at the termination position the module sends.
// [Agent.IsTerminated] reports the stop. [Agent.ServiceFailure] reports an
// unexpected stop.
//
// Ownership: Agent owns the BoundedLogAdapter, the log subscription, and the
// snapshot publication and start request of a running take. Agent owns the
// response publications and their in-flight adds. Agent does not own Images
// the subscription holds. Unbind clears the adapter Image reference only.
// Agent does not own the aeron.Client. Agent does not own the archive client.
type Agent struct {
	serviceID         int32
	service           ClusteredService
	committedPosition func() int64
	subscriptions     SubscriptionAdder
	control           ControlIngress
	moduleProxy       *ConsensusModuleProxy

	// archive is the polled archive client, or nil. Never a typed nil.
	archive snapshotArchiveControl

	// newSnapshotStart builds one start recording request. Set with archive.
	newSnapshotStart func(streamID int32, channel []byte) snapshotStartRequest

	// snapshotPublications adds each snapshot publication, or nil.
	snapshotPublications SnapshotPublicationAdder
	snapshotChannel      string
	snapshotStreamID     int32

	// snapshotRowOffer offers one snapshot row. Nil offers through
	// ExclusivePublication.Offer. Tests substitute.
	snapshotRowOffer func(publication *aeron.ExclusivePublication, row []byte) int64

	// take is the snapshot take in flight, or nil.
	take *snapshotTake

	// snapshotScratch is the reused row encode buffer.
	snapshotScratch []byte

	// timeUnit and appVersion come from the last NewLeadershipTermEvent or
	// the BEGIN marker of a loaded snapshot. timeUnit starts at
	// ClusterTimeUnit_NULL_VALUE: no unit known yet.
	timeUnit   clusterwire.ClusterTimeUnit
	appVersion int32

	// lastSnapshotRecordingID is the RelevantID of the last sent snapshot
	// ack. Starts at nullValue.
	lastSnapshotRecordingID int64

	adapter *BoundedLogAdapter

	// serviceFailure holds the first fatal service error, or nil. The stored
	// error wraps ErrFailed. The duty cycle never clears the field.
	serviceFailure error

	errorHandler func(error)

	// transport is never a typed nil.
	transport TerminationReporter

	// pendingJoin is the last JoinLog not yet bound, or nil. One slot. A newer
	// join supersedes an unbound older join and marks the prior join entry
	// ready.
	pendingJoin *logJoinEvent

	// coordinationAcks is the FIFO of pending ServiceAck entries. Drain is
	// prefix-gated: only a ready first queued entry may send. A start
	// entry, when configured, is queued at NewAgent.
	coordinationAcks []coordinationAck

	// sessions is the log-derived registry keyed by cluster session id.
	sessions map[int64]*containerClientSession

	// sessionList holds the sessions of the map in ascending id order.
	sessionList []*containerClientSession

	// clientSessions is the cached ClientSessions sequence. NewAgent builds
	// it once. ClientSessions then allocates nothing.
	clientSessions iter.Seq[ClientSession]

	// responsePublications adds each response publication, or nil. Never a
	// typed nil.
	responsePublications ResponsePublicationAdder

	// responseAdds holds the sessions with a response add in flight.
	responseAdds []*containerClientSession

	// egressScratch is the reused egress frame buffer.
	egressScratch []byte

	// egressLeadershipTermID is the egress header leadershipTermId. Starts at
	// nullValue. The recovery seed and each NewLeadershipTermEvent write it.
	egressLeadershipTermID int64

	logSubscription *aeron.Subscription

	leadershipHandler     NewLeadershipTermEventHandler
	snapshotHandler       TakeSnapshotHandler
	roleChangeHandler     RoleChangeHandler
	backgroundWorkHandler BackgroundWorkHandler
	terminateHandler      TerminateHandler

	// activeLifecycleCall is the running lifecycle call, or lifecycleCallNone.
	// A panic in the call also resets it to lifecycleCallNone.
	activeLifecycleCall lifecycleCall

	// ackID is the next ServiceAck id to send. Starts 0. Advances only on a
	// successful drain send.
	ackID int64

	// controlOnFragment is the cached Poll handler. NewAgent builds the handler
	// once so each controlWork tick does not allocate a method value.
	controlOnFragment func([]byte, int)

	// unreadyRequestBound is the minimum logPosition among unready request-kind
	// entries after the last sweep. hasUnreadyRequestBound is false when no such
	// entry exists. DoWork uses the pair as the log poll upper bound.
	unreadyRequestBound    int64
	hasUnreadyRequestBound bool

	// startAckDeadlineNs is the nowNs value that ends the wait for a failed
	// start ack. The first tick after a failed Start sets it, and sets
	// startAckDeadlineArmed. Until the agent marks the Start error, DoWork
	// drains only the start entry.
	startAckDeadlineNs    int64
	startAckDeadlineArmed bool

	// startAckTimeoutNs is the failed-start wait. NewAgent copies
	// StartAck.Timeout. Zero or negative takes DefaultRecoveryTimeout.
	startAckTimeoutNs int64

	// terminationPosition is the last delivered position, or nullValue.
	terminationPosition int64

	// terminationAckRequired is the ack flag of the last delivered frame.
	terminationAckRequired bool

	terminationSendFailures int

	started bool
	closed  bool

	// serviceActive holds from a successful Start until the stop or Close.
	serviceActive bool

	// terminating holds while Terminate and the termination drain run.
	terminating bool

	terminated bool

	role        codecs.Role
	memberID    int32
	logPosition int64
	clusterTime int64
}

// logJoinEvent holds one delivered JoinLog not yet bound, or the join just
// stored for handoff while an older image still drains to the cap.
type logJoinEvent struct {
	// addFailed defers the retry wait to the next joinWork. A failed add can
	// block past its own tick.
	addFailed bool

	// addRetryNs is the earliest nowNs for the next log subscription add.
	addRetryNs    int64
	addRetryArmed bool

	logPosition    int64
	maxLogPosition int64
	memberID       int32
	logSessionID   int32
	logStreamID    int32
	isStartup      bool
	role           codecs.Role
	logChannel     string
}

// joinAddRetryIntervalNs is the wait between log subscription adds after an
// add error. An add can block DoWork, so a failed add does not retry each tick.
const joinAddRetryIntervalNs = int64(time.Second)

// coordinationAckKind discriminates the readiness rule of one entry.
type coordinationAckKind uint8

const (
	// ackKindJoin: ready at join completion or supersession.
	ackKindJoin coordinationAckKind = iota

	// ackKindRequest: ready when the agent log position equals the requested
	// position.
	ackKindRequest

	// ackKindSnapshot: ready when the take resolves. No sweep rule.
	ackKindSnapshot

	// ackKindStart: queued at NewAgent. Ready after start work, on success
	// or failure.
	ackKindStart

	// ackKindTermination: queued ready at the stop, behind every other entry.
	ackKindTermination
)

// coordinationAck is one pending ServiceAck. Drain is prefix-gated: only a
// ready first queued entry may send.
//
// Readiness:
//   - start: queued at NewAgent. Ready after start work, on success or
//     failure. A nil Service counts as success. After a Start error,
//     RelevantID on the start entry is NULL_VALUE.
//   - join: ready at bind or supersession
//   - request: ready when the log reaches or passes the entry position.
//     A passed request moves logPosition to the agent log position.
//   - snapshot: ready when the take resolves, on success or failure
//   - termination: ready when queued
//
// When the module asks for the termination ack, the stop readies each unready
// entry at its own queued position.
//
// failure stores a fatal Start or take cause whose ack has not sent.
type coordinationAck struct {
	logPosition int64
	kind        coordinationAckKind
	ready       bool
	relevantID  int64
	failure     error
}

// Compile-time assertion: Agent satisfies Cluster.
var _ Cluster = (*Agent)(nil)

// NewAgent builds an Agent from cfg. The adapter starts without an Image.
// memberID starts at -1 and logPosition at 0 (cold-start service).
//
// When StartAck is set and LogPosition is non-negative, NewAgent queues one
// unready start entry. A negative LogPosition does not queue an entry. [Open]
// refuses a negative LogPosition.
func NewAgent(cfg AgentConfig) *Agent {
	a := &Agent{
		serviceID:               cfg.ServiceID,
		service:                 cfg.Service,
		committedPosition:       cfg.CommittedPosition,
		subscriptions:           cfg.Subscriptions,
		control:                 cfg.Control,
		moduleProxy:             cfg.ModuleProxy,
		errorHandler:            cfg.ErrorHandler,
		snapshotChannel:         cfg.SnapshotChannel,
		snapshotStreamID:        cfg.SnapshotStreamID,
		timeUnit:                clusterwire.ClusterTimeUnit_NULL_VALUE,
		lastSnapshotRecordingID: nullValue,
		memberID:                -1,
		sessions:                make(map[int64]*containerClientSession),
		egressLeadershipTermID:  nullValue,
		terminationPosition:     nullValue,
	}
	if a.snapshotChannel == "" {
		a.snapshotChannel = DefaultSnapshotChannel
	}
	if a.snapshotStreamID == 0 {
		a.snapshotStreamID = DefaultSnapshotStreamID
	}
	if client := cfg.Archive; client != nil {
		a.archive = client
		a.newSnapshotStart = func(streamID int32, channel []byte) snapshotStartRequest {
			return client.NewAsyncStartRecording(
				streamID,
				archivewire.SourceLocation_LOCAL,
				true,
				channel,
			)
		}
	}
	if !isNilOrTypedNil(cfg.SnapshotPublications) {
		a.snapshotPublications = cfg.SnapshotPublications
	}
	if !isNilOrTypedNil(cfg.ResponsePublications) {
		a.responsePublications = cfg.ResponsePublications
	}
	if !isNilOrTypedNil(cfg.Transport) {
		a.transport = cfg.Transport
	}
	a.leadershipHandler, _ = cfg.Service.(NewLeadershipTermEventHandler)
	a.snapshotHandler, _ = cfg.Service.(TakeSnapshotHandler)
	a.roleChangeHandler, _ = cfg.Service.(RoleChangeHandler)
	a.backgroundWorkHandler, _ = cfg.Service.(BackgroundWorkHandler)
	a.terminateHandler, _ = cfg.Service.(TerminateHandler)
	a.controlOnFragment = a.handleControlFragment
	a.clientSessions = a.walkClientSessions
	a.adapter = NewBoundedLogAdapter(a, cfg.FragmentLimit)
	if sa := cfg.StartAck; sa != nil && sa.LogPosition >= 0 {
		a.coordinationAcks = append(a.coordinationAcks, coordinationAck{
			logPosition: sa.LogPosition,
			kind:        ackKindStart,
			relevantID:  sa.RelevantID,
		})
		a.startAckTimeoutNs = sa.Timeout.Nanoseconds()
		if a.startAckTimeoutNs <= 0 {
			a.startAckTimeoutNs = DefaultRecoveryTimeout.Nanoseconds()
		}
	}
	return a
}

// DoWork runs one duty tick. nowNs is a monotonic time in nanoseconds. Take
// deadlines and the start ack deadline use nowNs. A clock that never advances
// does not fire a deadline. A lost START or STOP signal then stalls the take
// forever. A Start error on a stream that never connects then keeps DoWork in
// the start ack drain.
//
// Behaviour:
//   - DoWork checks Transport before every other step. On termination, DoWork
//     marks ErrTransportTerminating. DoWork then returns 0.
//   - After a Start error, each tick drains only the start entry, until the
//     agent marks the service failure.
//   - A fatal mark skips the remaining steps of the tick.
//   - A panic from Start propagates out of DoWork. The start ack does not
//     send. The agent does not mark a failure. The caller must not call
//     DoWork again.
//   - While the termination drain runs, each tick only sends queued acks.
//     DoWork returns 0 after the stop.
//   - A panic from Terminate propagates out of DoWork. A later DoWork runs the
//     termination drain.
//
// Returns the work count.
//
// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe from
// other goroutines.
func (a *Agent) DoWork(nowNs int64) int {
	if a.closed || a.serviceFailure != nil || a.terminated {
		return 0
	}

	if a.markIfTransportTerminating() {
		return 0
	}

	if a.terminating {
		return a.terminationDrainWork()
	}

	work := 0

	if !a.started {
		a.started = true
		work++
		if err := a.callStart(); err != nil {
			a.failAckEntry(ackKindStart, fmt.Errorf("%w: %w", ErrStart, err))
			if a.serviceFailure != nil {
				return work
			}
			return work + a.drainReadyCoordinationAck()
		}

		a.serviceActive = true
		a.markAckReady(ackKindStart)
	}
	if a.pendingStartFailure() != nil {
		return work + a.failedStartAckWork(nowNs)
	}

	if a.archive != nil {
		work += a.archive.Poll()
	}

	if a.take != nil {
		work += a.snapshotTakeWork(nowNs)
		if a.serviceFailure != nil {
			return work
		}
	}

	work += a.controlWork()
	if a.serviceFailure != nil {
		return work
	}

	if len(a.responseAdds) > 0 {
		work += a.responseAddWork()

		if a.serviceFailure != nil {
			return work
		}
	}

	if a.pendingJoin != nil && a.adapter.Image() == nil {
		work += a.joinWork(nowNs)
		if a.serviceFailure != nil {
			return work
		}
	}

	polled := 0
	if a.adapter.Image() != nil && a.committedPosition != nil {
		bound := a.committedPosition()
		if a.hasUnreadyRequestBound {
			bound = min(bound, a.unreadyRequestBound)
		}
		if a.take != nil {
			bound = min(bound, a.take.logPosition)
		}

		if a.terminationPosition != nullValue {
			bound = min(bound, a.terminationPosition)
		}
		polled = a.adapter.Poll(bound)
		work += polled
		if a.serviceFailure != nil {
			return work
		}
	}

	a.maybeUnbind(polled)

	work += a.terminationWork()

	if a.terminating || a.terminated || a.serviceFailure != nil {
		return work
	}
	work += a.backgroundWork(nowNs)
	return work
}

// Close calls Terminate first when the service started and did not stop.
//
// Close marks a fatal Start or take cause that still waits for its ack, unless
// the agent has already failed. The waiting ack does not send. Close does not
// report teardown errors. A take that Close ends does not send an ack.
//
// Close ends a running termination drain. The queued acks do not send. When
// the module asked for the ack, Close reports ErrTerminationAckPublish, unless
// the agent has already failed.
//
// Registry entries stay.
//
// Idempotent. The Agent is inert after Close. A Terminate panic skips the
// teardown. A second Close runs the teardown and does not call Terminate.
//
// Ownership: closes the snapshot publication and start request of a running
// take. Ends every in-flight response add and closes every response
// publication. Closes the log subscription Agent added. Does not close
// Images.
//
// Concurrency: a service callback must not call Close.
func (a *Agent) Close() {
	if a.closed {
		return
	}

	a.callTerminate()
	if err := a.pendingAckFailure(); err != nil {
		a.handleError(err)
	}

	if a.terminating {
		a.abandonTerminationDrain(aeron.Closed)
	}
	a.closed = true
	a.closeSnapshotTake()
	a.disconnectEgress()
	a.adapter.SetImage(nil)
	a.closeLogSubscription()
}

// IsClosed reports whether Close has run. DoWork returns 0 after Close.
func (a *Agent) IsClosed() bool {
	return a.closed
}

// Role returns the role adopted from the last bound JoinLog, or
// [codecs.RoleFollower] after unbind or before the first bind.
func (a *Agent) Role() codecs.Role {
	return a.role
}

// MemberID returns the member id from the last bound JoinLog, or -1 before the
// first bind.
func (a *Agent) MemberID() int32 {
	return a.memberID
}

// ServiceID returns [AgentConfig.ServiceID].
func (a *Agent) ServiceID() int32 {
	return a.serviceID
}

// LogPosition returns the last consumed frame-end position, including padding
// adoption on unbind or at a termination position. A recovery load in [Open]
// seeds it.
func (a *Agent) LogPosition() int64 {
	return a.logPosition
}

// ClusterTime returns the timestamp of the last dispatched log event. A
// recovery load in [Open] seeds it.
func (a *Agent) ClusterTime() int64 {
	return a.clusterTime
}

// ScheduleTimer implements [Cluster.ScheduleTimer].
func (a *Agent) ScheduleTimer(correlationID, deadline int64) bool {
	if a.refuseInsideLifecycleCall(refusedScheduleTimer) || !a.canPublish() {
		return false
	}
	result := a.moduleProxy.PublishScheduleTimer(codecs.ScheduleTimerFields{
		CorrelationID: correlationID,
		Deadline:      deadline,
	})
	return a.acceptOffer(result, ErrTimerPublish, nil)
}

// CancelTimer implements [Cluster.CancelTimer].
func (a *Agent) CancelTimer(correlationID int64) bool {
	if a.refuseInsideLifecycleCall(refusedCancelTimer) || !a.canPublish() {
		return false
	}
	result := a.moduleProxy.PublishCancelTimer(codecs.CancelTimerFields{
		CorrelationID: correlationID,
	})
	return a.acceptOffer(result, ErrTimerPublish, nil)
}

func (a *Agent) acceptOffer(result int64, cause, inner error) bool {
	if result > 0 {
		return true
	}
	if !aeron.IsOfferRetriable(result) {
		if inner != nil {
			a.handleError(fmt.Errorf("%w: %d: %w", cause, result, inner))
		} else {
			a.handleError(fmt.Errorf("%w: %d", cause, result))
		}
	}
	return false
}

// SessionCount returns the number of open sessions in the log-derived registry.
func (a *Agent) SessionCount() int {
	return len(a.sessions)
}

// HasSession reports whether sessionID is present in the registry.
func (a *Agent) HasSession(sessionID int64) bool {
	_, ok := a.sessions[sessionID]
	return ok
}

// ClientSession implements [Cluster.ClientSession].
func (a *Agent) ClientSession(id int64) (ClientSession, bool) {
	session, ok := a.sessions[id]
	if !ok {
		return nil, false
	}
	return session, true
}

// ClientSessions implements [Cluster.ClientSessions].
func (a *Agent) ClientSessions() iter.Seq[ClientSession] {
	return a.clientSessions
}

// PendingResponsePublicationAdds returns the number of sessions with a
// response publication add in flight.
func (a *Agent) PendingResponsePublicationAdds() int {
	return len(a.responseAdds)
}

// ResponsePublicationCount returns how many sessions hold a response
// publication. A non-leader or unbound agent returns 0.
func (a *Agent) ResponsePublicationCount() int {
	count := 0
	for _, session := range a.sessionList {
		if session.publication != nil {
			count++
		}
	}
	return count
}

// HasPendingJoin reports whether a JoinLog is stored and not yet bound.
func (a *Agent) HasPendingJoin() bool {
	return a.pendingJoin != nil
}

// IsLogBound reports whether a log Image is bound on the adapter.
func (a *Agent) IsLogBound() bool {
	return a.adapter.Image() != nil
}

// PendingCoordinationAcks returns the number of coordination-ack entries not
// yet drained (all entries still held).
func (a *Agent) PendingCoordinationAcks() int {
	return len(a.coordinationAcks)
}

// ReadyCoordinationAcks returns how many leading ack queue entries are ready to
// send. Counts the ready prefix only. The scan stops at the first unready
// entry. Drain is prefix-gated on the same rule.
func (a *Agent) ReadyCoordinationAcks() int {
	n := 0
	for i := range a.coordinationAcks {
		if !a.coordinationAcks[i].ready {
			break
		}
		n++
	}
	return n
}

// NextAckID returns the next ServiceAck id this service will send. Starts at 0.
// Advances only on a successful send, so the value equals the sent-ack count.
func (a *Agent) NextAckID() int64 {
	return a.ackID
}

// LastSnapshotRecordingID returns the RelevantID of the last sent snapshot
// ack, or -1 before any snapshot ack sends. A sent ack of a failed take
// carries -1.
func (a *Agent) LastSnapshotRecordingID() int64 {
	return a.lastSnapshotRecordingID
}

// Returns the first fatal service error, or nil. The result wraps ErrFailed
// and the cause. A non-nil ErrorHandler receives the same non-nil result.
func (a *Agent) ServiceFailure() error {
	return a.serviceFailure
}

// handleJoinLog is the JoinLog delivery entry. Unit tests and the control poll
// call it in-package.
//
// Order:
//  1. When a join is already pending, mark its coordinationAck ready
//     (supersession).
//  2. Append one join-kind coordinationAck for event.logPosition, ready false.
//  3. Cap a bound image at event.logPosition (handoff).
//  4. Store event as pendingJoin.
func (a *Agent) handleJoinLog(event logJoinEvent) {
	if a.closed {
		return
	}

	if a.pendingJoin != nil {
		a.markAckReady(ackKindJoin)
	}

	a.coordinationAcks = append(a.coordinationAcks, coordinationAck{
		logPosition: event.logPosition,
		kind:        ackKindJoin,
		relevantID:  nullValue,
	})

	if a.adapter.Image() != nil {
		a.adapter.SetMaxLogPosition(event.logPosition)
	}

	a.pendingJoin = &event
}

func (a *Agent) pendingStartFailure() error {
	if len(a.coordinationAcks) == 0 {
		return nil
	}
	e := a.coordinationAcks[0]
	if e.kind != ackKindStart {
		return nil
	}
	return e.failure
}

// failedStartAckWork drains the failed start entry. The first call sets
// startAckDeadlineNs, so a slow Start does not use up the wait. At
// startAckDeadlineNs, failedStartAckWork marks the Start error without the ack.
//
// Returns 1 when the ack sends. Returns 0 otherwise.
func (a *Agent) failedStartAckWork(nowNs int64) int {
	if !a.startAckDeadlineArmed {
		a.startAckDeadlineArmed = true
		a.startAckDeadlineNs = deadlineAfter(nowNs, a.startAckTimeoutNs)
	}
	work := a.drainReadyCoordinationAck()
	if a.serviceFailure == nil && nowNs >= a.startAckDeadlineNs {
		if err := a.pendingStartFailure(); err != nil {
			a.handleError(err)
		}
	}
	return work
}

// deadlineAfter returns nowNs plus waitNs, capped at math.MaxInt64.
func deadlineAfter(nowNs, waitNs int64) int64 {
	if nowNs > 0 && waitNs > math.MaxInt64-nowNs {
		return math.MaxInt64
	}

	return nowNs + waitNs
}

// failAckEntry sets the first unready entry of kind ready with RelevantID
// NULL_VALUE. A non-fatal cause reports at once. A fatal cause stays on the
// entry. The drain marks that cause after the ack. With no such entry, or no
// ack sender, a fatal cause marks at once.
func (a *Agent) failAckEntry(kind coordinationAckKind, cause error) {
	e := a.markAckReady(kind)
	if e == nil {
		a.handleError(cause)
		return
	}

	e.relevantID = nullValue

	if isFatal(cause) && a.canPublish() {
		e.failure = cause
		return
	}

	a.handleError(cause)
}

// markAckReady sets ready on the first unready entry of kind and returns
// it. Returns nil when no entry matches. At most one start entry is queued
// (NewAgent). At most one join entry is unready (the current pending join).
func (a *Agent) markAckReady(kind coordinationAckKind) *coordinationAck {
	for i := range a.coordinationAcks {
		e := &a.coordinationAcks[i]
		if e.kind == kind && !e.ready {
			e.ready = true
			return e
		}
	}
	return nil
}

// joinWork progresses a pending join when no image is bound.
//
// Behaviour: a subscription add error reports. An add error during transport
// termination marks the failure. The add retries joinAddRetryIntervalNs after
// the next tick. A newer join adds at once.
func (a *Agent) joinWork(nowNs int64) int {
	event := a.pendingJoin
	if event == nil {
		return 0
	}

	// Reuse the subscription when channel and stream match. Otherwise close and re-add.
	if a.logSubscription != nil &&
		(a.logSubscription.Channel() != event.logChannel ||
			a.logSubscription.StreamID() != event.logStreamID) {
		a.closeLogSubscription()
	}
	if a.logSubscription == nil {
		if a.subscriptions == nil {
			return 0
		}

		if event.addFailed {
			event.addFailed = false
			event.addRetryArmed = true
			event.addRetryNs = deadlineAfter(nowNs, joinAddRetryIntervalNs)
			return 0
		}

		if event.addRetryArmed && nowNs < event.addRetryNs {
			return 0
		}

		sub, err := a.subscriptions.AddSubscription(event.logStreamID, event.logChannel)
		if err != nil {
			event.addFailed = true
			a.handleError(a.withTransportTermination(
				fmt.Errorf("%w: %w", ErrLogSubscriptionAdd, err),
			))
			return 0
		}
		// No subscription yet: the next tick retries the add.
		if sub == nil {
			return 0
		}
		a.logSubscription = sub
	}

	image, found := a.logSubscription.ImageBySessionID(event.logSessionID)
	if !found {
		return 0
	}

	joinPosition := image.JoinPosition()
	if event.logPosition != a.logPosition || joinPosition != event.logPosition {
		a.handleError(fmt.Errorf(
			"%w: logPosition=%d eventPosition=%d joinPosition=%d",
			ErrLogNotContiguous,
			a.logPosition,
			event.logPosition,
			joinPosition,
		))
		return 0
	}

	a.adapter.SetImage(image)
	a.adapter.SetMaxLogPosition(event.maxLogPosition)
	a.memberID = event.memberID
	a.markAckReady(ackKindJoin)
	a.joinEgress(event)
	a.pendingJoin = nil
	a.changeRole(event.role)
	return 1
}

// maybeUnbind releases a done image after a poll with no progress, unless a
// snapshot take runs. It advances logPosition to the image position to cover
// consumed padding. It closes the subscription, so a later join on the same
// session id does not find the done image.
func (a *Agent) maybeUnbind(polled int) {
	if a.take != nil || polled != 0 || !a.adapter.isDone() {
		return
	}
	a.logPosition = max(a.adapter.Image().Position(), a.logPosition)
	a.adapter.SetImage(nil)
	a.closeLogSubscription()
	a.disconnectEgress()
	a.changeRole(codecs.RoleFollower)
}

// closeLogSubscription closes the log subscription, if one is open.
func (a *Agent) closeLogSubscription() {
	if a.logSubscription == nil {
		return
	}

	_ = a.logSubscription.Close()
	a.logSubscription = nil
}

// handleError passes err to the error handler. When isFatal reports true,
// handleError stores err, wrapped with ErrFailed, as the service failure. The
// handler then receives the wrapped error. handleError drops err after Close or
// an earlier mark.
func (a *Agent) handleError(err error) {
	if a.closed || a.serviceFailure != nil {
		return
	}

	if isFatal(err) {
		a.serviceFailure = fmt.Errorf("%w: %w", ErrFailed, err)
		err = a.serviceFailure
	}

	if a.errorHandler != nil {
		a.errorHandler(err)
	}
}

// isFatal reports whether err ends the service. These errors are fatal:
//   - a cause the agent cannot recover from: a Start error, a log gap or
//     dispatch fault, a terminal ack code, a terminating transport client,
//     or a termination position the log passed
//   - an add on a closed or terminating transport client
//   - an archive STORAGE_SPACE error
func isFatal(err error) bool {
	switch {
	case errors.Is(err, ErrStart),
		errors.Is(err, ErrLogNotContiguous),
		errors.Is(err, ErrAckPublish),
		errors.Is(err, ErrLogDispatchHeader),
		errors.Is(err, ErrLogDispatchSchema),
		errors.Is(err, ErrLogDispatchDecode),
		errors.Is(err, ErrActionPositionMismatch),
		errors.Is(err, ErrTransportTerminating),
		errors.Is(err, ErrTerminationPositionPassed),
		errors.Is(err, aeron.ErrClientClosed),
		errors.Is(err, aeron.ErrConductorTerminating):
		return true
	}

	return wrapsStorageSpace(err)
}

// wrapsStorageSpace checks every branch of err. [archive.ExceptionCode] reads
// only the first archive exception.
func wrapsStorageSpace(err error) bool {
	exception, ok := err.(*archive.Exception)
	if ok && exception.ErrorCode == archive.ErrorStorageSpace {
		return true
	}

	switch wrapped := err.(type) {
	case interface{ Unwrap() error }:
		return wrapsStorageSpace(wrapped.Unwrap())
	case interface{ Unwrap() []error }:
		return slices.ContainsFunc(wrapped.Unwrap(), wrapsStorageSpace)
	}
	return false
}

// markIfTransportTerminating returns true when it marks the failure. The
// failure wraps a pending fatal ack cause.
func (a *Agent) markIfTransportTerminating() bool {
	cause := a.transportTerminatingCause()
	if cause == nil {
		return false
	}

	a.handleError(cause)
	return true
}

// withTransportTermination wraps add error err with the termination cause when
// the transport client is terminating. isFatal does not recognize the timeout
// that a client can return when it terminates during the add.
func (a *Agent) withTransportTermination(err error) error {
	cause := a.transportTerminatingCause()
	if cause == nil {
		return err
	}
	return fmt.Errorf("%w: %w", cause, err)
}

// transportTerminatingCause returns nil until the transport client terminates.
// The cause wraps a pending fatal ack cause.
func (a *Agent) transportTerminatingCause() error {
	if a.transport == nil || !a.transport.IsTerminating() {
		return nil
	}

	pending := a.pendingAckFailure()
	switch {
	case pending == nil:
		return ErrTransportTerminating
	case errors.Is(pending, ErrTransportTerminating):
		return pending
	}
	return fmt.Errorf("%w: %w", ErrTransportTerminating, pending)
}

func (a *Agent) pendingAckFailure() error {
	for i := range a.coordinationAcks {
		if err := a.coordinationAcks[i].failure; err != nil {
			return err
		}
	}
	return nil
}

// seedRecovery runs in [Open] before the first bind. memberID and role keep
// their values until that bind.
func (a *Agent) seedRecovery(state codecs.RecoveryStateKeyFields) {
	a.logPosition = state.LogPosition
	a.clusterTime = state.Timestamp
	a.egressLeadershipTermID = state.LeadershipTermID
}

// adoptConsumed sets logPosition to the frame end and clusterTime to the event
// timestamp.
func (a *Agent) adoptConsumed(logPosition, clusterTime int64) {
	a.logPosition = logPosition
	a.clusterTime = clusterTime
}

func (a *Agent) handleSessionMessage(
	logPosition, sessionID, timestamp int64,
	payload []byte,
) aeron.ControlledAction {
	a.adoptConsumed(logPosition, timestamp)
	if a.service != nil {
		session, _ := a.ClientSession(sessionID)
		a.service.SessionMessage(session, timestamp, payload)
	}
	return aeron.ControlledContinue
}

func (a *Agent) handleSessionOpen(
	logPosition int64,
	fields codecs.SessionOpenEventFields,
) aeron.ControlledAction {
	a.adoptConsumed(logPosition, fields.Timestamp)

	if _, exists := a.sessions[fields.ClusterSessionID]; exists {
		a.handleError(fmt.Errorf("%w: %d", ErrSessionClash, fields.ClusterSessionID))
		return aeron.ControlledBreak
	}

	session := newContainerClientSession(
		a,
		fields.ClusterSessionID,
		fields.ResponseStreamID,
		fields.ResponseChannel,
		fields.EncodedPrincipal,
	)
	if a.role == codecs.RoleLeader {
		a.connectClientSession(session)

		if a.serviceFailure != nil {
			return aeron.ControlledBreak
		}
	}
	a.addClientSession(session)
	if a.service != nil {
		a.service.SessionOpen(session, fields.Timestamp)
	}
	return aeron.ControlledContinue
}

func (a *Agent) handleSessionClose(
	logPosition int64,
	fields codecs.SessionCloseEventFields,
) aeron.ControlledAction {
	a.adoptConsumed(logPosition, fields.Timestamp)

	session := a.removeClientSession(fields.ClusterSessionID)
	if session == nil {
		a.handleError(fmt.Errorf(
			"%w: session %d: %d",
			ErrUnknownSessionClose,
			fields.ClusterSessionID,
			fields.CloseReason,
		))
		return aeron.ControlledContinue
	}
	a.disconnectClientSession(session)

	if a.serviceFailure != nil {
		return aeron.ControlledBreak
	}

	if a.service != nil {
		a.service.SessionClose(session, fields.Timestamp, fields.CloseReason)
	}
	return aeron.ControlledContinue
}

func (a *Agent) handleTimerEvent(
	logPosition int64,
	fields codecs.TimerEventFields,
) aeron.ControlledAction {
	a.adoptConsumed(logPosition, fields.Timestamp)
	if a.service != nil {
		a.service.TimerEvent(fields.CorrelationID, fields.Timestamp)
	}
	return aeron.ControlledContinue
}

func (a *Agent) handleNewLeadershipTermEvent(
	logPosition int64,
	fields codecs.NewLeadershipTermEventFields,
) aeron.ControlledAction {
	a.adoptConsumed(logPosition, fields.Timestamp)
	unit, ok := resolveTimeUnit(fields.TimeUnit)
	a.timeUnit = unit
	if !ok {
		a.handleError(fmt.Errorf("%w: %d", ErrUnknownTimeUnit, fields.TimeUnit))
	}
	a.appVersion = fields.AppVersion
	a.egressLeadershipTermID = fields.LeadershipTermID
	if a.leadershipHandler != nil {
		a.leadershipHandler.NewLeadershipTermEvent(
			fields.LeadershipTermID,
			fields.LogPosition,
			fields.Timestamp,
			fields.TermBaseLogPosition,
			fields.LeaderMemberID,
			fields.LogSessionID,
			fields.TimeUnit,
			fields.AppVersion,
		)
	}
	return aeron.ControlledContinue
}

// resolveTimeUnit maps NULL_VALUE to MILLIS. Returns false for a unit outside
// MILLIS, MICROS and NANOS.
func resolveTimeUnit(
	unit clusterwire.ClusterTimeUnit,
) (clusterwire.ClusterTimeUnit, bool) {
	switch unit {
	case clusterwire.ClusterTimeUnit_NULL_VALUE:
		return clusterwire.ClusterTimeUnit_MILLIS, true
	case clusterwire.ClusterTimeUnit_MILLIS,
		clusterwire.ClusterTimeUnit_MICROS,
		clusterwire.ClusterTimeUnit_NANOS:
		return unit, true
	}
	return unit, false
}

// handleClusterAction delivers one ClusterActionRequest from the log adapter.
//
// Behaviour:
//   - A decoded LogPosition other than the frame-end position marks failure.
//     The time and position from the frame stay noted.
//   - SNAPSHOT with Flags == 0 starts a take. The log poll holds at the action
//     position until the take resolves.
func (a *Agent) handleClusterAction(
	logPosition int64,
	fields codecs.ClusterActionRequestFields,
) aeron.ControlledAction {
	a.adoptConsumed(logPosition, fields.Timestamp)

	if fields.LogPosition != logPosition {
		return a.handleDispatchFailure(ErrActionPositionMismatch)
	}

	switch fields.Action {
	case clusterwire.ClusterAction_SUSPEND, clusterwire.ClusterAction_RESUME:
		return aeron.ControlledContinue
	case clusterwire.ClusterAction_SNAPSHOT:
		if fields.Flags != 0 {
			return aeron.ControlledContinue
		}
		a.startSnapshotTake(logPosition, fields.LeadershipTermID)
		return aeron.ControlledBreak
	case clusterwire.ClusterAction_NULL_VALUE:
		fallthrough
	default:
		a.handleError(ErrUnknownClusterAction)
		return aeron.ControlledContinue
	}
}

func (a *Agent) handleDispatchFailure(err error) aeron.ControlledAction {
	a.handleError(err)
	return aeron.ControlledAbort
}
