package service

import (
	"iter"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/cluster/codecs"
	"github.com/warrenguy/luas/cluster/codecs/clusterwire"
)

// SubscriptionAdder registers subscriptions on a media driver.
//
// *aeron.Client satisfies this interface as-built.
type SubscriptionAdder interface {
	AddSubscription(streamID int32, channel string) (*aeron.Subscription, error)
}

var _ SubscriptionAdder = (*aeron.Client)(nil)

// ClusteredService is the deterministic user state machine fed from the
// committed cluster log.
//
// Behaviour: callbacks carry cluster time, not wall-clock time. Payload slices
// are valid only for the callback. Implementations must copy what they retain.
//
// Concurrency: methods run inside [Agent.DoWork].
type ClusteredService interface {
	// Start runs once, before any log event. After a non-nil error, the
	// agent tries the start ack under the rules of [StartAckConfig]. The
	// agent then marks the service failure. The failure wraps [ErrStart] and
	// the Start error. When the transport client terminates, the failure
	// also wraps [ErrTransportTerminating]. With no queued start ack, the
	// agent marks the service failure at once. The duty cycle stops.
	//
	// The agent refuses module requests and session offers inside Start.
	// See [ErrLifecycleRefused].
	Start(cluster Cluster) error

	// SessionOpen runs after the agent adds session to the registry.
	//
	// Behaviour: on the leader, session.Offer inside SessionOpen returns
	// aeron.NotConnected. The response publication add has not completed.
	SessionOpen(session ClientSession, timestamp int64)

	// SessionClose runs after the agent removes session from the registry.
	// The agent also closes the response publication, or cancels its add,
	// before SessionClose runs.
	//
	// Behaviour: on the leader, session.Offer returns aeron.NotConnected.
	SessionClose(session ClientSession, timestamp int64, reason clusterwire.CloseReason)

	// SessionMessage delivers one session message payload.
	//
	// Behaviour: session is nil when the message session id is not open.
	// Service messages arrive with a nil session. A Java module appends
	// them under a service session id, which never names a client session.
	SessionMessage(session ClientSession, timestamp int64, payload []byte)

	TimerEvent(correlationID, timestamp int64)
}

// NewLeadershipTermEventHandler is the optional extension a service implements
// to observe leadership term events (upstream default-method parity).
// NewAgent detects it once with a type assertion.
type NewLeadershipTermEventHandler interface {
	NewLeadershipTermEvent(
		leadershipTermID int64,
		logPosition int64,
		timestamp int64,
		termBaseLogPosition int64,
		leaderMemberID int32,
		logSessionID int32,
		timeUnit clusterwire.ClusterTimeUnit,
		appVersion int32,
	)
}

// TakeSnapshotHandler is the optional extension a service implements to
// write snapshot state when the cluster requests a snapshot action.
// NewAgent detects the handler once with a type assertion.
//
// Behaviour: the agent records publication in the member's Archive. The
// agent writes the begin marker, one row per open session, and the end
// marker. TakeSnapshot then offers the service state. The agent closes
// publication after TakeSnapshot returns.
//
// publication is never nil. TakeSnapshot must retry BackPressured and
// AdminAction offer codes. One Offer must not exceed
// publication.MaxMessageLength. TakeSnapshot must not close publication.
//
// A non-nil error fails the take. The agent queues one ServiceAck with
// RelevantID -1. The error is fatal when it wraps one of these:
//   - an archive STORAGE_SPACE exception
//   - aeron.ErrClientClosed or aeron.ErrConductorTerminating
//   - a sentinel of this package whose godoc states that the error is fatal
//
// A non-fatal error reaches ErrorHandler as ErrTakeSnapshot. The service keeps
// running. A fatal error marks the service failure. The mark stops the duty
// cycle.
//
// Concurrency: methods run inside [Agent.DoWork]. TakeSnapshot blocks the
// duty cycle. In invoker mode, the caller cannot run the transport client's
// DoWork meanwhile. A run past that client's InterServiceTimeout terminates
// it.
type TakeSnapshotHandler interface {
	TakeSnapshot(publication *aeron.ExclusivePublication) error
}

// LoadSnapshotHandler is the optional extension a service implements to
// restore its state from a snapshot. Open detects the handler once with a
// type assertion.
//
// Behaviour: Open calls LoadSnapshot once per snapshot load. image is the
// archive replay of this service's snapshot recording. image sits at the
// first byte TakeSnapshot wrote. LoadSnapshot reads the service's own
// format to its end. image stays open for the whole call. LoadSnapshot must
// not wait for image to close.
//
// LoadSnapshot must not close image. image is valid only during the call.
// A non-nil error fails Open.
//
// Concurrency: LoadSnapshot runs on the goroutine that calls Open. The
// call returns before the first DoWork, so before Start. In invoker mode,
// Open does not run the transport client's DoWork meanwhile. A run past that
// client's InterServiceTimeout terminates it.
type LoadSnapshotHandler interface {
	LoadSnapshot(image *aeron.Image) error
}

// RoleChangeHandler is the optional extension a service implements to
// observe the role changes of its member. NewAgent detects the handler once
// with a type assertion.
//
// Behaviour: the agent calls RoleChange only when the value Role returns
// changes. Role returns role during the call. A bind calls RoleChange with
// the JoinLog role after the agent updates session egress for the join. An
// unbind calls RoleChange with codecs.RoleFollower after the agent closes
// every response publication. A fatal mark before either call skips that call.
// Role stays unchanged. An unbind waits for a running snapshot take to end.
// RoleChange does not run during a take.
//
// The agent refuses module requests and session offers inside RoleChange.
// See [ErrLifecycleRefused].
//
// Concurrency: methods run inside [Agent.DoWork].
type RoleChangeHandler interface {
	RoleChange(role codecs.Role)
}

// BackgroundWorkHandler is the optional extension a service implements to
// run work outside the replicated state machine. NewAgent detects the
// handler once with a type assertion.
//
// Behaviour: the agent calls DoBackgroundWork once per DoWork call, after
// the log poll. The first call follows a successful Start. The agent does
// not call DoBackgroundWork after a Start error, a service failure, a stop
// at the termination position, or Close. nowNs is the DoWork argument, not
// cluster time.
//
// DoBackgroundWork must not change service state. The agent refuses module
// requests and session offers inside DoBackgroundWork. See
// [ErrLifecycleRefused].
//
// Returns the work count. DoWork adds a positive count to its own.
//
// Concurrency: methods run inside [Agent.DoWork]. A slow call delays the duty
// cycle.
type BackgroundWorkHandler interface {
	DoBackgroundWork(nowNs int64) int
}

// TerminateHandler is the optional extension a service implements to observe
// the end of its container. NewAgent detects the handler once with a type
// assertion.
//
// Behaviour: the agent calls Terminate once, when its log reaches the
// termination position from the module. Otherwise, Close calls Terminate after
// a successful Start. Cluster reads stay valid inside Terminate. The agent
// refuses module requests and session offers inside Terminate. See
// [ErrLifecycleRefused].
//
// A panic propagates out of Agent.DoWork or Agent.Close. Terminate does not run
// again. After a DoWork panic, later DoWork calls end the stop. Those calls send
// the queued acks when the module asked for the ack. After a Close panic, a
// second Close runs the teardown.
//
// A non-nil error reports one error that wraps ErrTerminate. The error is
// fatal when it wraps an error that [TakeSnapshotHandler] lists as fatal. A
// fatal error marks the service failure. The termination ack does not send.
// The stop continues after a non-fatal error.
//
// Concurrency: Terminate runs inside [Agent.DoWork] or [Agent.Close], on the
// goroutine that runs DoWork.
type TerminateHandler interface {
	Terminate(cluster Cluster) error
}

// Cluster is the service-facing proxy into member state.
//
// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe from
// other goroutines. Call from log-event callbacks, DoWork, and Terminate only.
// The agent refuses module requests and session offers inside Start, RoleChange,
// DoBackgroundWork, and Terminate. See [ErrLifecycleRefused].
type Cluster interface {
	LogPosition() int64
	ClusterTime() int64
	MemberID() int32
	Role() codecs.Role

	// ScheduleTimer requests one cluster timer for correlationID at deadline
	// (cluster time, nanoseconds). A new request reschedules an existing timer for
	// the same correlationID. Reschedule and cancel race with expiry. A TimerEvent
	// already appended still arrives. Expiry is leader-only.
	//
	// Call from log-event callbacks.
	//
	// Returns:
	//   - true when the agent sent the request frame
	//   - false inside Start, RoleChange, DoBackgroundWork, or Terminate. The
	//     call reports [ErrLifecycleRefused]. Must not retry inside that
	//     callback.
	//   - false when the proxy is absent or the publication is unbound.
	//     Retry from a later callback.
	//   - false after the container closes. Must not retry.
	//   - false when the offer returns a retriable code. Retry from a
	//     later callback.
	//   - false when the offer returns a terminal publication code. The agent
	//     reports ErrTimerPublish. Must not retry.
	ScheduleTimer(correlationID, deadline int64) bool

	// CancelTimer asks the consensus module to cancel the timer for correlationID.
	// Cancel races with expiry. A TimerEvent already appended still arrives.
	//
	// Call from log-event callbacks.
	//
	// Returns:
	//   - true when the agent sent the request frame
	//   - false inside Start, RoleChange, DoBackgroundWork, or Terminate. The
	//     call reports [ErrLifecycleRefused]. Must not retry inside that
	//     callback.
	//   - false when the proxy is absent or the publication is unbound.
	//     Retry from a later callback.
	//   - false after the container closes. Must not retry.
	//   - false when the offer returns a retriable code. Retry from a
	//     later callback.
	//   - false when the offer returns a terminal publication code. The agent
	//     reports ErrTimerPublish. Must not retry.
	CancelTimer(correlationID int64) bool

	// CloseClientSession asks the consensus module to close the open session
	// id. The leader appends the close to the log. Every service then gets
	// SessionClose with reason SERVICE_ACTION.
	//
	// Behaviour: sends on every role. A follower's module marks the session
	// closing, so a later leader still appends the close.
	//
	// Call from log-event callbacks or DoWork, outside Start, RoleChange,
	// DoBackgroundWork, and Terminate.
	//
	// Returns:
	//   - false inside Start, RoleChange, DoBackgroundWork, or Terminate. The
	//     call reports [ErrLifecycleRefused]. Must not retry inside that
	//     callback.
	//   - true when this member sent the frame, now or on an earlier call
	//   - false when id is not an open session. The call reports
	//     [ErrCloseUnknownSession]. Must not retry.
	//   - false when the proxy is absent or the publication is unbound.
	//     Retry from a later callback.
	//   - false after the container closes. Must not retry.
	//   - false when the offer returns a retriable code. Retry from a
	//     later callback.
	//   - false when the offer returns a terminal publication code. The agent
	//     reports ErrCloseSessionPublish. Must not retry.
	CloseClientSession(id int64) bool

	// ClientSession returns the open session with id, or nil and false when
	// id is not open.
	ClientSession(id int64) (ClientSession, bool)

	// ClientSessions returns the open sessions in ascending id order.
	//
	// Behaviour: each walk reads the live registry. The registry does not
	// change during a callback.
	//
	// Allocation: the call allocates nothing. A range loop allocates its
	// yield closure per walk. A stored yield func passed to the sequence
	// allocates nothing.
	ClientSessions() iter.Seq[ClientSession]
}
