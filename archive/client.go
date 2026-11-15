package archive

import (
	"context"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/archive/codecs"
	"github.com/warrenguy/luas/archive/codecs/archivewire"
	"github.com/warrenguy/luas/internal/transportfault"
)

// Client is a connected archive control session over one control-response
// subscription.
//
// Poll demuxes control responses and recording descriptors on the shared subscription
// through PollControlEvents.
//
// Poll never polls the underlying pollers directly and never calls DoWork.
//
// On an invoker-mode transport client, the session goroutine also calls
// DoWork.
//
// Recording signals drop while Client does not hold a session handler, an
// open AsyncStartRecording, or an open AsyncExtendRecording.
// Config.RecordingSignalHandler or SetRecordingSignalHandler sets the session
// handler. The session handler receives signals on Poll and on every blocking
// control await that polls through it. *AwaitSignal methods install a per-call
// tracker for the proof signal instead.
//
// Client holds at most one pending signal tracker. NewAsyncStartRecording and
// NewAsyncExtendRecording install the tracker of their handle. Poll and
// ListRecording send each signal to the session handler, then to that
// tracker. AsyncStartRecording lists the caller rules.
//
// Close releases the control publication and subscription. Close also releases
// the Aeron client when Connect created it. Close is idempotent.
//
// Poll and the blocking calls close the session when they observe a fault on
// the Connect-owned transport client. The blocking call that observes the
// fault returns the cause. TransportFault then returns the cause.
//
// A blocking call returns:
//   - ErrTransportTerminating on a fault of a caller-supplied transport client
//   - ControlResponseNotConnectedError when an empty poll finds no connected
//     response image
//
// The session stays open in both cases.
//
// Not safe for concurrent use.
type Client struct {
	controlSessionID int64
	archiveID        int64

	proxy            *Proxy
	controlPoller    *ControlResponsePoller
	descriptorPoller *RecordingDescriptorPoller
	signalHandler    RecordingSignalHandler

	// Nil drops each unsolicited archive ERROR.
	errorResponseHandler func(error)

	// pendingSignalTracker receives each signal after signalHandler while an
	// AsyncStartRecording or AsyncExtendRecording holds the slot. Nil marks an
	// empty slot.
	pendingSignalTracker RecordingSignalHandler

	controlSubscription *aeron.Subscription
	controlAssembler    *FragmentAssembler
	aeronClient         *aeron.Client
	ownsAeronClient     bool
	cfg                 Config
	closed              bool

	// transportFault reads the termination cause of a connect-owned client.
	// It is nil when the caller supplied Config.Aeron.
	transportFault *transportfault.Monitor

	// transportFaultCause is the error that closed the session on a transport
	// client fault. Nil until a fault closes the session.
	transportFaultCause error
}

// Connect performs the blocking archive control connect handshake and returns a session
// wrapper.
//
// Connect calls DoWork on a caller-supplied invoker-mode transport client
// before each AsyncConnect.Poll.
//
// Connect loops until the handshake completes, ctx is done, or ConnectTimeout
// elapses. Each step paces with cfg.IdleStrategy.Idle(0). Connect calls
// IdleStrategy.Reset on return so a shared backoff instance does not stay in
// PARKING before the post-connect duty cycle.
//
// When cfg.Aeron is nil, ctx also bounds owned transport attach (CNC ready and
// driver heartbeat) via NewAsyncConnect. Shared Aeron skips that wait.
//
// Outside response mode, NewAsyncConnect draws a session id before the
// handshake. The transport client DriverTimeout bounds the draw, not ctx.
// Connect returns a draw error as is, not ctx.Err().
//
// NewAsyncConnect normalizes and validates cfg; pre-normalizing is harmless.
//
// On handshake failure or ctx cancellation, partial resources are released via
// AsyncConnect.Close.
//
// Returns ctx.Err() when the context is done before handshake success
// (context.Canceled or context.DeadlineExceeded). Connect timeout errors apply only
// while the context remains active. A completed handshake returns a session even if ctx
// is canceled concurrently on that iteration.
//
// Not safe for concurrent use during connect.
func Connect(ctx context.Context, cfg Config) (*Client, error) {
	connect, err := NewAsyncConnect(ctx, cfg)
	if err != nil {
		return nil, err
	}

	notifyConnectAsyncConnectObserver(connect)

	result, err := awaitConnect(ctx, connect)
	if err != nil {
		_ = connect.Close()
		return nil, err
	}
	return newClient(result), nil
}

func awaitConnect(ctx context.Context, connect *AsyncConnect) (*ConnectResult, error) {
	idle := connect.cfg.IdleStrategy
	defer idle.Reset()

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		notifyAwaitConnectStepObserver(connect)
		if !connect.ownsAeronClient && connect.client.HasConductorInvoker() {
			connect.client.DoWork()
		}
		result, err := connect.Poll()
		if err != nil {
			return nil, aeron.PreferContextError(ctx, err)
		}
		if result != nil {
			return result, nil
		}
		idle.Idle(0)
	}
}

// NewClient builds a session from a successful connect result.
//
// AsyncConnect.Poll moves the control handles into result. NewClient moves
// them into the session. Close the session afterwards. Do not Close
// AsyncConnect after Poll returns a result. A nil result panics.
func NewClient(result *ConnectResult) *Client {
	return newClient(result)
}

func newClient(result *ConnectResult) *Client {
	client := &Client{
		controlSessionID:     result.ControlSessionID,
		archiveID:            result.ArchiveID,
		proxy:                result.Proxy,
		controlPoller:        result.Poller,
		descriptorPoller:     NewRecordingDescriptorPoller(),
		signalHandler:        result.Config.RecordingSignalHandler,
		errorResponseHandler: result.Config.ErrorResponseHandler,
		controlSubscription:  result.ControlSubscription,
		controlAssembler:     NewFragmentAssembler(nil),
		aeronClient:          result.Aeron,
		ownsAeronClient:      result.OwnsAeronClient,
		cfg:                  result.Config,
		transportFault:       result.transportFault,
	}
	if client.controlPoller != nil {
		client.controlPoller.ResetCapture()
	}
	return client
}

// ControlSessionID returns the archive control session id assigned at connect.
func (client *Client) ControlSessionID() int64 {
	return client.controlSessionID
}

// ArchiveID returns the archive id reported at connect. It returns
// cnc.NullValue when the archive protocol is older than 1.11.0.
func (client *Client) ArchiveID() int64 {
	return client.archiveID
}

// Config returns the normalized archive config stored at connect.
//
// Nil receiver returns a zero Config.
func (client *Client) Config() Config {
	if client == nil {
		return Config{}
	}
	return client.cfg
}

// Close sends a best-effort CloseSessionRequest, then releases the control publication
// and subscription. When Connect created the underlying Aeron client, Close closes it
// too; a user-supplied client is left open.
//
// Close is idempotent: after the first call it releases nothing further and returns nil.
func (client *Client) Close() error {
	if client.closed {
		return nil
	}
	client.closed = true
	if client.proxy != nil {
		_, _ = client.proxy.TryCloseSession(client.controlSessionID)
		_ = client.proxy.Close()
	}
	if client.controlSubscription != nil {
		_ = client.controlSubscription.Close()
		client.controlSubscription = nil
	}
	if client.ownsAeronClient && client.aeronClient != nil {
		_ = client.aeronClient.Close()
		client.aeronClient = nil
	}
	return nil
}

// TransportFault returns the error that closed the session on an owned
// transport client fault. The error wraps ErrTransportTerminating and the
// conductor cause. Returns nil when a fault did not close the session.
func (client *Client) TransportFault() error {
	return client.transportFaultCause
}

// closeOnTransportFault closes the session when the owned transport client
// has terminated. Returns true on close.
func (client *Client) closeOnTransportFault() bool {
	if client.closed {
		return false
	}
	cause := transportTerminatingError(client.transportFault, client.aeronClient)
	if cause == nil {
		return false
	}
	client.transportFaultCause = cause
	_ = client.Close()
	return true
}

// transportFaultError returns the error that ends a blocking call after a
// transport fault. Returns nil when a transport fault is not pending. Closes
// the session on an owned transport client fault.
//
// Concurrency: call from the goroutine that runs [Client.Poll]. Not safe from
// other goroutines. Must not run inside a transport client callback.
func (client *Client) transportFaultError() error {
	if client.ownsAeronClient {
		client.closeOnTransportFault()
		return client.transportFaultCause
	}
	return transportTerminationSentinel(client.aeronClient)
}

func transportTerminationSentinel(aeronClient *aeron.Client) error {
	if aeronClient.IsTerminating() {
		return ErrTransportTerminating
	}
	return nil
}

// IsClosed reports whether Close has been invoked.
func (client *Client) IsClosed() bool {
	if client == nil {
		return true
	}
	return client.closed
}

// IsConnected reports whether the control response subscription has a
// connected image.
//
// Returns false after Close, including when an owned transport client fault
// closes the session. Returns false once the transport client force-closes
// its subscriptions on termination.
func (client *Client) IsConnected() bool {
	if client == nil || client.closed || client.controlSubscription == nil {
		return false
	}
	return client.controlSubscription.IsConnected()
}

// Poll runs one demux poll of the control-response subscription. Poll sends
// control responses and recording descriptors to the session pollers. Poll
// sends each recording signal to the session handler, then to the pending
// signal tracker of an open AsyncStartRecording or AsyncExtendRecording. Poll
// drops signals when neither exists. Poll never calls DoWork.
//
// Poll sends each unsolicited ERROR of this session to the error response
// handler after the image poll.
//
// Poll closes the session when the owned transport client has terminated,
// then returns 0. TransportFault returns the cause. The owned client Close
// sleeps AeronConfig.ClientCloseLingerDuration.
//
// Concurrency: invoker-only. Poll takes the transport client lock after an
// owned client error. Must not run inside a transport client callback.
func (client *Client) Poll() int {
	return client.pollControlEvents(client.sessionPollTargets(client.signalTarget()))
}

// pollControlEvents closes the session on an owned transport client fault,
// else runs one demux poll through targets.
func (client *Client) pollControlEvents(targets ControlPollTargets) int {
	if client.closeOnTransportFault() {
		return 0
	}
	return PollControlEvents(
		client.controlSubscription,
		DefaultControlResponseFragmentLimit,
		client.controlAssembler,
		targets,
	)
}

func (client *Client) listRecordingPollTargets() ControlPollTargets {
	return ControlPollTargets{
		SignalHandler:        client.signalTarget(),
		ControlSessionID:     client.controlSessionID,
		DescriptorPoller:     client.descriptorPoller,
		ErrorResponseHandler: client.errorResponseHandler,
	}
}

func (client *Client) pollListRecording() int {
	return client.pollControlEvents(client.listRecordingPollTargets())
}

// pollRecordingSignalAwait routes control-event polls through targets for the whole
// await (offer retry and response loop).
func (client *Client) pollRecordingSignalAwait(targets ControlPollTargets) func() int {
	return func() int {
		return client.pollControlEvents(targets)
	}
}

// SetRecordingSignalHandler overrides Config.RecordingSignalHandler for Poll and
// for blocking control awaits that poll through it. *AwaitSignal routes proof
// signals to the per-call tracker, not this handler.
//
// handler does not replace the pending signal tracker of an open
// AsyncStartRecording or AsyncExtendRecording.
func (client *Client) SetRecordingSignalHandler(handler RecordingSignalHandler) {
	client.signalHandler = handler
}

// SetErrorResponseHandler sets the handler for unsolicited archive ERROR
// responses. Nil drops each ERROR. Config.ErrorResponseHandler sets the same
// handler at connect.
//
// The handler receives each own-session ERROR whose correlation id does not
// match an outstanding request. Poll and every blocking control call run the
// handler.
//
// The error is an *Exception with the archive code, correlation id and
// message. Match it with ExceptionCode or AsException.
//
// Concurrency: call from the goroutine that runs [Client.Poll]. Not safe from
// other goroutines. The handler runs on the goroutine that called Poll or a
// blocking method.
//
// The handler may call Close and the accessors. The handler must not call
// Poll, a blocking method or a New* request builder. A nested call can drop
// the reply that a waiting call needs. The waiting call then returns
// ControlResponseTimeoutError. The client does not check the rule.
//
// A Close from the handler ends a blocking call that has sent its request.
// The call returns ControlResponseNotConnectedError on its next empty poll.
func (client *Client) SetErrorResponseHandler(handler func(error)) {
	client.errorResponseHandler = handler
}

// StartRecording offers a StartRecordingRequest2 and blocks until the archive replies
// with a matching ControlResponse. The returned RelevantID is the subscription
// registration id on the wire, not the catalog recording id; await RecordingSignal_START
// for the assigned recording id.
//
// autoStop true releases the recording subscription when the recording
// session ends. autoStop false keeps the subscription until StopRecording on
// the same channel and stream. Pass false only when the subscription must
// outlive the recording session.
//
// Returns ctx.Err() when the context is done before a response is received.
func (client *Client) StartRecording(
	ctx context.Context,
	streamID int32,
	sourceLocation archivewire.SourceLocation,
	autoStop bool,
	channel []byte,
) (int64, error) {
	return client.sessionControlOps(client.Poll).startRecording(
		ctx,
		streamID,
		sourceLocation,
		booleanTypeFromBool(autoStop),
		channel,
	)
}

// StartRecordingAwaitSignal offers a StartRecordingRequest2 and blocks until tracker
// latches the correlated START RecordingSignalEvent.
//
// Returns the catalog recording id from that signal.
//
// Behaviour:
//   - the proof check reads tracker before the control response
//   - a latched START wins when both are present
//   - ERROR fails the wait only when START is not yet latched
//   - a matching OK alone does not complete the wait
//   - the control response carries the subscription registration id, not the
//     catalog recording id
//   - the archive sends the START signal only after an image appears on the
//     recording subscription
//
// autoStop: see StartRecording.
//
// ControlResponseTimeout bounds the whole wait after a successful offer.
//
// On timeout or on ControlResponseNotConnectedError:
//   - the recording may still be started
//   - the caller owns the cleanup
//
// Use StartRecording when only an OK ControlResponse proof is required.
//
// tracker must be non-nil.
//
// The tracker carries the START signal proof and is dereferenced on every proof check.
//
// Returns ctx.Err() when the context is done before proof is received.
func (client *Client) StartRecordingAwaitSignal(
	ctx context.Context,
	streamID int32,
	sourceLocation archivewire.SourceLocation,
	autoStop bool,
	channel []byte,
	tracker *StartSignalTracker,
) (recordingID int64, err error) {
	targets := client.sessionPollTargets(tracker)
	err = client.sessionControlOps(
		client.pollRecordingSignalAwait(targets),
	).startRecordingAwaitSignal(
		ctx,
		streamID,
		sourceLocation,
		booleanTypeFromBool(autoStop),
		channel,
		targets,
		tracker,
	)
	if err != nil {
		return 0, err
	}
	return tracker.StartedRecordingID(), nil
}

// ExtendRecordingAwaitSignal offers an ExtendRecordingRequest2 and blocks until
// tracker latches the correlated EXTEND RecordingSignalEvent.
//
// The returned id is the recording id the caller asked to extend. The archive
// does not assign a new recording id.
//
// Behaviour:
//   - a matching OK alone does not complete the wait. The archive sends OK
//     before catalog image validation
//   - a latched EXTEND completes the wait. The archive sends EXTEND only after
//     that validation passes
//   - ERROR fails the wait only when EXTEND is not yet latched
//
// tracker must be non-nil and built with the same recordingID passed to this
// method:
//   - NewExtendSignalTracker filters EXTEND by that id
//   - a mismatch ignores a real EXTEND and the wait times out
//
// autoStop true:
//   - releases the recording subscription when the recording session ends
//   - makes the archive release the subscription when validation refuses the
//     extend after OK
//
// autoStop false:
//   - the caller must release the subscription by stopping the recording on
//     the same channel and stream
//
// Pass false only when the subscription must outlive the recording session.
//
// ControlResponseTimeout bounds the whole wait after a successful offer.
//
// On timeout or on ControlResponseNotConnectedError:
//   - the extend may still have taken effect
//   - the caller owns the cleanup
//
// Concurrency: call from the goroutine that runs [Client.Poll]. Not safe
// from other goroutines.
//
// Drives the transport client while it waits.
//
// Returns an error when:
//   - the client cannot offer the extend request within the offer bound
//   - the archive refuses the extend, including ErrorUnknownRecording,
//     ErrorActiveRecording, ErrorActiveSubscription, and ErrorInvalidExtension
//   - a correlated EXTEND signal does not arrive within the response bound
//   - the transport client terminates
//   - the archive disconnects before the proof arrives
//   - ctx is done before the proof arrives
func (client *Client) ExtendRecordingAwaitSignal(
	ctx context.Context,
	recordingID int64,
	streamID int32,
	sourceLocation archivewire.SourceLocation,
	autoStop bool,
	channel []byte,
	tracker *ExtendSignalTracker,
) (
	extendedRecordingID int64,
	err error,
) {
	targets := client.sessionPollTargets(tracker)
	err = client.sessionControlOps(
		client.pollRecordingSignalAwait(targets),
	).extendRecordingAwaitSignal(
		ctx,
		recordingID,
		streamID,
		sourceLocation,
		booleanTypeFromBool(autoStop),
		channel,
		targets,
		tracker,
	)
	if err != nil {
		return 0, err
	}
	return tracker.ExtendedRecordingID(), nil
}

func booleanTypeFromBool(value bool) archivewire.BooleanType {
	if value {
		return archivewire.BooleanType_TRUE
	}
	return archivewire.BooleanType_FALSE
}

// StopRecording offers a StopRecordingRequest and blocks until the archive replies with
// a matching OK ControlResponse.
//
// Returns ctx.Err() when the context is done before a response is received.
func (client *Client) StopRecording(
	ctx context.Context,
	streamID int32,
	channel []byte,
) error {
	return client.sessionControlOps(client.Poll).stopRecording(ctx, streamID, channel)
}

// StopRecordingAwaitSignal offers a StopRecordingRequest and blocks until tracker
// latches a matching STOP RecordingSignalEvent.
//
// The proof check reads tracker before the control response, so a latched STOP
// wins when both are captured: ERROR fails the wait only when STOP is not yet
// latched. A matching OK alone does not complete: control OK can race ahead of
// the STOP signal, and callers need the latched stop position.
//
// ControlResponseTimeout bounds the whole wait after a successful offer. Timeout or
// ControlResponseNotConnectedError may fire after the await consumed a matching OK
// but STOP did not latch in time. Treat either as inconclusive for stop success.
// Use StopRecording when an OK ControlResponse proof is enough.
//
// tracker must be non-nil: it carries the STOP signal proof and is dereferenced on
// every proof check. Matching follows NewStopSignalTracker: a negative recordingID
// latches the first STOP for the session, including unrelated recordings.
//
// Returns ctx.Err() when the context is done before proof is received.
func (client *Client) StopRecordingAwaitSignal(
	ctx context.Context,
	streamID int32,
	channel []byte,
	tracker *StopSignalTracker,
) error {
	targets := client.sessionPollTargets(tracker)
	return client.sessionControlOps(
		client.pollRecordingSignalAwait(targets),
	).stopRecordingAwaitSignal(
		ctx,
		streamID,
		channel,
		targets,
		tracker,
	)
}

// StopRecordingByIdentity offers a StopRecordingByIdentityRequest and blocks
// until the archive replies.
//
// Behaviour: the archive aborts the active recording session of recordingID.
// The STOP signal goes to the control session that started the recording, not
// to this one. The catalog stop position appears once the archive closes the
// recording session. That close can lag the reply.
//
// Returns:
//   - true, nil: this request removed the recording subscription of an active
//     recording
//   - false, nil: no active recording. After an earlier stop the catalog stop
//     position can still be null
//   - false, err: the offer or the reply failed. ErrorUnknownRecording when
//     the catalog holds no recordingID. ctx.Err() when the context is done
//     first
func (client *Client) StopRecordingByIdentity(
	ctx context.Context,
	recordingID int64,
) (
	stopped bool,
	err error,
) {
	return client.sessionControlOps(client.Poll).stopRecordingByIdentity(ctx, recordingID)
}

// StartReplay offers a ReplayRequest and blocks until the archive replies with a
// matching ControlResponse, returning the assigned replay session id.
//
// Returns ctx.Err() when the context is done before a response is received.
func (client *Client) StartReplay(
	ctx context.Context,
	recordingID int64,
	position int64,
	length int64,
	replayStreamID int32,
	replayChannel []byte,
) (int64, error) {
	return client.sessionControlOps(client.Poll).startReplay(
		ctx,
		recordingID,
		position,
		length,
		replayStreamID,
		replayChannel,
	)
}

// StopReplay offers a StopReplayRequest and blocks until the archive replies with a
// matching OK ControlResponse.
//
// Returns ctx.Err() when the context is done before a response is received.
func (client *Client) StopReplay(ctx context.Context, replaySessionID int64) error {
	return client.sessionControlOps(client.Poll).stopReplay(ctx, replaySessionID)
}

// TruncateRecording offers a TruncateRecordingRequest and blocks until the
// archive replies with a matching OK ControlResponse.
//
// Behaviour: truncates a stopped recording to position. The archive lowers the
// catalog stop position to position, stops every replay of that recording, and
// queues segment files at or above position for deletion. Truncating to the
// recording start position removes every segment and leaves the catalog entry
// in place. It does not invalidate the recording.
//
// Concurrency: call from the goroutine that runs [Client.Poll]. Not safe
// from other goroutines. Blocks until the reply arrives, the bound expires,
// the transport client terminates, or the archive disconnects.
//
// Returns the deleted segment count from the reply RelevantID on OK. Returns
// ctx.Err() when the context is done before a response arrives. Returns an
// *Exception when the archive refuses:
//   - ErrorActiveRecording: recording still running
//   - ErrorInvalidPosition: position below start, above stop, or not aligned
//     to logbuffer.FrameAlignment
//   - ErrorUnknownRecording: recordingID does not name a recording
//   - ErrorGeneric: another delete is in progress for recordingID, or position
//     is frame-aligned but not a data-header boundary
//
// The recording is unchanged on every refusal except the data-header one.
// The archive lowers the catalog stop position before that check. The extent
// shrinks while segment bytes above it remain.
func (client *Client) TruncateRecording(
	ctx context.Context,
	recordingID int64,
	position int64,
) (
	deletedSegmentCount int64,
	err error,
) {
	return client.sessionControlOps(client.Poll).truncateRecording(ctx, recordingID, position)
}

// ListRecording lists a single recording by id, returning its descriptor extent fields
// with var-ascii bytes cloned at latch time for retention.
//
// Returns ctx.Err() when the context is done before a response is received.
func (client *Client) ListRecording(
	ctx context.Context,
	recordingID int64,
) (codecs.RecordingDescriptorFields, error) {
	return client.sessionControlOps(client.pollListRecording).listRecording(ctx, recordingID)
}

func (client *Client) sessionPollTargets(
	handler RecordingSignalHandler,
) ControlPollTargets {
	return ControlPollTargets{
		SignalHandler:        handler,
		ControlSessionID:     client.controlSessionID,
		ResponsePoller:       client.controlPoller,
		DescriptorPoller:     client.descriptorPoller,
		ErrorResponseHandler: client.errorResponseHandler,
	}
}
