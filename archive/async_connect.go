package archive

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/aeron/conductor"
	"github.com/warrenguy/luas/archive/codecs/archivewire"
	"github.com/warrenguy/luas/internal/transportfault"
)

// ConnectState mirrors Java Client.AsyncConnect step values.
type ConnectState int

const (
	ConnectStateCreateControlResponseSubscription ConnectState = iota - 1
	ConnectStateCreateControlRequestPublication
	ConnectStateAwaitPublicationConnected
	ConnectStateSendMessage
	ConnectStatePollResponse
	ConnectStateSendArchiveIDRequest
	ConnectStateAwaitArchiveIDResponse
	ConnectStateConcludeConnect
	ConnectStateDone
)

var connectStateNames = [...]string{
	"CREATE_CONTROL_RESPONSE_SUBSCRIPTION",
	"CREATE_CONTROL_REQUEST_PUBLICATION",
	"AWAIT_PUBLICATION_CONNECTED",
	"SEND_MESSAGE",
	"POLL_RESPONSE",
	"SEND_ARCHIVE_ID_REQUEST",
	"AWAIT_ARCHIVE_ID_RESPONSE",
	"CONCLUDE_CONNECT",
	"DONE",
}

// String returns the Java AsyncConnect state name for diagnostics.
func (state ConnectState) String() string {
	idx := int(state) - int(ConnectStateCreateControlResponseSubscription)
	if idx < 0 || idx >= len(connectStateNames) {
		return fmt.Sprintf("ConnectState(%d)", state)
	}
	return connectStateNames[idx]
}

type connectTransport struct {
	responseRegID           int64
	responseSubscription    *aeron.Subscription
	responsePoller          *ControlResponsePoller
	requestRegID            int64
	requestPublication      ControlPublication
	cachedResponseChannel   string
	cachedResponseChannelOK bool
}

type connectSession struct {
	controlSessionID int64
	archiveID        int64
	serverVersion    int32
	correlationID    int64
	// established is set once the archive has named a control session.
	// Close then sends a best-effort CloseSessionRequest for it.
	established bool
}

type connectWireState struct {
	length           int
	buffer           []byte
	encodedChallenge []byte
}

// AsyncConnect runs the non-blocking archive connect handshake.
//
// With Config.Aeron set to an invoker-mode client, the caller calls DoWork
// between Poll calls. OwnsAeronClient reports whether Close closes the
// transport client.
//
// A gap between Poll calls longer than the resolved InterServiceTimeout
// terminates a connect-owned invoker-mode client.
// aeron.Client.InterServiceTimeout reports the resolved value.
//
// Concurrency: one goroutine calls Poll and Close. Poll must not run inside
// a callback of its transport client.
type AsyncConnect struct {
	cfg             Config
	client          *aeron.Client
	ownsAeronClient bool

	// transportFault reads the termination cause of a connect-owned client.
	// It is nil when the caller supplied Config.Aeron.
	transportFault *transportfault.Monitor
	// transportFaultErr is the first Poll error from a terminated owned
	// client. Later Poll calls return it.
	transportFaultErr error

	state ConnectState

	deadlineNs int64
	nanoClock  func() int64

	transport connectTransport
	session   connectSession
	wire      connectWireState

	closed bool
}

// NewAsyncConnect validates configuration, concludes archive config, and starts
// the connect state machine at CREATE_CONTROL_RESPONSE_SUBSCRIPTION.
//
// ctx bounds owned transport attach only (aeron.Connect wait for CNC ready and
// driver heartbeat). Prefer Connect for a blocking handshake with cancel support.
// Poll does not accept context cancellation; check cancel between Poll calls and
// call Close on cancel. ctx is not retained on AsyncConnect.
//
// Behaviour:
//   - Outside response mode, NewAsyncConnect draws one session id from the
//     driver. Both effective control channels carry that id as session-id.
//   - The draw blocks for one driver round trip. The transport client
//     DriverTimeout bounds the draw, not ctx.
//   - The connect deadline starts after the draw.
//   - A draw error closes an owned transport client. When that client has
//     terminated, the error wraps ErrTransportTerminating.
//
// Concurrency: with Config.Aeron set to an invoker-mode client, call from
// the goroutine that runs [aeron.Client.DoWork]. NewAsyncConnect must not
// run inside a callback of that client.
func NewAsyncConnect(ctx context.Context, cfg Config) (*AsyncConnect, error) {
	cfg = cfg.Normalize()
	if err := cfg.ValidateForConnect(); err != nil {
		return nil, err
	}
	concluded, client, owns, monitor, err := concludeConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if usesControlChannelSessionID(concluded) {
		sessionID, err := client.NextAvailableSessionID(
			concluded.effectiveControlRequestStreamID,
		)
		if err != nil {
			if fault := transportTerminatingError(monitor, client); fault != nil {
				err = fault
			}
			if owns {
				_ = client.Close()
			}
			return nil, fmt.Errorf("archive: next session id: %w", err)
		}
		concluded = applyControlChannelSessionID(concluded, sessionID)
	}
	nanoClock := resolveNanoClock(concluded.AeronConfig.NanoClock)
	return &AsyncConnect{
		cfg:             concluded,
		client:          client,
		ownsAeronClient: owns,
		transportFault:  monitor,
		state:           ConnectStateCreateControlResponseSubscription,
		deadlineNs:      nanoClock() + concluded.ConnectTimeout.Nanoseconds(),
		nanoClock:       nanoClock,
		session: connectSession{
			correlationID: conductor.NullCorrelationID,
		},
		transport: connectTransport{
			responseRegID: conductor.NullCorrelationID,
			requestRegID:  conductor.NullCorrelationID,
		},
	}, nil
}

// resolveNanoClock returns nanoClock when set, else a wall-clock nanosecond source.
func resolveNanoClock(nanoClock func() int64) func() int64 {
	if nanoClock != nil {
		return nanoClock
	}
	return func() int64 { return time.Now().UnixNano() }
}

// State returns the current connect state.
func (ac *AsyncConnect) State() ConnectState {
	if ac == nil {
		return ConnectStateCreateControlResponseSubscription
	}
	return ac.state
}

// OwnsAeronClient reports whether Close closes the transport client.
func (ac *AsyncConnect) OwnsAeronClient() bool {
	if ac == nil {
		return false
	}
	return ac.ownsAeronClient
}

// Poll advances the connect handshake by one step.
//
// Poll calls DoWork once on a connect-owned invoker-mode client, before the
// fault check.
//
// Returns (nil, nil) while in progress, (*ConnectResult, nil) on success, and
// (nil, err) on terminal failure.
//
// A terminated connect-owned transport client fails Poll in every state,
// before the deadline check. The error wraps ErrTransportTerminating and the
// conductor cause. Later Poll calls return the same error and skip the tick.
//
// Concurrency: one goroutine calls Poll. The DoWork call takes the
// transport client lock.
func (ac *AsyncConnect) Poll() (*ConnectResult, error) {
	if ac == nil {
		return nil, errors.New("archive: nil async connect")
	}
	if ac.closed {
		return nil, errors.New("archive: async connect closed")
	}
	if ac.state == ConnectStateDone {
		return nil, errors.New("archive: async connect already completed")
	}
	if ac.transportFaultErr == nil {
		ac.tickOwnedClient()
	}
	if err := ac.transportFaultError(); err != nil {
		return nil, err
	}
	if ac.state != ConnectStateConcludeConnect {
		if err := ac.checkDeadline(); err != nil {
			return nil, err
		}
	}

	result, err := ac.pollState()
	if err != nil {
		if faultErr := ac.transportFaultError(); faultErr != nil {
			return nil, faultErr
		}
	}
	return result, err
}

func (ac *AsyncConnect) tickOwnedClient() {
	if ac.ownsAeronClient && ac.client.HasConductorInvoker() {
		ac.client.DoWork()
	}
}

// transportFaultError returns the first termination error of the owned
// transport client. Later calls return the same error. Returns nil while the
// owned client runs.
func (ac *AsyncConnect) transportFaultError() error {
	if ac.transportFaultErr != nil {
		return ac.transportFaultErr
	}
	ac.transportFaultErr = transportTerminatingError(ac.transportFault, ac.client)
	return ac.transportFaultErr
}

func (ac *AsyncConnect) pollState() (*ConnectResult, error) {
	switch ac.state {
	case ConnectStateCreateControlResponseSubscription:
		return ac.pollCreateControlResponseSubscription()
	case ConnectStateCreateControlRequestPublication:
		return ac.pollCreateControlRequestPublication()
	case ConnectStateAwaitPublicationConnected:
		return ac.pollAwaitPublicationConnected()
	case ConnectStateSendMessage:
		return ac.offerConnectWire(ConnectStatePollResponse)
	case ConnectStatePollResponse, ConnectStateAwaitArchiveIDResponse:
		return ac.pollPollResponse()
	case ConnectStateSendArchiveIDRequest:
		return ac.offerConnectWire(ConnectStateAwaitArchiveIDResponse)
	case ConnectStateConcludeConnect:
		return ac.pollConcludeConnect()
	case ConnectStateDone:
		return nil, errors.New("archive: async connect already completed")
	default:
		return nil, fmt.Errorf("archive: invalid connect state %d", ac.state)
	}
}

// Close releases partial connect resources. Idempotent.
//
// Close sends one best-effort CloseSessionRequest when the archive has
// already named a control session. A connect that fails after the
// handshake then leaves no session behind on the archive.
//
// After a successful Poll returns ConnectResult, resources move to the result and
// Close is a true no-op: closed is not set and Poll continues to report
// "already completed". Do not call Close on success paths.
func (ac *AsyncConnect) Close() error {
	if ac == nil || ac.closed {
		return nil
	}
	if ac.state == ConnectStateDone {
		return nil
	}
	ac.closed = true
	ac.zeroSensitiveBuffers()
	ac.cancelPendingRegistration(&ac.transport.responseRegID)
	ac.cancelPendingRegistration(&ac.transport.requestRegID)
	if ac.transport.responseSubscription != nil {
		_ = ac.transport.responseSubscription.Close()
		ac.transport.responseSubscription = nil
	}
	ac.transport.responsePoller = nil
	if pub := ac.transport.requestPublication; pub != nil {
		if ac.session.established {
			_, _ = NewProxy(pub, ac.cfg).TryCloseSession(ac.session.controlSessionID)
		}
		_ = pub.Close()
		ac.transport.requestPublication = nil
	}
	if ac.ownsAeronClient && ac.client != nil {
		return ac.client.Close()
	}
	return nil
}

func (ac *AsyncConnect) checkDeadline() error {
	if ac.nanoClock() <= ac.deadlineNs {
		return nil
	}
	connected := ac.transport.responseSubscription != nil &&
		ac.transport.responseSubscription.IsConnected()
	return fmt.Errorf(
		"archive: connect timeout in state %v request=%s response=%s responseConnected=%t",
		ac.state,
		ac.cfg.EffectiveControlRequestChannel(),
		ac.cfg.EffectiveControlResponseChannel(),
		connected,
	)
}

func pollAsyncRegistration[T any](
	regID *int64,
	offer func() (int64, error),
	poll func(int64) (T, bool, error),
	onNotDone func(),
	onDone func(T),
) error {
	if *regID == conductor.NullCorrelationID {
		nextRegID, err := offer()
		if err != nil {
			return err
		}
		*regID = nextRegID
		return nil
	}
	resource, done, err := poll(*regID)
	if err != nil {
		return err
	}
	if !done {
		if onNotDone != nil {
			onNotDone()
		}
		return nil
	}
	*regID = conductor.NullCorrelationID
	onDone(resource)
	return nil
}

func (ac *AsyncConnect) pollCreateControlResponseSubscription() (*ConnectResult, error) {
	err := pollAsyncRegistration(
		&ac.transport.responseRegID,
		func() (int64, error) {
			return ac.client.OfferAddSubscription(
				ac.cfg.EffectiveControlResponseStreamID(),
				ac.cfg.EffectiveControlResponseChannel(),
			)
		},
		ac.pollAddControlResponseSubscription,
		func() {
			if ac.shouldRetryControlResponseSubscription() {
				ac.cancelPendingRegistration(&ac.transport.responseRegID)
			}
		},
		func(sub *aeron.Subscription) {
			if aeron.IsControlModeResponse(ac.cfg.effectiveControlResponseChannel) {
				ac.cfg.effectiveControlRequestChannel = aeron.SetChannelParam(
					ac.cfg.effectiveControlRequestChannel,
					aeron.ChannelResponseCorrelationIDParam,
					strconv.FormatInt(sub.RegistrationID(), 10),
				)
			}
			ac.transport.responseSubscription = sub
			ac.transport.responsePoller = NewControlResponsePoller(
				sub,
				DefaultConnectControlResponseFragmentLimit,
			)
			ac.state = ConnectStateCreateControlRequestPublication
		},
	)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (ac *AsyncConnect) pollCreateControlRequestPublication() (*ConnectResult, error) {
	err := pollAsyncRegistration(
		&ac.transport.requestRegID,
		func() (int64, error) {
			return ac.client.OfferAddExclusivePublication(
				ac.cfg.EffectiveControlRequestStreamID(),
				ac.cfg.EffectiveControlRequestChannel(),
			)
		},
		ac.pollAddControlRequestPublication,
		nil,
		func(pub ControlPublication) {
			ac.transport.requestPublication = pub
			ac.state = ConnectStateAwaitPublicationConnected
		},
	)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (ac *AsyncConnect) resolvedResponseChannel() (string, bool) {
	if ac.transport.cachedResponseChannelOK {
		return ac.transport.cachedResponseChannel, true
	}
	if ac.transport.responseSubscription == nil {
		return "", false
	}
	channel, ok := resolveResponseChannel(ac.transport.responseSubscription)
	if ok {
		ac.transport.cachedResponseChannel = channel
		ac.transport.cachedResponseChannelOK = true
	}
	return channel, ok
}

func (ac *AsyncConnect) pollAwaitPublicationConnected() (*ConnectResult, error) {
	if ac.transport.responseSubscription == nil {
		return nil, errors.New("archive: control response subscription missing")
	}
	if _, ok := ac.resolvedResponseChannel(); !ok {
		return nil, nil
	}
	if ac.transport.requestPublication == nil || !ac.transport.requestPublication.IsConnected() {
		return nil, nil
	}
	if err := ac.prepareConnectRequest(); err != nil {
		return nil, err
	}
	ac.state = ConnectStateSendMessage
	return nil, nil
}

// offerConnectWire offers the encoded wire once. A taken offer moves the
// state to next.
func (ac *AsyncConnect) offerConnectWire(next ConnectState) (*ConnectResult, error) {
	pub := ac.transport.requestPublication
	if pub == nil {
		return nil, errors.New("archive: control publication missing")
	}
	result := pub.Offer(ac.wire.buffer[:ac.wire.length])
	switch result {
	case aeron.Closed, aeron.NotConnected:
		return nil, errors.New("archive: unexpected loss of connection")
	case aeron.BackPressured, aeron.AdminAction:
		return nil, nil
	case aeron.UnsupportedMessageLength:
		return nil, fmt.Errorf(
			"archive: connect wire length %d exceeds control publication max message length %d",
			ac.wire.length,
			pub.MaxMessageLength(),
		)
	case aeron.MaxPositionExceeded:
		return nil, errors.New("archive: control publication stream position exceeded")
	}
	if result < 0 {
		return nil, fmt.Errorf("archive: control offer failed: status=%d", result)
	}
	ac.state = next
	return nil, nil
}

func (ac *AsyncConnect) pollPollResponse() (*ConnectResult, error) {
	poller := ac.transport.responsePoller
	if poller == nil {
		return nil, errors.New("archive: control response poller missing")
	}
	poller.Poll()
	if !poller.IsPollComplete() {
		return nil, nil
	}
	if ac.connectPollResponseIgnorable(poller) {
		return nil, nil
	}
	if poller.IsChallenged() {
		return ac.handleConnectChallenge(poller)
	}
	return ac.handleConnectControlResponse(poller)
}

func (ac *AsyncConnect) connectPollResponseIgnorable(poller *ControlResponsePoller) bool {
	capture, ok := poller.Capture()
	if !ok {
		return true
	}
	correlationID := ac.session.correlationID
	if correlationID != conductor.NullCorrelationID &&
		capture.CorrelationID != correlationID {
		return true
	}
	if capture.Challenged() {
		return false
	}
	return capture.TemplateID != int32(archivewire.ControlResponseSbeTemplateID)
}

func (ac *AsyncConnect) handleConnectChallenge(
	poller *ControlResponsePoller,
) (*ConnectResult, error) {
	capture, _ := poller.Capture()
	ac.wire.encodedChallenge = bytes.Clone(capture.EncodedChallenge)
	ac.session.controlSessionID = capture.ControlSessionID
	ac.session.established = true
	ac.session.correlationID = conductor.NullCorrelationID
	if err := ac.prepareChallengeResponse(); err != nil {
		return nil, err
	}
	ac.state = ConnectStateSendMessage
	return nil, nil
}

func (ac *AsyncConnect) handleConnectControlResponse(
	poller *ControlResponsePoller,
) (*ConnectResult, error) {
	resp, ok := poller.Response()
	if !ok {
		return nil, errors.New("archive: connect response missing")
	}
	if resp.Code != archivewire.ControlResponseCode_OK {
		return nil, MapControlResponse(resp)
	}
	ac.session.controlSessionID = resp.ControlSessionID
	ac.session.established = true
	if ac.state == ConnectStateAwaitArchiveIDResponse {
		ac.session.archiveID = resp.RelevantID
		ac.state = ConnectStateConcludeConnect
		return nil, nil
	}

	// The connect OK carries the control session id in RelevantID. The archive
	// id needs its own request. Servers before 1.11.0 do not serve it.
	ac.session.serverVersion = resp.Version
	if resp.Version < protocolVersionWithArchiveID {
		ac.session.archiveID = cnc.NullValue
		ac.state = ConnectStateConcludeConnect
		return nil, nil
	}
	if err := ac.prepareArchiveIDRequest(); err != nil {
		return nil, err
	}
	ac.state = ConnectStateSendArchiveIDRequest
	return nil, nil
}

func (ac *AsyncConnect) pollConcludeConnect() (*ConnectResult, error) {
	result := &ConnectResult{
		ControlSessionID:    ac.session.controlSessionID,
		ArchiveID:           ac.session.archiveID,
		ServerVersion:       ac.session.serverVersion,
		ControlPublication:  ac.transport.requestPublication,
		ControlSubscription: ac.transport.responseSubscription,
		Proxy:               NewProxy(ac.transport.requestPublication, ac.cfg),
		Poller:              ac.transport.responsePoller,
		Aeron:               ac.client,
		OwnsAeronClient:     ac.ownsAeronClient,
		Config:              ac.cfg,
		transportFault:      ac.transportFault,
	}
	ac.transferConnectResourcesToResult()
	ac.zeroSensitiveBuffers()
	ac.state = ConnectStateDone
	return result, nil
}

func (ac *AsyncConnect) transferConnectResourcesToResult() {
	ac.transport.responseRegID = conductor.NullCorrelationID
	ac.transport.requestRegID = conductor.NullCorrelationID
	ac.transport.responseSubscription = nil
	ac.transport.responsePoller = nil
	ac.transport.requestPublication = nil
	ac.client = nil
	ac.ownsAeronClient = false
	ac.transportFault = nil
}

func (ac *AsyncConnect) cancelPendingRegistration(regID *int64) {
	if *regID == conductor.NullCorrelationID || ac.client == nil {
		return
	}
	ac.client.CancelPending(*regID)
	*regID = conductor.NullCorrelationID
}

func (ac *AsyncConnect) ensureCorrelationID() {
	if ac.session.correlationID == conductor.NullCorrelationID {
		ac.session.correlationID = ac.client.NextCorrelationID()
	}
}

func (ac *AsyncConnect) zeroSensitiveBuffers() {
	clear(ac.wire.buffer[:cap(ac.wire.buffer)])
	ac.wire.buffer = nil
	ac.wire.length = 0
	ac.wire.encodedChallenge = nil
}
