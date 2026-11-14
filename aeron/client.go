package aeron

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/aeron/conductor"
	"github.com/warrenguy/luas/mmap"
	"github.com/warrenguy/luas/ringbuffer"
)

const (
	driverHeartbeatPollInterval = 50 * time.Millisecond
	closeDrainMaxIdleTicks      = 3
)

// Client is the Aeron media driver client. The caller drives conductor work
// with DoWork. Client does not start a conductor goroutine.
//
// A Client starts one goroutine when it maps its first log buffer. The
// goroutine unmaps released log buffers. Close stops that goroutine.
//
// Concurrency: clientLock admits one call at a time. These calls take
// clientLock:
//   - DoWork, Close, and CancelPending
//   - Add*, OfferAdd*, PollAdd*, and the destination methods
//   - AddCounter, Counter.Close, NextAvailableSessionID, and PollCommand
//
// A call from another goroutine waits until the holder returns. DoWork returns
// 0 instead of waiting. Close must not run during a blocking call. See Close.
//
// Another goroutine may call Publication Offer when the call does not overlap
// a blocking call. One goroutine at a time polls a Subscription and its
// images. See [Subscription].
//
// ErrorHandler, AvailableImageHandler, UnavailableImageHandler, and
// PublicationErrorFrameHandler run inside DoWork or a blocking call, on the
// goroutine that holds clientLock. ErrorHandler also runs without clientLock:
//   - on the Close goroutine, for a resource Close on another goroutine
//   - on the poll goroutine, for a corrupt frame or a recovered fragment
//     handler panic
//
// A handler on the goroutine that holds clientLock fails fast on a call that
// needs clientLock:
//   - A method with an error result returns [ErrReentrantCall].
//   - DoWork returns 0. CancelPending returns. Each reports
//     [ErrReentrantCall] to ErrorHandler.
//
// An ErrorHandler on the Close or poll goroutine does not fail fast.
// Calls from that ErrorHandler take clientLock like any other caller.
//
// A handler may call IsTerminating, the lookups, offers, polls, and resource
// Close. These calls do not take clientLock. A handler must not wait for a
// goroutine that waits on clientLock.
//
// See AvailableImageHandler and PublicationErrorFrameHandler for their own
// contracts.
type Client struct {
	clientID       int64
	mapped         *cnc.MappedCncFile
	countersReader *cnc.CountersReader
	conductor      *conductor.Conductor
	// terminationConductor does not change after construction, so IsTerminating
	// reads it without clientLock.
	terminationConductor    *conductor.Conductor
	driverProxy             *conductor.DriverProxy
	nanoClock               conductor.NanoClock
	driverTimeout           time.Duration
	interServiceTimeout     time.Duration
	clientCloseLinger       time.Duration
	pollTimeout             time.Duration
	publicationOfferTimeout time.Duration
	idleSleepDuration       time.Duration
	serviceTimestampsSeeded bool
	clientLock              sync.Mutex
	handlerScope            handlerScope
	closed                  atomic.Bool
}

func (client *Client) lockClient() error {
	if err := client.checkReentrant(); err != nil {
		return err
	}
	client.clientLock.Lock()
	return nil
}

func (client *Client) checkReentrant() error {
	if client == nil {
		return nil
	}
	if client.handlerScope.isHandlerGoroutine() {
		return ErrReentrantCall
	}
	return nil
}

// reportReentrantCall sends ErrReentrantCall to ErrorHandler once per
// call. A nested call from ErrorHandler does not send a second report.
func (client *Client) reportReentrantCall() {
	if client.handlerScope.reporting || client.conductor == nil {
		return
	}
	client.handlerScope.reporting = true
	defer func() { client.handlerScope.reporting = false }()
	client.conductor.ReportError(ErrReentrantCall)
}

func (client *Client) unlockClient() {
	client.clientLock.Unlock()
}

// Connect maps CNC and builds a Client.
//
// ctx bounds CNC ready and driver heartbeat waits only (earlier of
// [Config.DriverTimeout] and ctx.Deadline()). A successful Client is independent
// of ctx; callers must Close it. Cancel does not close a live client.
//
// Returns ctx.Err() when the context is done before connect succeeds. Driver
// timeout errors apply only while the context remains active.
func Connect(ctx context.Context, cfg Config) (*Client, error) {
	cfg = normalizeConfig(cfg)
	if err := validateConnectConfig(cfg); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	mapped, rings, err := openCnc(ctx, cfg)
	if err != nil {
		return nil, err
	}

	connected := false
	defer func() {
		if !connected {
			_ = mapped.Close()
		}
	}()

	cfg.InterServiceTimeout, err = resolveInterServiceTimeout(cfg, mapped.Metadata())
	if err != nil {
		return nil, err
	}

	client, err := newConductorClient(ctx, cfg, mapped, rings)
	if err != nil {
		return nil, err
	}
	connected = true
	return client, nil
}

func validateConnectConfig(cfg Config) error {
	if cfg.AeronDir == "" {
		return errors.New("aeron: AeronDir is required")
	}
	if cfg.KeepAliveInterval < 0 {
		return errors.New("aeron: KeepAliveInterval must not be negative")
	}
	if cfg.InterServiceTimeout < 0 {
		return errors.New("aeron: InterServiceTimeout must not be negative")
	}
	if timeout := cfg.InterServiceTimeout; timeout != 0 && timeout <= cfg.KeepAliveInterval {
		return fmt.Errorf(
			"aeron: InterServiceTimeout %v must exceed KeepAliveInterval %v",
			timeout,
			cfg.KeepAliveInterval,
		)
	}
	if cfg.resourceLingerDuration() < 0 {
		return errors.New("aeron: ResourceLingerDuration must not be negative")
	}
	if cfg.ClientCloseLingerDuration < 0 {
		return errors.New("aeron: ClientCloseLingerDuration must not be negative")
	}
	if !cfg.useConductorInvokerEnabled() {
		return errors.New("aeron: dedicated conductor invoker is not supported")
	}
	if cfg.PollTimeout < 0 {
		return errors.New("aeron: PollTimeout must not be negative")
	}
	if cfg.PublicationOfferTimeout < 0 {
		return errors.New("aeron: PublicationOfferTimeout must not be negative")
	}
	return nil
}

func openCnc(ctx context.Context, cfg Config) (*cnc.MappedCncFile, *cnc.Rings, error) {
	cncPath := filepath.Join(cfg.AeronDir, cnc.FileName)
	if err := cnc.WaitCncReady(ctx, cncPath, cfg.DriverTimeout); err != nil {
		return nil, nil, fmt.Errorf("aeron: wait for cnc ready: %w", err)
	}

	mapped, err := cnc.MapCncFile(cncPath)
	if err != nil {
		return nil, nil, fmt.Errorf("aeron: map cnc file: %w", err)
	}

	opened := false
	defer func() {
		if !opened {
			_ = mapped.Close()
		}
	}()

	rings, err := cnc.WireRings(mapped)
	if err != nil {
		return nil, nil, fmt.Errorf("aeron: wire rings: %w", err)
	}

	if rings.Mapping != nil {
		mmap.PreTouch(rings.Mapping.Data)
	}

	opened = true
	return mapped, rings, nil
}

func conductorConnectConfig(cfg Config, clientID int64, client *Client) conductor.Config {
	config := conductor.Config{
		ClientID:               clientID,
		DriverTimeout:          cfg.DriverTimeout,
		KeepAliveInterval:      cfg.KeepAliveInterval,
		IdleSleepDuration:      cfg.IdleSleepDuration,
		InterServiceTimeout:    cfg.InterServiceTimeout,
		EpochClock:             cfg.EpochClock,
		NanoClock:              cfg.NanoClock,
		SubscriberErrorHandler: cfg.ErrorHandler,
		ImageLifecycle:         ConductorImageLifecycle,
		ResourceLifecycle: conductorResourceLifecycle{
			availableImageHandler:   cfg.AvailableImageHandler,
			unavailableImageHandler: cfg.UnavailableImageHandler,
			client:                  client,
		},
		MediaDriverDir:   cfg.AeronDir,
		ResourceLingerNs: cfg.resourceLingerDuration().Nanoseconds(),
	}
	if client != nil {
		config.HandlerScope = &client.handlerScope
	}
	config.ErrorHandler = wrapConductorErrorHandler(cfg.ErrorHandler)
	if handler := cfg.PublicationErrorFrameHandler; handler != nil {
		config.PublicationErrorFrameHandler = func(event conductor.PublicationError) {
			handler(publicationErrorFrameFrom(event))
		}
	}
	return config
}

// wrapConductorErrorHandler maps each conductor error through
// mapConductorError before handler sees it. A nil handler returns nil.
func wrapConductorErrorHandler(handler func(error)) func(error) {
	if handler == nil {
		return nil
	}
	return func(err error) {
		handler(mapConductorError(err))
	}
}

func newConductorClient(
	ctx context.Context,
	cfg Config,
	mapped *cnc.MappedCncFile,
	rings *cnc.Rings,
) (*Client, error) {
	if err := waitForDriverHeartbeat(
		ctx,
		rings.ToDriver,
		cfg.DriverTimeout,
		cfg.EpochClock,
	); err != nil {
		return nil, err
	}

	countersReader, err := cnc.NewCountersReader(
		mapped.CountersMetaDataBuffer(),
		mapped.CountersValuesBuffer(),
	)
	if err != nil {
		return nil, fmt.Errorf("aeron: counters reader: %w", err)
	}

	if err := validateControlProtocolVersion(countersReader); err != nil {
		return nil, err
	}

	clientID := rings.ToDriver.NextCorrelationID()
	if err := validateClientID(clientID); err != nil {
		return nil, err
	}

	driverProxy := conductor.NewDriverProxy(rings.ToDriver, clientID)

	// Client exists before NewConductor so ResourceLifecycle can bind it on Add*.
	client := &Client{
		clientID:                clientID,
		mapped:                  mapped,
		countersReader:          countersReader,
		driverProxy:             driverProxy,
		nanoClock:               cfg.NanoClock,
		driverTimeout:           cfg.DriverTimeout,
		interServiceTimeout:     cfg.InterServiceTimeout,
		clientCloseLinger:       cfg.ClientCloseLingerDuration,
		pollTimeout:             cfg.PollTimeout,
		publicationOfferTimeout: cfg.PublicationOfferTimeout,
		idleSleepDuration:       cfg.IdleSleepDuration,
	}

	config := conductorConnectConfig(cfg, clientID, client)

	conductorInstance, err := conductor.NewConductor(
		driverProxy,
		rings.ToClients,
		countersReader,
		config,
	)
	if err != nil {
		return nil, err
	}
	client.conductor = conductorInstance
	client.terminationConductor = conductorInstance
	return client, nil
}

func (client *Client) doWorkLocked() int {
	if client.conductor == nil {
		return 0
	}
	if !client.serviceTimestampsSeeded {
		client.conductor.SeedServiceTimestamps(client.nanoClock())
		client.serviceTimestampsSeeded = true
	}
	return client.conductor.DoWork()
}

// DoWork runs one conductor service tick under clientLock. DoWork returns the
// work count, or 0 when:
//   - Close has published closed
//   - another goroutine holds clientLock
//   - the tick does not make progress
//   - the caller is inside a handler
//
// A call from a handler reports [ErrReentrantCall] to ErrorHandler. The
// handler call reports even after Close publishes closed. A non-handler
// call returns 0 after Close publishes closed. A non-handler call does
// not report.
//
// See Client godoc for the invoker and concurrency contract.
func (client *Client) DoWork() int {
	if client == nil {
		return 0
	}
	if client.handlerScope.isHandlerGoroutine() {
		client.reportReentrantCall()
		return 0
	}

	// A non-handler return on closed does not wait for teardown.
	if client.closed.Load() {
		return 0
	}

	// Skip the tick instead of waiting. A blocking call that holds clientLock
	// drives the conductor.
	if !client.clientLock.TryLock() {
		return 0
	}

	defer client.unlockClient()

	if client.closed.Load() {
		return 0
	}
	workDone := client.doWorkLocked()
	invokeDoWorkAfterHook(client)
	return workDone
}

// IsTerminating reports whether the conductor has entered termination.
// Causes include driver shutdown, a stale keepalive, and a client or
// service timeout.
//
// Returns false after Close.
//
// Concurrency: safe from any goroutine.
func (client *Client) IsTerminating() bool {
	if client == nil || client.closed.Load() || client.terminationConductor == nil {
		return false
	}

	return client.terminationConductor.IsTerminating()
}

// Close closes the client resources and unmaps the CnC file. See
// [Config.ClientCloseLingerDuration] for calls that run during Close.
//
// Behaviour: Close is idempotent.
//
// Concurrency: callers should not run Close beside another Close or a
// blocking call. If they do:
//   - The first Close to win the closed CAS runs teardown under clientLock.
//     Other Close calls return nil at once. Nil does not mean the CnC file
//     is unmapped.
//   - Close sets closed before it waits on clientLock. A blocking call that
//     sees closed returns ErrClientClosed, even after a driver answer on the
//     same poll tick.
//   - The blocking call cancels its incomplete add and rolls back partial
//     add resources. Teardown runs after the blocking call releases
//     clientLock.
//
// Returns the first error from unmapping CnC. Teardown errors go to
// ErrorHandler. A call from a handler on an open client returns
// [ErrReentrantCall]. The handler call does not close the client.
func (client *Client) Close() error {
	if client == nil || client.closed.Load() {
		return nil
	}
	if err := client.checkReentrant(); err != nil {
		return err
	}
	if !client.closed.CompareAndSwap(false, true) {
		return nil
	}
	client.clientLock.Lock()
	defer client.unlockClient()
	return client.closeTeardownLocked()
}

func (client *Client) closeTeardownLocked() error {
	if client.conductor != nil {
		client.conductor.CloseAllResources()
		client.conductor.ClearPendingEntriesForClose()
		client.drainCloseResponses()
		time.Sleep(client.clientCloseLinger)
		client.conductor.UnmapLingeringLogBuffers()
	}

	if client.driverProxy != nil {
		client.driverProxy.ClientClose()
	}

	var err error
	if client.mapped != nil {
		err = client.mapped.Close()
		client.mapped = nil
	}
	client.conductor = nil
	client.countersReader = nil
	client.driverProxy = nil
	return err
}

func (client *Client) drainCloseResponses() {
	// closeDrainMaxIdleTicks idle DoWork ticks exit early so unit tests with
	// synthetic drivers finish without waiting the full DriverTimeout cap.
	deadline := time.Now().Add(client.driverTimeout)
	idleTicks := 0
	for time.Now().Before(deadline) {
		if client.doWorkLocked() != 0 {
			idleTicks = 0
		} else if idleTicks++; idleTicks >= closeDrainMaxIdleTicks {
			return
		}
		time.Sleep(client.idleSleepDuration)
	}
}

// CancelPending abandons an in-flight OfferAdd* registration or an async
// destination command. The driver can still apply the command after a cancel.
//
// Idempotent when the pending entry is absent. Not safe for concurrent use with
// OfferAdd* on the same correlation id.
//
// A call from a handler does not cancel the entry. The handler call
// reports [ErrReentrantCall] to ErrorHandler. Call CancelPending after
// DoWork returns.
func (client *Client) CancelPending(correlationID int64) {
	if client == nil || correlationID == conductor.NullCorrelationID {
		return
	}
	if err := client.lockClient(); err != nil {
		client.reportReentrantCall()
		return
	}
	defer client.unlockClient()
	if client.conductor != nil {
		client.conductor.CancelPending(correlationID)
	}
}

// NextCorrelationID allocates the next correlation id from the driver ring.
// Concurrent Close during allocation is unsupported; closed and nil proxy checks
// are best-effort without clientLock.
func (client *Client) NextCorrelationID() int64 {
	if client == nil || client.closed.Load() || client.driverProxy == nil {
		return 0
	}
	return client.driverProxy.NextCorrelationID()
}

// NextAvailableSessionID blocks until the driver returns the next session id or
// DriverTimeout elapses.
//
// NextAvailableSessionID holds clientLock for the whole wait. DoWork from
// another goroutine returns 0 until this call returns.
//
// Concurrent Close during this call is unsupported. See Client godoc.
func (client *Client) NextAvailableSessionID(streamID int32) (int32, error) {
	if err := client.requireReadyWithDriverProxy(); err != nil {
		return 0, err
	}
	correlationID := client.driverProxy.NextCorrelationID()
	registration, err := client.awaitResponse(
		correlationID,
		conductor.PendingKindNextSessionID,
		client.driverTimeout,
		func() error {
			return client.driverProxy.NextAvailableSessionID(correlationID, streamID)
		},
	)
	if err != nil {
		return 0, err
	}
	return registration.Int32Value, nil
}

func (client *Client) errIfClosed() error {
	if client == nil || client.closed.Load() {
		return ErrClientClosed
	}
	return nil
}

func (client *Client) requireReadyWithConductor() error {
	if client == nil {
		return ErrClientNotReady
	}
	if err := client.errIfClosed(); err != nil {
		return err
	}
	if client.conductor == nil || client.nanoClock == nil {
		return ErrClientNotReady
	}
	return nil
}

func (client *Client) requireReadyWithDriverProxy() error {
	if err := client.requireReadyWithConductor(); err != nil {
		return err
	}
	if client.driverProxy == nil {
		return ErrClientNotReady
	}
	return nil
}

// mapConductorTerminationError maps driver shutdown and keepalive-stale causes to
// DriverTimeoutError.
//
// Client and service timeouts pass through for errors.Is.
func mapConductorTerminationError(conductorInstance *conductor.Conductor) error {
	cause := conductorInstance.TerminationCause()
	if cause == nil {
		cause = conductor.ErrDriverShutdown
	}
	return mapConductorError(cause)
}

// awaitResponse blocks until the pending registration completes or times out.
// Poll ticks call doWorkLocked, not DoWork. See Client godoc for concurrency.
func (client *Client) awaitResponse(
	correlationID int64,
	kind conductor.PendingKind,
	timeout time.Duration,
	offer func() error,
) (conductor.PendingRegistration, error) {
	if client == nil {
		return conductor.PendingRegistration{}, ErrClientNotReady
	}
	if err := client.lockClient(); err != nil {
		return conductor.PendingRegistration{}, err
	}
	defer client.unlockClient()

	if err := client.requireReadyWithConductor(); err != nil {
		return conductor.PendingRegistration{}, err
	}
	conductorInstance := client.conductor
	defer conductorInstance.SetActiveCorrelationID(conductor.NullCorrelationID)
	startNs := client.nanoClock()
	deadlineNs := startNs + timeout.Nanoseconds()
	if err := conductorInstance.RegisterPendingWithDeadline(
		kind,
		correlationID,
		deadlineNs,
	); err != nil {
		return conductor.PendingRegistration{}, mapRegistrationError(err)
	}
	pendingCompleted := false
	defer func() {
		if !pendingCompleted {
			conductorInstance.CancelPending(correlationID)
		}
	}()

	conductorInstance.SetActiveCorrelationID(correlationID)
	if offer != nil {
		if err := offer(); err != nil {
			return conductor.PendingRegistration{}, err
		}
	}
	registration, completed, err := client.awaitPendingPoll(
		conductorInstance,
		correlationID,
		deadlineNs,
		func(pollErr error) error {
			return mapPollPendingError(correlationID, timeout, pollErr)
		},
	)
	if completed {
		pendingCompleted = true
		if err != nil {
			return conductor.PendingRegistration{}, err
		}
		return registration, nil
	}
	if err != nil {
		return conductor.PendingRegistration{}, err
	}
	return conductor.PendingRegistration{}, newAwaitTimeoutError(correlationID, timeout)
}

func (client *Client) awaitPendingPollTick(
	conductorInstance *conductor.Conductor,
	correlationID int64,
	mapPollErr func(error) error,
) (conductor.PendingRegistration, bool, error) {
	if err := client.requireReadyWithConductor(); err != nil {
		return conductor.PendingRegistration{}, false, err
	}
	if conductorInstance.IsTerminating() {
		return conductor.PendingRegistration{}, false, mapConductorTerminationError(conductorInstance)
	}
	client.doWorkLocked()
	done, registration, err := conductorInstance.PollPending(correlationID)
	if !done {
		return conductor.PendingRegistration{}, false, nil
	}
	return client.completeAwaitPendingPollTick(
		conductorInstance,
		registration,
		mapPollErr,
		err,
	)
}

func (client *Client) completeAwaitPendingPollTick(
	conductorInstance *conductor.Conductor,
	registration conductor.PendingRegistration,
	mapPollErr func(error) error,
	pollErr error,
) (conductor.PendingRegistration, bool, error) {
	if pollErr != nil {
		return conductor.PendingRegistration{}, true, mapPollErr(pollErr)
	}
	// Close CAS may publish closed after the iteration-start check; best-effort
	// ErrClientClosed rather than returning success on the same tick.
	if err := client.errIfClosed(); err != nil {
		conductorInstance.RollbackAddPendingResources(registration)
		return registration, true, err
	}
	return registration, true, nil
}

func (client *Client) awaitPendingPoll(
	conductorInstance *conductor.Conductor,
	correlationID int64,
	deadlineNs int64,
	mapPollErr func(error) error,
) (conductor.PendingRegistration, bool, error) {
	for client.nanoClock() < deadlineNs {
		registration, completed, err := client.awaitPendingPollTick(
			conductorInstance,
			correlationID,
			mapPollErr,
		)
		if err != nil {
			return registration, completed, err
		}
		if completed {
			return registration, true, nil
		}
		invokeAwaitPollSleepingHook(client)
		time.Sleep(client.idleSleepDuration)
	}
	return conductor.PendingRegistration{}, false, nil
}

//nolint:staticcheck // Aeron Java API uses ClientId.
func (client *Client) ClientId() int64 {
	if client == nil {
		return 0
	}
	return client.clientID
}

// InterServiceTimeout returns the longest DoWork gap the Client tolerates.
// Connect resolves a zero Config.InterServiceTimeout from the driver.
func (client *Client) InterServiceTimeout() time.Duration {
	if client == nil {
		return 0
	}
	return client.interServiceTimeout
}

func (client *Client) CncMetadata() (cnc.Metadata, error) {
	mapped, err := client.mappedCncFile()
	if err != nil {
		return cnc.Metadata{}, err
	}
	return mapped.Metadata(), nil
}

func (client *Client) CountersReader() (*cnc.CountersReader, error) {
	if err := client.errIfClosed(); err != nil {
		return nil, err
	}
	if client.countersReader == nil {
		return nil, errors.New("aeron: client has no counters reader")
	}
	return client.countersReader, nil
}

func (client *Client) ErrorLogBuffer() ([]byte, error) {
	mapped, err := client.mappedCncFile()
	if err != nil {
		return nil, err
	}
	return mapped.ErrorLogBuffer(), nil
}

func (client *Client) mappedCncFile() (*cnc.MappedCncFile, error) {
	if err := client.errIfClosed(); err != nil {
		return nil, err
	}
	if client.mapped == nil {
		return nil, errors.New("aeron: client has no mapped cnc file")
	}
	return client.mapped, nil
}

func driverShutdownHeartbeatError(heartbeatMs int64) error {
	if heartbeatMs == conductor.DriverShutdownHeartbeat {
		return &DriverTimeoutError{Cause: conductor.ErrDriverShutdown}
	}
	return nil
}

// Epoch deadline is the connect timeout; wall clock and driverHeartbeatPollInterval
// backstop frozen-epoch tests and shutdown observed after the poll sleep only.
//
// Wall wait uses the earlier of timeout and ctx.Deadline(). Returns ctx.Err() when
// the context is done before a heartbeat is accepted.
func waitForDriverHeartbeat(
	ctx context.Context,
	toDriver *ringbuffer.ManyToOneRingBuffer,
	timeout time.Duration,
	epochClock conductor.EpochClock,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	driverTimeoutMs := timeout.Milliseconds()
	startMs := epochClock()
	deadlineMs := startMs + driverTimeoutMs
	wallBound := EffectiveRetryBound(ctx, timeout, time.Now())
	wallDeadline := time.Now().Add(wallBound)
	sawPositiveHeartbeat := false

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		heartbeatMs := toDriver.ConsumerHeartbeatTime()
		if err := driverShutdownHeartbeatError(heartbeatMs); err != nil {
			return PreferContextError(ctx, err)
		}

		nowMs := epochClock()
		if heartbeatMs > 0 {
			sawPositiveHeartbeat = true
			ageMs := max(nowMs-heartbeatMs, 0)
			if ageMs <= driverTimeoutMs {
				return nil
			}
			break
		}

		if nowMs >= deadlineMs || !time.Now().Before(wallDeadline) {
			break
		}
		if err := sleepContext(ctx, driverHeartbeatPollInterval); err != nil {
			return err
		}
	}

	if err := driverShutdownHeartbeatError(toDriver.ConsumerHeartbeatTime()); err != nil {
		return PreferContextError(ctx, err)
	}

	cause := ErrDriverHeartbeatNotReady
	if sawPositiveHeartbeat {
		cause = ErrDriverHeartbeatStaleAtConnect
	}
	return PreferContextError(ctx, &DriverTimeoutError{Cause: cause})
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func validateClientID(clientID int64) error {
	if clientID < 0 {
		return errors.New("aeron: invalid client id from driver ring")
	}
	return nil
}

func validateControlProtocolVersion(reader *cnc.CountersReader) error {
	want := int64(conductor.ControlProtocolVersion)
	got, err := reader.CounterValue(conductor.SystemCounterControlProtocolVersion)
	if err != nil {
		return fmt.Errorf("aeron: read control protocol version counter: %w", err)
	}
	if got != want {
		return &ControlProtocolVersionError{Got: got, Want: want}
	}
	return nil
}
