package conductor

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/aeron/logbuffer"
	"github.com/warrenguy/luas/broadcast"
)

var (
	ErrDriverShutdown          = errors.New("conductor: media driver has been shutdown")
	ErrClientTimeout           = errors.New("conductor: client timeout from driver")
	ErrConductorServiceTimeout = errors.New(
		"conductor: service interval exceeded",
	)
	ErrResourceLifecycleRequired = errors.New(
		"conductor: ResourceLifecycle is required when ImageLifecycle is set",
	)
	ErrNegativeResourceLingerNs = errors.New(
		"conductor: ResourceLingerNs must not be negative",
	)
)

// HeartbeatCounterClosedError reports a previously found client heartbeat
// timestamp counter whose record is no longer active.
//
// Behaviour: Unwrap returns ErrClientTimeout.
type HeartbeatCounterClosedError struct {
	CounterID int32
}

func (err *HeartbeatCounterClosedError) Error() string {
	return fmt.Sprintf(
		"conductor: unexpected close of heartbeat timestamp counter: %d",
		err.CounterID,
	)
}

func (err *HeartbeatCounterClosedError) Unwrap() error {
	return ErrClientTimeout
}

type DriverKeepaliveStaleError struct {
	AgeMs     int64
	TimeoutMs int64
}

func (err *DriverKeepaliveStaleError) Error() string {
	return fmt.Sprintf(
		"conductor: media driver keepalive stale: age=%dms timeout=%dms",
		err.AgeMs,
		err.TimeoutMs,
	)
}

type EpochClock func() int64

type NanoClock func() int64

// HandlerScope marks the user handler calls that the conductor makes
// on the invoker goroutine.
type HandlerScope interface {
	// EnterHandler opens the scope for the caller when none is open. A
	// nested call returns false.
	EnterHandler() bool

	// ExitHandler clears the scope. Call ExitHandler only when EnterHandler
	// returns true.
	ExitHandler()
}

type Config struct {
	ClientID int64
	// DriverTimeout is compared against epoch-millisecond driver heartbeats;
	// sub-millisecond values truncate via Milliseconds().
	DriverTimeout       time.Duration
	KeepAliveInterval   time.Duration
	IdleSleepDuration   time.Duration
	InterServiceTimeout time.Duration
	EpochClock          EpochClock
	NanoClock           NanoClock
	// ErrorHandler receives conductor faults and recovered panics from image
	// handlers and PublicationErrorFrameHandler. Nil swallows those errors.
	// The conductor calls it without holding a resource lock. A retire error
	// arrives after the conductor closes the handle and removes its map entry.
	ErrorHandler func(error)
	// HandlerScope is optional. Nil skips the scope.
	HandlerScope HandlerScope
	// SubscriberErrorHandler is optional; nil swallows subscriber-side errors.
	SubscriberErrorHandler func(error)
	// ImageLifecycle is optional; nil drops image attach/detach events.
	ImageLifecycle ImageLifecycle
	// ResourceLifecycle is optional; nil skips handle construction on ready events.
	ResourceLifecycle ResourceLifecycle
	// MediaDriverDir confines log file paths from the driver; required when
	// ImageLifecycle is non-nil.
	MediaDriverDir string
	// Zero unmaps a log buffer when its last reference drops.
	ResourceLingerNs int64
	// PublicationErrorFrameHandler is optional. Nil drops every publication
	// error frame. The conductor calls the handler once per publication
	// whose OriginalRegistrationID equals the frame RegistrationID. The call
	// does not hold a resource lock. The handler may close publications.
	// A Client method that needs clientLock returns ErrReentrantCall.
	PublicationErrorFrameHandler PublicationErrorHandler
}

func ValidateConfig(config Config) error {
	if config.ImageLifecycle != nil && config.ResourceLifecycle == nil {
		return ErrResourceLifecycleRequired
	}
	if config.ResourceLingerNs < 0 {
		return ErrNegativeResourceLingerNs
	}
	return nil
}

type Conductor struct {
	driverProxy            *DriverProxy
	eventsAdapter          *DriverEventsAdapter
	countersReader         *cnc.CountersReader
	config                 Config
	driverTimeoutMs        int64
	idleSleepNs            int64
	interServiceTimeoutNs  int64
	keepAliveIntervalNs    int64
	timeOfLastKeepAliveNs  int64
	timeOfLastServiceNs    int64
	heartbeatCounterID     int32
	livenessProbeRequested bool
	pending                pendingRegistry
	activeCorrelationID    int64
	receivedCorrelationID  int64
	// Resource maps; populated by ready handlers and subscription pre-insert.
	publications  map[int64]PublicationHandle
	subscriptions map[int64]SubscriptionHandle
	// logBuffersRegistry is the sole log-buffer map: dedup mmap, refCount, linger.
	logBuffersRegistry *LogBuffersRegistry
	resourceMu         sync.RWMutex
	// resourceWriteLockDepth is updated only under resourceMu write lock;
	// suspend/resume during image handlers lets foreign drain acquire write lock.
	resourceWriteLockDepth int32
	imageLifecycle         ImageLifecycle
	resourceLifecycle      ResourceLifecycle
	// cachedCorrelatedPending avoids a second pending.Get on the adapter ready path.
	cachedCorrelatedPending *PendingRegistration
	// inCallback is agent-only; suppresses nested image handler delivery.
	inCallback       bool
	terminationCause error
	// Client.IsTerminating reads terminating without clientLock.
	terminating atomic.Bool
}

func NewConductor(
	driverProxy *DriverProxy,
	broadcastReceiver *broadcast.BroadcastReceiver,
	countersReader *cnc.CountersReader,
	config Config,
) (*Conductor, error) {
	if err := ValidateConfig(config); err != nil {
		return nil, err
	}
	return newConductor(driverProxy, broadcastReceiver, countersReader, config), nil
}

func newConductor(
	driverProxy *DriverProxy,
	broadcastReceiver *broadcast.BroadcastReceiver,
	countersReader *cnc.CountersReader,
	config Config,
) *Conductor {
	conductor := &Conductor{
		driverProxy:           driverProxy,
		countersReader:        countersReader,
		config:                config,
		driverTimeoutMs:       config.DriverTimeout.Milliseconds(),
		idleSleepNs:           config.IdleSleepDuration.Nanoseconds(),
		interServiceTimeoutNs: config.InterServiceTimeout.Nanoseconds(),
		keepAliveIntervalNs:   config.KeepAliveInterval.Nanoseconds(),
		heartbeatCounterID:    cnc.NullCounterID,
		pending:               newPendingRegistry(),
		publications:          make(map[int64]PublicationHandle),
		subscriptions:         make(map[int64]SubscriptionHandle),
		logBuffersRegistry:    newUnmappingLogBuffersRegistry(config.MediaDriverDir),
		imageLifecycle:        config.ImageLifecycle,
		resourceLifecycle:     config.ResourceLifecycle,
		activeCorrelationID:   NullCorrelationID,
		receivedCorrelationID: NullCorrelationID,
	}
	copyReceiver := NewCopyBroadcastReceiver(broadcastReceiver)
	conductor.eventsAdapter = NewDriverEventsAdapter(
		config.ClientID,
		copyReceiver,
		conductor.handleClientTimeout,
		DriverEventsAdapterHandlers{
			PublicationReady:          conductor.handlePublicationReady,
			ExclusivePublicationReady: conductor.handleExclusivePublicationReady,
			SubscriptionReady:         conductor.handleSubscriptionReady,
			AvailableImage:            conductor.handleAvailableImage,
			UnavailableImage:          conductor.handleUnavailableImage,
			PublicationError:          conductor.handlePublicationError,
			CorrelatedReady:           conductor.handleCorrelatedEvent,
			UnknownRegistration:       conductor.handleUnknownRegistrationError,
		},
		&conductor.pending,
	)
	if config.NanoClock != nil {
		conductor.SeedServiceTimestamps(config.NanoClock())
	}
	return conductor
}

func (conductor *Conductor) DoWork() int {
	if conductor.terminating.Load() {
		return 0
	}
	return conductor.service()
}

func (conductor *Conductor) IsTerminating() bool {
	return conductor.terminating.Load()
}

func (conductor *Conductor) TerminationCause() error {
	return conductor.terminationCause
}

func (conductor *Conductor) SeedServiceTimestamps(nowNs int64) {
	conductor.timeOfLastServiceNs = nowNs
	conductor.timeOfLastKeepAliveNs = nowNs
}

func (conductor *Conductor) service() int {
	nowNs := conductor.config.NanoClock()
	workCount := conductor.checkTimeouts(nowNs)
	if conductor.terminating.Load() {
		return workCount
	}

	conductor.receivedCorrelationID = NullCorrelationID
	conductor.cachedCorrelatedPending = nil
	received, receiveErr := conductor.eventsAdapter.Receive(
		conductor.activeCorrelationID,
		conductor,
	)
	workCount += received
	if receiveErr != nil {
		// A receive error is work, so the runner does not idle while the
		// receiver recovers.
		conductor.handleBroadcastReceiveError(receiveErr)
		workCount++
	}
	if conductor.livenessProbeRequested {
		conductor.consumeLivenessProbe()
	}
	expireWorkCount, timedOutActiveCorrelationID := conductor.pending.ExpireBefore(
		nowNs,
		conductor.receivedCorrelationID,
		conductor.activeCorrelationID,
		conductor.handlePendingExpired,
	)
	workCount += expireWorkCount
	if timedOutActiveCorrelationID != NullCorrelationID {
		conductor.receivedCorrelationID = timedOutActiveCorrelationID
	}
	// Re-sample NanoClock so linger sweep sees time advanced during this tick.
	if conductor.logBuffersRegistry.HasScheduledLinger() {
		workCount += underResourceWriteLockResult(conductor, func() int {
			invokeLingerSweepBlocker(conductor)
			return conductor.sweepLingeringLogBuffers(conductor.config.NanoClock())
		})
	}
	return workCount
}

// handleBroadcastReceiveError applies a Receive error from the events adapter.
//
// Behaviour:
//   - ErrBroadcastLapped: requests a liveness probe. Passes the error to
//     ErrorHandler. Does not terminate.
//   - Any other error: terminates through terminateWithProbedCause.
//
// Concurrency: invoker-only.
func (conductor *Conductor) handleBroadcastReceiveError(err error) {
	if !errors.Is(err, ErrBroadcastLapped) {
		conductor.terminateWithProbedCause(err)
		return
	}
	conductor.livenessProbeRequested = true
	conductor.ReportError(err)
}

// consumeLivenessProbe applies a heartbeat-counter probe after Receive returns.
// Dispatch has already completed.
//
// Concurrency: call from the goroutine that runs [Conductor.DoWork]. Not safe
// from other goroutines.
func (conductor *Conductor) consumeLivenessProbe() {
	if conductor.terminating.Load() {
		return
	}
	conductor.livenessProbeRequested = false
	if conductor.hasReclaimedHeartbeatCounter() {
		conductor.terminateHeartbeatCounterClosed()
	}
}

func (conductor *Conductor) checkTimeouts(nowNs int64) int {
	if (conductor.timeOfLastServiceNs+conductor.idleSleepNs)-nowNs >= 0 {
		return 0
	}

	conductor.checkServiceInterval(nowNs)
	conductor.timeOfLastServiceNs = nowNs
	if conductor.terminating.Load() {
		return 1
	}

	return conductor.checkLiveness(nowNs)
}

// checkServiceInterval terminates when the service interval has expired.
//
// Behaviour:
//   - A reclaimed heartbeat counter terminates with HeartbeatCounterClosedError.
//   - Otherwise terminates with ErrConductorServiceTimeout.
func (conductor *Conductor) checkServiceInterval(nowNs int64) {
	if (conductor.timeOfLastServiceNs+conductor.interServiceTimeoutNs)-nowNs >= 0 {
		return
	}

	conductor.terminateWithProbedCause(ErrConductorServiceTimeout)
}

// terminateWithProbedCause terminates with HeartbeatCounterClosedError when
// the heartbeat counter is reclaimed. Otherwise terminates with cause.
func (conductor *Conductor) terminateWithProbedCause(cause error) {
	if conductor.hasReclaimedHeartbeatCounter() {
		conductor.terminateHeartbeatCounterClosed()
		return
	}
	conductor.terminateWithError(cause)
}

// hasReclaimedHeartbeatCounter reports a previously found client heartbeat
// counter whose record is no longer active for this client.
//
// Behaviour: reports false while the counter was never found. A reclaimed
// or reused record means the driver removed this client. Reads record
// state through IsCounterActive. Does not read the counter value.
//
// Concurrency: invoker-only.
func (conductor *Conductor) hasReclaimedHeartbeatCounter() bool {
	if conductor.heartbeatCounterID == cnc.NullCounterID {
		return false
	}
	return !conductor.countersReader.IsCounterActive(
		conductor.heartbeatCounterID,
		conductor.config.ClientID,
		cnc.ClientHeartbeatTypeID,
	)
}

func (conductor *Conductor) terminateHeartbeatCounterClosed() {
	conductor.terminateWithError(&HeartbeatCounterClosedError{
		CounterID: conductor.heartbeatCounterID,
	})
}

func (conductor *Conductor) checkLiveness(nowNs int64) int {
	if (conductor.timeOfLastKeepAliveNs+conductor.keepAliveIntervalNs)-nowNs >= 0 {
		return 0
	}

	nowMs := conductor.config.EpochClock()
	lastKeepaliveMs := conductor.driverProxy.LastDriverKeepaliveMs()

	if lastKeepaliveMs == DriverShutdownHeartbeat {
		conductor.terminateWithError(ErrDriverShutdown)
		return 1
	}

	// Unpublished driver heartbeat: wait for Connect/harness freshness gate; do not false-stale.
	if lastKeepaliveMs <= 0 {
		return 1
	}

	if nowMs > lastKeepaliveMs+conductor.driverTimeoutMs {
		conductor.terminateWithError(&DriverKeepaliveStaleError{
			AgeMs:     nowMs - lastKeepaliveMs,
			TimeoutMs: conductor.driverTimeoutMs,
		})
		return 1
	}

	if conductor.heartbeatCounterID == cnc.NullCounterID {
		counterID, ok := conductor.countersReader.FindHeartbeatCounter(
			conductor.config.ClientID,
			cnc.ClientHeartbeatTypeID,
		)
		if !ok {
			return 1
		}
		conductor.heartbeatCounterID = counterID
	} else if conductor.hasReclaimedHeartbeatCounter() {
		conductor.terminateHeartbeatCounterClosed()
		return 1
	}

	return conductor.storeClientHeartbeat(nowNs, nowMs)
}

func (conductor *Conductor) storeClientHeartbeat(nowNs, nowMs int64) int {
	// Reclaim may land between the active check above and this store; next tick terminates.
	if err := conductor.countersReader.SetCounterValue(
		conductor.heartbeatCounterID,
		nowMs,
	); err != nil {
		conductor.terminateWithError(err)
		return 1
	}
	conductor.timeOfLastKeepAliveNs = nowNs
	return 1
}

func (conductor *Conductor) handleClientTimeout() {
	if conductor.terminating.Load() {
		return
	}
	conductor.terminateWithError(ErrClientTimeout)
}

// handleUnknownRegistrationError requests a heartbeat-counter probe after
// Receive returns.
//
// Behaviour: sets livenessProbeRequested. Does not terminate. Dispatch
// continues so a correlated pending can fail with DriverRegistrationError.
//
// Concurrency: invoker-only.
func (conductor *Conductor) handleUnknownRegistrationError() {
	conductor.livenessProbeRequested = true
}

// Add kinds complete in handlePublicationReady, handleExclusivePublicationReady,
// and handleSubscriptionReady; do not set receivedCorrelationID here or
// PollPending would finish before metadata is filled.
func (conductor *Conductor) handleCorrelatedEvent(correlationID int64) {
	registration, ok := conductor.pending.Get(correlationID)
	if !ok {
		conductor.cachedCorrelatedPending = nil
		conductor.receivedCorrelationID = correlationID
		return
	}
	conductor.cachedCorrelatedPending = registration
	if !addPendingKind(registration.Kind) {
		conductor.receivedCorrelationID = correlationID
	}
}

func (conductor *Conductor) pendingForReady(correlationID int64) (*PendingRegistration, bool) {
	cached := conductor.cachedCorrelatedPending
	if cached != nil && cached.CorrelationID == correlationID {
		conductor.cachedCorrelatedPending = nil
		return cached, true
	}
	return conductor.pending.Get(correlationID)
}

func (conductor *Conductor) addReadyRegistration(
	correlationID int64,
	wantKind PendingKind,
) (*PendingRegistration, bool) {
	registration, ok := conductor.pendingForReady(correlationID)
	if !ok || registration.Kind != wantKind {
		return nil, false
	}
	if registration.Err != nil || registration.Ready {
		return nil, false
	}
	return registration, true
}

func (conductor *Conductor) handlePublicationReady(event PublicationBuffersReady) {
	conductor.completePublicationReady(event, PendingKindAddPublication)
}

func (conductor *Conductor) handleExclusivePublicationReady(event PublicationBuffersReady) {
	conductor.completePublicationReady(event, PendingKindAddExclusivePublication)
}

func (conductor *Conductor) handleSubscriptionReady(event SubscriptionReady) {
	conductor.completeSubscriptionReady(event)
}

func (conductor *Conductor) completePublicationReady(
	event PublicationBuffersReady,
	wantKind PendingKind,
) bool {
	registration, ok := conductor.addReadyRegistration(
		event.CorrelationID,
		wantKind,
	)
	if !ok {
		return false
	}

	var publication PublicationHandle
	if conductor.resourceLifecycle != nil {
		var buildErr error
		publication, buildErr = conductor.buildAndInsertPublication(event, registration)
		if buildErr != nil {
			conductor.failPending(registration, buildErr)
			return false
		}
	}

	return conductor.completePendingSuccess(registration, func(registration *PendingRegistration) {
		registration.StreamID = event.StreamID
		registration.Publication = publication
	})
}

func (conductor *Conductor) completeSubscriptionReady(event SubscriptionReady) bool {
	registration, ok := conductor.addReadyRegistration(
		event.CorrelationID,
		PendingKindAddSubscription,
	)
	if !ok {
		return false
	}

	var subscription SubscriptionHandle
	if conductor.resourceLifecycle != nil {
		var buildErr error
		subscription, buildErr = conductor.applySubscriptionReady(event, registration)
		if buildErr != nil {
			conductor.failPending(registration, buildErr)
			return false
		}
	}

	return conductor.completePendingSuccess(registration, func(registration *PendingRegistration) {
		registration.Subscription = subscription
	})
}

func (conductor *Conductor) buildAndInsertPublication(
	event PublicationBuffersReady,
	registration *PendingRegistration,
) (PublicationHandle, error) {
	var publication PublicationHandle
	var buildErr error
	conductor.underResourceWriteLock(func() {
		resolvedLogFileKey, err := conductor.logBuffersRegistry.resolveLogFileKey(event.LogFileName)
		if err != nil {
			buildErr = err
			return
		}
		logBuffers, err := conductor.acquireLogBuffersByKey(resolvedLogFileKey)
		if err != nil {
			buildErr = err
			return
		}
		publication, buildErr = newPublicationForPendingKind(
			conductor.resourceLifecycle,
			registration.Kind,
			event,
			logBuffers,
			conductor.countersReader,
			registration.Channel,
		)
		if buildErr == nil {
			buildErr = checkPublicationHandle(publication, event.CorrelationID)
		}
		if buildErr != nil {
			conductor.releaseLogBuffersByKey(resolvedLogFileKey, conductor.config.ResourceLingerNs)
			return
		}
		conductor.insertPublication(event.CorrelationID, publication)
		if bindErr := conductor.bindPublicationOnInsert(publication); bindErr != nil {
			buildErr = bindErr
			return
		}
	})
	return publication, buildErr
}

// checkPublicationHandle rejects a nil handle. It also rejects a handle whose
// RegistrationID differs from the ready correlation id. Insert, retire, and
// rollback all look the publication up by that id, so the two must match.
func checkPublicationHandle(publication PublicationHandle, correlationID int64) error {
	if isAbsentMapValue(publication) {
		return errors.New("conductor: resource lifecycle returned nil publication handle")
	}
	registrationID := publication.RegistrationID()
	if registrationID != correlationID {
		return fmt.Errorf(
			"conductor: publication handle registration id %d, want %d",
			registrationID,
			correlationID,
		)
	}
	return nil
}

func (conductor *Conductor) applySubscriptionReady(
	event SubscriptionReady,
	registration *PendingRegistration,
) (SubscriptionHandle, error) {
	var subscription SubscriptionHandle
	var buildErr error
	conductor.underResourceWriteLock(func() {
		var readyErr error
		subscription, readyErr = conductor.subscriptionForReady(event.CorrelationID, registration)
		if readyErr != nil {
			buildErr = readyErr
			return
		}
		buildErr = conductor.resourceLifecycle.ApplySubscriptionReady(subscription, event)
	})
	return subscription, buildErr
}

func (conductor *Conductor) subscriptionForReady(
	correlationID int64,
	registration *PendingRegistration,
) (SubscriptionHandle, error) {
	if subscription, ok := conductor.subscriptionByRegID(correlationID); ok {
		return subscription, nil
	}
	return conductor.createAndInsertPreSubscription(
		correlationID,
		registration.StreamID,
		registration.Channel,
	)
}

func (conductor *Conductor) createAndInsertPreSubscription(
	correlationID int64,
	streamID int32,
	channel string,
) (SubscriptionHandle, error) {
	subscription := conductor.resourceLifecycle.NewPreSubscription(
		correlationID,
		streamID,
		channel,
	)
	conductor.insertSubscription(correlationID, subscription)
	if bindErr := conductor.bindSubscriptionOnInsert(subscription); bindErr != nil {
		return nil, bindErr
	}
	return subscription, nil
}

// preInsertSubscription inserts subscriptions[correlationID] before AddSubscription
// offer. RegisterPendingAdd invokes this immediately after pending registration;
// callers must offer synchronously so image events do not arrive in the gap.
func (conductor *Conductor) preInsertSubscription(
	correlationID int64,
	streamID int32,
	channel string,
) error {
	if conductor.resourceLifecycle == nil {
		return nil
	}
	var preInsertErr error
	conductor.underResourceWriteLock(func() {
		if _, ok := conductor.subscriptionByRegID(correlationID); ok {
			return
		}
		_, preInsertErr = conductor.createAndInsertPreSubscription(
			correlationID,
			streamID,
			channel,
		)
	})
	return preInsertErr
}

// RollbackAddPendingResources retires the resource that an add kind stored
// under registration.CorrelationID.
//
// Behaviour:
//   - A subscription enters the map when the client offers the add. A
//     publication enters the map when its ready event arrives.
//   - Releases log buffer references with Config.ResourceLingerNs.
//   - Does not offer REMOVE commands to the driver.
//
// A publication is missing from the map when:
//   - the ready event never arrived
//   - a bind failure retired the handle
//   - Close retired the handle
func (conductor *Conductor) RollbackAddPendingResources(registration PendingRegistration) {
	if conductor.resourceLifecycle == nil {
		return
	}
	err := underResourceWriteLockResult(conductor, func() error {
		switch registration.Kind {
		case PendingKindAddSubscription:
			sub, ok := conductor.subscriptionByRegID(registration.CorrelationID)
			if !ok {
				return nil
			}
			registryKeys, err := conductor.resourceLifecycle.RetireSubscription(sub)
			if err != nil {
				conductor.resourceLifecycle.MarkSubscriptionClosed(sub)
			}
			conductor.releaseLogBuffersByKeys(registryKeys, conductor.config.ResourceLingerNs)
			conductor.removeSubscription(registration.CorrelationID)
			return err

		case PendingKindAddPublication, PendingKindAddExclusivePublication:
			pub, ok := conductor.publicationByRegID(registration.CorrelationID)
			if !ok {
				return nil
			}
			registryKey, err := conductor.resourceLifecycle.RetirePublication(pub)
			if err != nil {
				conductor.resourceLifecycle.MarkPublicationClosed(pub)
			}
			if registryKey != "" {
				conductor.releaseLogBuffersByKey(registryKey, conductor.config.ResourceLingerNs)
			}
			conductor.removePublication(registration.CorrelationID)
			return err

		case PendingKindOperationSuccess,
			PendingKindNextSessionID,
			PendingKindRemovePublication,
			PendingKindRemoveSubscription,
			PendingKindAddCounter,
			PendingKindDestination:
		}
		return nil
	})
	conductor.ReportError(err)
}

func (conductor *Conductor) bindPublicationOnInsert(publication PublicationHandle) error {
	if isAbsentMapValue(publication) {
		return nil
	}
	return conductor.resourceLifecycle.BindPublication(publication, conductor)
}

func (conductor *Conductor) bindSubscriptionOnInsert(subscription SubscriptionHandle) error {
	if isAbsentMapValue(subscription) {
		return nil
	}
	return conductor.resourceLifecycle.BindSubscription(subscription, conductor)
}

func (conductor *Conductor) handlePendingExpired(
	registration PendingRegistration,
	removed bool,
) {
	if !removed {
		return
	}
	conductor.RollbackAddPendingResources(registration)
}

// failPending rolls back add resources and sets Err on the entry. A
// PendingKindDestination entry leaves receivedCorrelationID unchanged, so the
// error displaces no other await's mark.
func (conductor *Conductor) failPending(registration *PendingRegistration, err error) {
	conductor.RollbackAddPendingResources(*registration)
	registration.Err = err
	if registration.Kind == PendingKindDestination {
		return
	}
	conductor.receivedCorrelationID = registration.CorrelationID
}

func (conductor *Conductor) handlePublicationError(event PublicationError) {
	handler := conductor.config.PublicationErrorFrameHandler
	if handler == nil {
		return
	}
	matchCount := withResourceRead(conductor, func() int {
		return countPublicationsByOriginalRegistrationID(
			conductor.publications,
			event.RegistrationID,
		)
	})
	if matchCount == 0 {
		return
	}

	// One scope covers every match.
	entered := conductor.enterHandler()
	defer conductor.exitHandler(entered)

	// Call the handler outside the read lock. A handler that closes a
	// publication takes resourceMu.
	for range matchCount {
		conductor.invokePublicationErrorFrameHandler(handler, event)
	}
}

// invokePublicationErrorFrameHandler calls handler. A recovered panic goes to
// ErrorHandler.
func (conductor *Conductor) invokePublicationErrorFrameHandler(
	handler PublicationErrorHandler,
	event PublicationError,
) {
	defer func() {
		conductor.reportHandlerPanic(recover(), "publication error frame handler")
	}()
	handler(event)
}

// terminateWithError records err as the termination cause. The conductor stays
// terminating after this call.
//
// Behaviour: does not offer REMOVE commands to the driver.
//
// Concurrency: call from the goroutine that runs [Conductor.DoWork], inside
// service. Not safe from other goroutines. The caller must not hold resourceMu.
func (conductor *Conductor) terminateWithError(err error) {
	entered := conductor.enterHandler()
	defer conductor.exitHandler(entered)

	conductor.terminating.Store(true)
	conductor.terminationCause = err
	conductor.ClearPending()
	conductor.forceCloseResources()
	conductor.ReportError(err)
}

func newPublicationForPendingKind(
	lifecycle ResourceLifecycle,
	kind PendingKind,
	event PublicationBuffersReady,
	logBuffers *logbuffer.LogBuffers,
	countersReader *cnc.CountersReader,
	channel string,
) (PublicationHandle, error) {
	switch kind {
	case PendingKindAddPublication:
		return lifecycle.NewPublication(event, logBuffers, countersReader, channel)
	case PendingKindAddExclusivePublication:
		return lifecycle.NewExclusivePublication(event, logBuffers, countersReader, channel)
	case PendingKindOperationSuccess,
		PendingKindNextSessionID,
		PendingKindAddSubscription,
		PendingKindRemovePublication,
		PendingKindRemoveSubscription,
		PendingKindAddCounter,
		PendingKindDestination:
	}
	return nil, fmt.Errorf("conductor: pending kind %v is not a publication kind", kind)
}

// ReportError calls ErrorHandler inside HandlerScope. Production callers
// hold clientLock. Production callers do not hold resourceMu.
func (conductor *Conductor) ReportError(err error) {
	if err == nil || conductor.config.ErrorHandler == nil {
		return
	}
	entered := conductor.enterHandler()
	defer conductor.exitHandler(entered)
	conductor.config.ErrorHandler(err)
}

// reportCloseError does not enter HandlerScope. Publication,
// ExclusivePublication, and Subscription Close do not hold clientLock.
// A scope on those Close calls would store the wrong goroutine id.
func (conductor *Conductor) reportCloseError(err error) {
	if err == nil || conductor.config.ErrorHandler == nil {
		return
	}
	conductor.config.ErrorHandler(err)
}

func (conductor *Conductor) enterHandler() bool {
	scope := conductor.config.HandlerScope
	if scope == nil {
		return false
	}
	return scope.EnterHandler()
}

func (conductor *Conductor) exitHandler(entered bool) {
	if entered {
		conductor.config.HandlerScope.ExitHandler()
	}
}
