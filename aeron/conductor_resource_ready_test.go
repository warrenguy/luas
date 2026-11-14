package aeron

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/aeron/conductor"
)

const (
	resourceReadyPublicationLimitCounterID   int32 = 0
	resourceReadyPublicationChannelCounterID int32 = 1
	resourceReadySubscriberPositionCounterID int32 = 2

	resourceReadyPublicationCorrelationID    int64 = 1001
	resourceReadySubscriptionCorrelationID   int64 = 3003
	resourceReadyPublicationRegistrationID   int64 = 2002
	resourceReadyAvailableImageCorrelationID int64 = 4004

	resourceReadyStreamID int32 = 7
	resourceReadyChannel        = "aeron:ipc"
)

var (
	resourceReadyGoldenPublicationReady = conductor.PublicationBuffersReady{
		CorrelationID:             resourceReadyPublicationCorrelationID,
		RegistrationID:            resourceReadyPublicationRegistrationID,
		SessionID:                 42,
		StreamID:                  resourceReadyStreamID,
		PublicationLimitCounterID: resourceReadyPublicationLimitCounterID,
		ChannelStatusCounterID:    resourceReadyPublicationChannelCounterID,
	}
	resourceReadyGoldenSubscriptionReady = conductor.SubscriptionReady{
		CorrelationID:          resourceReadySubscriptionCorrelationID,
		ChannelStatusCounterID: 12,
	}
	resourceReadyGoldenAvailableImage = conductor.AvailableImage{
		CorrelationID:              resourceReadyAvailableImageCorrelationID,
		SessionID:                  42,
		StreamID:                   resourceReadyStreamID,
		SubscriptionRegistrationID: resourceReadySubscriptionCorrelationID,
		SubscriberPositionID:       resourceReadySubscriberPositionCounterID,
		SourceIdentity:             "ipc",
	}
)

type applyFailingResourceLifecycle struct {
	conductorResourceLifecycle
	applyErr error
}

func (lifecycle applyFailingResourceLifecycle) ApplySubscriptionReady(
	sub conductor.SubscriptionHandle,
	event conductor.SubscriptionReady,
) error {
	if lifecycle.applyErr != nil {
		return lifecycle.applyErr
	}
	return lifecycle.conductorResourceLifecycle.ApplySubscriptionReady(sub, event)
}

func setupResourceReadyConductorTest(
	tb testing.TB,
	logFilename string,
	resourceLifecycle conductor.ResourceLifecycle,
	exclusive bool,
	mutateConfig ...func(*conductor.Config),
) (logPath string, conductorInstance *conductor.Conductor) {
	tb.Helper()

	dir, logPath := setupConductorImageLifecycleKindLogFile(tb, logFilename, exclusive)
	mutateConfig = append([]func(*conductor.Config){
		func(config *conductor.Config) {
			config.ResourceLifecycle = resourceLifecycle
		},
	}, mutateConfig...)
	conductorInstance = newConductorWithImageLifecycleForTest(
		tb,
		noopConductorErrorHandler,
		dir,
		mutateConfig...,
	)
	conductorInstance.SetCountersReaderForTest(countersReaderForResourceReadyTest(tb))
	return logPath, conductorInstance
}

func countersReaderForResourceReadyTest(tb testing.TB) *cnc.CountersReader {
	tb.Helper()
	return newTestCountersReaderFromRecords(3, func(meta, values []byte) {
		cnc.WriteCounterRecord(
			meta,
			values,
			resourceReadyPublicationLimitCounterID,
			cnc.CounterRecordAllocated,
			cnc.DriverPublisherLimitTypeID,
			resourceReadyPublicationRegistrationID,
			0,
		)
		err := cnc.WriteChannelStatusCounterRecord(
			meta,
			values,
			resourceReadyPublicationChannelCounterID,
			cnc.CounterRecordAllocated,
			cnc.DriverSendChannelStatusTypeID,
			resourceReadyChannel,
			resourceReadyPublicationRegistrationID,
			1,
		)
		if err != nil {
			panic(err)
		}
		cnc.WriteCounterRecord(
			meta,
			values,
			resourceReadySubscriberPositionCounterID,
			cnc.CounterRecordAllocated,
			cnc.DriverSubscriberPositionTypeID,
			resourceReadySubscriptionCorrelationID,
			TestSubscriberPositionAtJoin,
		)
	})
}

func publicationReadyEventForLogPath(logPath string) conductor.PublicationBuffersReady {
	event := resourceReadyGoldenPublicationReady
	event.LogFileName = logPath
	return event
}

func availableImageEventForSubscription(logPath string) conductor.AvailableImage {
	event := resourceReadyGoldenAvailableImage
	event.LogFileName = logPath
	return event
}

func registerResourceReadyPublicationPending(
	t *testing.T,
	conductorInstance *conductor.Conductor,
	exclusive bool,
) {
	t.Helper()

	pendingKind := conductor.PendingKindAddPublication
	if exclusive {
		pendingKind = conductor.PendingKindAddExclusivePublication
	}
	require.NoError(
		t,
		conductorInstance.RegisterPendingAdd(
			pendingKind,
			resourceReadyPublicationCorrelationID,
			resourceReadyStreamID,
			resourceReadyChannel,
			time.Second,
		),
	)
	conductorInstance.SetActiveCorrelationID(resourceReadyPublicationCorrelationID)
}

func transmitResourceReadyPublicationReady(
	t *testing.T,
	conductorInstance *conductor.Conductor,
	logPath string,
	exclusive bool,
) {
	t.Helper()

	eventType := conductor.OnPublicationReady
	if exclusive {
		eventType = conductor.OnExclusivePublicationReady
	}
	transmitConductorImageLifecycleEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		eventType,
		conductor.PublicationBuffersReadyBytes(publicationReadyEventForLogPath(logPath)),
	)
	require.Equal(t, 1, conductorInstance.DoWork())
}

func registerResourceReadySubscriptionPending(
	t *testing.T,
	conductorInstance *conductor.Conductor,
) {
	t.Helper()
	require.NoError(
		t,
		conductorInstance.RegisterPendingAdd(
			conductor.PendingKindAddSubscription,
			resourceReadySubscriptionCorrelationID,
			resourceReadyStreamID,
			resourceReadyChannel,
			time.Second,
		),
	)
	_, ok := conductorInstance.SubscriptionByRegID(resourceReadySubscriptionCorrelationID)
	require.True(t, ok)
}

func transmitResourceReadyAvailableImage(
	t *testing.T,
	conductorInstance *conductor.Conductor,
	logPath string,
) {
	t.Helper()
	transmitConductorImageLifecycleEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(availableImageEventForSubscription(logPath)),
	)
	require.Equal(t, 1, conductorInstance.DoWork())
}

func transmitResourceReadySubscriptionReady(
	t *testing.T,
	conductorInstance *conductor.Conductor,
) {
	t.Helper()
	conductorInstance.SetActiveCorrelationID(resourceReadySubscriptionCorrelationID)
	transmitConductorImageLifecycleEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnSubscriptionReady,
		conductor.SubscriptionReadyBytes(resourceReadyGoldenSubscriptionReady),
	)
	require.Equal(t, 1, conductorInstance.DoWork())
}

func setupResourceReadySubscriptionWithAvailableImage(
	t *testing.T,
	conductorInstance *conductor.Conductor,
	registry *conductor.LogBuffersRegistry,
	logPath string,
) {
	t.Helper()
	registerResourceReadySubscriptionPending(t, conductorInstance)
	transmitResourceReadyAvailableImage(t, conductorInstance, logPath)
	_, ok := conductorInstance.SubscriptionByRegID(resourceReadySubscriptionCorrelationID)
	require.True(t, ok)
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
}

func TestConductor_handleExclusivePublicationReady_refcountsLog(t *testing.T) {
	logPath, conductorInstance := setupResourceReadyConductorTest(
		t,
		"exclusive-shared.log",
		ConductorResourceLifecycle,
		true,
	)

	registerResourceReadyPublicationPending(t, conductorInstance, true)
	transmitResourceReadyPublicationReady(t, conductorInstance, logPath, true)

	done, registration, err := conductorInstance.PollPending(resourceReadyPublicationCorrelationID)
	require.True(t, done)
	require.NoError(t, err)
	ep, ok := registration.Publication.(*ExclusivePublication)
	require.True(t, ok)
	gotEP, ok := conductorInstance.PublicationByRegID(resourceReadyPublicationCorrelationID)
	require.True(t, ok)
	require.Same(t, ep, gotEP)

	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
}

func TestConductor_handlePublicationReady_refcountsLog(t *testing.T) {
	logPath, conductorInstance := setupResourceReadyConductorTest(
		t,
		"shared.log",
		ConductorResourceLifecycle,
		false,
	)

	registerResourceReadyPublicationPending(t, conductorInstance, false)
	transmitResourceReadyPublicationReady(t, conductorInstance, logPath, false)

	done, registration, err := conductorInstance.PollPending(resourceReadyPublicationCorrelationID)
	require.True(t, done)
	require.NoError(t, err)
	pub, ok := registration.Publication.(*Publication)
	require.True(t, ok)
	gotPub, ok := conductorInstance.PublicationByRegID(resourceReadyPublicationCorrelationID)
	require.True(t, ok)
	require.Same(t, pub, gotPub)

	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))

	registerResourceReadySubscriptionPending(t, conductorInstance)
	transmitResourceReadyAvailableImage(t, conductorInstance, logPath)
	require.Equal(
		t,
		int32(2),
		conductor.LogBuffersRegistryRefCountForTest(registry, logPath),
	)
}

func TestConductor_availableThenSubscriptionReady_preInsert(t *testing.T) {
	logPath, conductorInstance := setupResourceReadyConductorTest(
		t,
		"image.log",
		ConductorResourceLifecycle,
		false,
	)

	registerResourceReadySubscriptionPending(t, conductorInstance)

	transmitResourceReadyAvailableImage(t, conductorInstance, logPath)

	sub, ok := conductorInstance.SubscriptionByRegID(resourceReadySubscriptionCorrelationID)
	require.True(t, ok)
	aeronSub, err := subscriptionFromHandle(sub)
	require.NoError(t, err)
	require.Equal(t, 1, aeronSub.ImageCount())

	transmitResourceReadySubscriptionReady(t, conductorInstance)

	done, registration, err := conductorInstance.PollPending(resourceReadySubscriptionCorrelationID)
	require.True(t, done)
	require.NoError(t, err)
	require.NotNil(t, registration.Subscription)
	require.Equal(t, 1, aeronSub.ImageCount())
	require.Equal(
		t,
		int32(12),
		SubscriptionChannelStatusCounterIDForTest(aeronSub),
	)

	got, ok := conductorInstance.SubscriptionByRegID(resourceReadySubscriptionCorrelationID)
	require.True(t, ok)
	require.Same(t, registration.Subscription, got)
}

func TestConductor_addSubscription_availableImageThenDriverError_restoresRefcount(t *testing.T) {
	logPath, conductorInstance := setupResourceReadyConductorTest(
		t,
		"driver-error.log",
		ConductorResourceLifecycle,
		false,
	)
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)

	setupResourceReadySubscriptionWithAvailableImage(t, conductorInstance, registry, logPath)

	require.True(
		t,
		conductorInstance.DriverError(conductor.ErrorResponse{
			OffendingCommandCorrelationID: resourceReadySubscriptionCorrelationID,
			ErrorCode:                     11,
			ErrorMessage:                  "add subscription rejected",
		}),
	)

	done, _, err := conductorInstance.PollPending(resourceReadySubscriptionCorrelationID)
	require.True(t, done)
	require.Error(t, err)
	_, ok := conductorInstance.SubscriptionByRegID(resourceReadySubscriptionCorrelationID)
	require.False(t, ok)
	require.Zero(t, conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
}

func TestConductor_addSubscription_availableImageThenApplyFailure_restoresRefcount(t *testing.T) {
	errApply := errors.New("aeron_test: apply subscription ready failed")
	logPath, conductorInstance := setupResourceReadyConductorTest(
		t,
		"apply-failure.log",
		applyFailingResourceLifecycle{applyErr: errApply},
		false,
	)
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)

	setupResourceReadySubscriptionWithAvailableImage(t, conductorInstance, registry, logPath)

	transmitResourceReadySubscriptionReady(t, conductorInstance)

	done, _, err := conductorInstance.PollPending(resourceReadySubscriptionCorrelationID)
	require.True(t, done)
	require.ErrorIs(t, err, errApply)
	_, ok := conductorInstance.SubscriptionByRegID(resourceReadySubscriptionCorrelationID)
	require.False(t, ok)
	require.Zero(t, conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
}

func TestConductor_rollbackAddPending_lingersLog(t *testing.T) {
	const (
		lingerNs = int64(time.Second)
		startNs  = int64(1_000)
	)

	cancelAfterReadyTick := func(
		t *testing.T,
		conductorInstance *conductor.Conductor,
		correlationID int64,
		_ *int64,
	) {
		conductorInstance.DoWork()
		require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
		conductorInstance.CancelPending(correlationID)
	}
	subscriptionWithImage := func(
		t *testing.T,
		conductorInstance *conductor.Conductor,
		logPath string,
	) {
		setupResourceReadySubscriptionWithAvailableImage(
			t,
			conductorInstance,
			conductor.LogBuffersRegistryForTest(conductorInstance),
			logPath,
		)
	}
	subscriptionFound := func(conductorInstance *conductor.Conductor) bool {
		_, ok := conductorInstance.SubscriptionByRegID(resourceReadySubscriptionCorrelationID)
		return ok
	}

	tests := []struct {
		name          string
		correlationID int64
		setup         func(*testing.T, *conductor.Conductor, string)
		rollback      func(*testing.T, *conductor.Conductor, int64, *int64)
		found         func(*conductor.Conductor) bool
		// untilUnmap: the log stays mapped until UnmapLingeringLogBuffers.
		untilUnmap bool
	}{
		{
			name:          "subscriptionCancelAfterReadyTick",
			correlationID: resourceReadySubscriptionCorrelationID,
			setup: func(t *testing.T, conductorInstance *conductor.Conductor, logPath string) {
				subscriptionWithImage(t, conductorInstance, logPath)
				transmitResourceReadySubscriptionReady(t, conductorInstance)
			},
			rollback: cancelAfterReadyTick,
			found:    subscriptionFound,
		},
		{
			name:          "subscriptionExpire",
			correlationID: resourceReadySubscriptionCorrelationID,
			setup:         subscriptionWithImage,
			rollback: func(
				t *testing.T,
				conductorInstance *conductor.Conductor,
				correlationID int64,
				nanoNs *int64,
			) {
				// Two steps keep each gap under the inter-service timeout.
				for _, elapsed := range []time.Duration{600 * time.Millisecond, time.Second} {
					*nanoNs = startNs + int64(elapsed)
					conductorInstance.DoWork()
				}
				require.False(t, conductorInstance.IsTerminating())

				done, _, err := conductorInstance.PollPending(correlationID)
				require.True(t, done)
				require.ErrorIs(t, err, conductor.ErrAwaitTimeout)
			},
			found: subscriptionFound,
		},
		{
			name:          "subscriptionDriverError",
			correlationID: resourceReadySubscriptionCorrelationID,
			setup:         subscriptionWithImage,
			rollback: func(
				t *testing.T,
				conductorInstance *conductor.Conductor,
				correlationID int64,
				_ *int64,
			) {
				require.True(t, conductorInstance.DriverError(conductor.ErrorResponse{
					OffendingCommandCorrelationID: correlationID,
					ErrorCode:                     11,
					ErrorMessage:                  "add subscription rejected",
				}))
			},
			found: subscriptionFound,
		},
		{
			name:          "subscriptionClearForClose",
			correlationID: resourceReadySubscriptionCorrelationID,
			setup:         subscriptionWithImage,
			rollback: func(_ *testing.T, conductorInstance *conductor.Conductor, _ int64, _ *int64) {
				conductorInstance.ClearPendingEntriesForClose()
			},
			found:      subscriptionFound,
			untilUnmap: true,
		},
		{
			name:          "publicationCancelAfterReadyTick",
			correlationID: resourceReadyPublicationCorrelationID,
			setup: func(t *testing.T, conductorInstance *conductor.Conductor, logPath string) {
				registerResourceReadyPublicationPending(t, conductorInstance, false)
				transmitResourceReadyPublicationReady(t, conductorInstance, logPath, false)
			},
			rollback: cancelAfterReadyTick,
			found: func(conductorInstance *conductor.Conductor) bool {
				_, ok := conductorInstance.PublicationByRegID(resourceReadyPublicationCorrelationID)
				return ok
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nanoNs := startNs
			logPath, conductorInstance := setupResourceReadyConductorTest(
				t,
				"rollback-linger.log",
				ConductorResourceLifecycle,
				false,
				func(config *conductor.Config) {
					config.NanoClock = func() int64 { return nanoNs }
					config.ResourceLingerNs = lingerNs
				},
			)
			registry := conductor.LogBuffersRegistryForTest(conductorInstance)

			// Setup: the resource holds the log.
			tc.setup(t, conductorInstance, logPath)
			require.True(t, tc.found(conductorInstance))

			// Rollback: the log stays mapped for the linger.
			tc.rollback(t, conductorInstance, tc.correlationID, &nanoNs)

			require.False(t, tc.found(conductorInstance))
			require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
			require.Zero(t, conductor.LogBuffersRegistryRefCountForTest(registry, logPath))

			wantDeadlineNs := nanoNs + lingerNs
			if tc.untilUnmap {
				wantDeadlineNs = math.MaxInt64
			}
			require.Equal(t, wantDeadlineNs, conductor.LingerDeadlineNsForTest(registry, logPath))
		})
	}
}
