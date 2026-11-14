package aeron

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/aeron/conductor"
	"github.com/warrenguy/luas/atomicbuffer"
	"github.com/warrenguy/luas/broadcast"
)

const (
	conductorImageLifecycleTestCorrelationID     int64 = 4004
	conductorImageLifecycleTestRegistrationID    int64 = 5005
	conductorImageLifecycleTestSubscriberCounter int32 = TestSubscriberPositionCounterID
)

var (
	conductorImageLifecycleGoldenAvailableImage = conductor.AvailableImage{
		CorrelationID:              conductorImageLifecycleTestCorrelationID,
		SessionID:                  42,
		StreamID:                   7,
		SubscriptionRegistrationID: conductorImageLifecycleTestRegistrationID,
		SubscriberPositionID:       conductorImageLifecycleTestSubscriberCounter,
		LogFileName:                "/dev/shm/aeron-publication.log",
		SourceIdentity:             "ipc",
	}
	conductorImageLifecycleGoldenUnavailableImage = conductor.UnavailableImage{
		CorrelationID:              conductorImageLifecycleTestCorrelationID,
		SubscriptionRegistrationID: conductorImageLifecycleTestRegistrationID,
		StreamID:                   7,
		Channel:                    "aeron:ipc",
	}
)

func noopConductorErrorHandler(error) {}

func newConductorWithImageLifecycleForTest(
	tb testing.TB,
	errorHandler func(error),
	mediaDriverDir string,
	mutateConfig ...func(*conductor.Config),
) *conductor.Conductor {
	tb.Helper()

	meta := make([]byte, cnc.CounterMetadataLength)
	values := make([]byte, cnc.CounterValueLength)
	countersReader, err := cnc.NewCountersReader(meta, values)
	require.NoError(tb, err)

	config := conductor.Config{
		ClientID:            42,
		DriverTimeout:       10 * time.Second,
		KeepAliveInterval:   500 * time.Millisecond,
		IdleSleepDuration:   16 * time.Millisecond,
		InterServiceTimeout: time.Second,
		EpochClock:          func() int64 { return 0 },
		NanoClock:           func() int64 { return 0 },
		ErrorHandler:        errorHandler,
		ImageLifecycle:      ConductorImageLifecycle,
		MediaDriverDir:      mediaDriverDir,
	}
	for _, mutate := range mutateConfig {
		mutate(&config)
	}
	conductorInstance, err := conductor.NewConductorUnchecked(config, countersReader)
	require.NoError(tb, err)
	tb.Cleanup(func() { conductorInstance.UnmapLingeringLogBuffers() })
	return conductorInstance
}

func setupConductorImageLifecycleLogFile(tb testing.TB, filename string) (dir, logPath string) {
	return setupConductorImageLifecycleKindLogFile(tb, filename, false)
}

func setupConductorImageLifecycleKindLogFile(
	tb testing.TB,
	filename string,
	exclusive bool,
) (dir, logPath string) {
	tb.Helper()

	dir = tb.TempDir()
	logPath = filepath.Join(dir, filename)
	WriteTestKindLogFileForTest(tb, logPath, exclusive)
	return dir, logPath
}

func registerConductorImageLifecycleSubscriptionForTest(
	conductorInstance *conductor.Conductor,
	streamID int32,
) *Subscription {
	sub := NewSubscriptionForTest(
		conductorImageLifecycleTestRegistrationID,
		streamID,
		"aeron:ipc",
	)
	conductor.RegisterSubscriptionForTest(
		conductorInstance,
		conductorImageLifecycleTestRegistrationID,
		sub,
	)
	return sub
}

func subscriberPositionCountersReaderForConductorTest(tb testing.TB) *cnc.CountersReader {
	tb.Helper()
	return NewTestCountersReaderWithSubscriberPositionForTest(
		tb,
		TestSubscriberPositionAtJoin,
		conductorImageLifecycleTestRegistrationID,
	)
}

func availableImageEventForConductorTest(logPath string) conductor.AvailableImage {
	event := conductorImageLifecycleGoldenAvailableImage
	event.LogFileName = logPath
	return event
}

func availableImageBytesWithRawLogPathForConductorTest(
	event conductor.AvailableImage,
	logFileName string,
) []byte {
	bodyLength := conductor.AvailableImageBodyLength(logFileName, event.SourceIdentity)
	payload := make([]byte, bodyLength)
	buffer := atomicbuffer.Wrap(payload)

	buffer.PutInt64(conductor.AvailableImageCorrelationIDOffset, event.CorrelationID)
	buffer.PutInt32(conductor.AvailableImageSessionIDOffset, event.SessionID)
	buffer.PutInt32(conductor.AvailableImageStreamIDOffset, event.StreamID)
	buffer.PutInt64(
		conductor.AvailableImageSubscriptionRegistrationIDOffset,
		event.SubscriptionRegistrationID,
	)
	buffer.PutInt32(
		conductor.AvailableImageSubscriberPositionIDOffset,
		event.SubscriberPositionID,
	)

	logFileNameFieldOffset := conductor.AvailableImageFixedLength
	buffer.PutInt32(logFileNameFieldOffset, int32(len(logFileName)))
	buffer.PutStringBytes(
		logFileNameFieldOffset+atomicbuffer.StringASCIIHeaderLength,
		logFileName,
	)

	logFileNameFieldEnd := logFileNameFieldOffset +
		atomicbuffer.StringASCIIEncodedLength(len(logFileName))
	sourceIdentityOffset := atomicbuffer.Align(logFileNameFieldEnd, atomicbuffer.SizeInt32)
	buffer.PutInt32(sourceIdentityOffset, int32(len(event.SourceIdentity)))
	buffer.PutStringBytes(
		sourceIdentityOffset+atomicbuffer.StringASCIIHeaderLength,
		event.SourceIdentity,
	)

	return payload
}

func transmitConductorImageLifecycleEvent(
	tb testing.TB,
	adapter *conductor.DriverEventsAdapter,
	msgTypeID int,
	payload []byte,
) {
	tb.Helper()

	copyReceiver := adapter.CopyReceiverForTest()
	buf := copyReceiver.BroadcastReceiver().Buffer()
	tx, err := broadcast.NewBroadcastTransmitter(buf)
	require.NoError(tb, err)
	require.NoError(tb, tx.Transmit(msgTypeID, payload))
}

func TestConductor_handleAvailableImage_createsImage(t *testing.T) {
	t.Parallel()

	dir, logPath := setupConductorImageLifecycleLogFile(t, "image.log")

	conductorInstance := newConductorWithImageLifecycleForTest(t, noopConductorErrorHandler, dir)
	conductorInstance.SetCountersReaderForTest(subscriberPositionCountersReaderForConductorTest(t))

	sub := registerConductorImageLifecycleSubscriptionForTest(
		conductorInstance,
		conductorImageLifecycleGoldenAvailableImage.StreamID,
	)
	var attachedImage *Image
	SubscriptionSetAvailableImageHandlerForTest(sub, func(image *Image) {
		attachedImage = image
	})

	transmitConductorImageLifecycleEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(availableImageEventForConductorTest(logPath)),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.Equal(t, 1, sub.ImageCount())
	require.True(t, sub.IsConnected())
	require.NotNil(t, attachedImage)
	require.Equal(t, conductorImageLifecycleTestCorrelationID, attachedImage.CorrelationID())
	require.Equal(
		t,
		conductorImageLifecycleTestSubscriberCounter,
		attachedImage.SubscriberPositionID(),
	)
	require.Equal(t, TestSubscriberPositionAtJoin, attachedImage.JoinPosition())
}

func TestConductor_handleAvailableImage_createsImage_withinMediaDriverDir(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "nested", "image.log")
	require.NoError(t, os.MkdirAll(filepath.Dir(logPath), 0o755))
	WriteTestLogFileForTest(t, logPath)

	conductorInstance := newConductorWithImageLifecycleForTest(t, noopConductorErrorHandler, dir)
	conductorInstance.SetCountersReaderForTest(subscriberPositionCountersReaderForConductorTest(t))

	sub := registerConductorImageLifecycleSubscriptionForTest(
		conductorInstance,
		conductorImageLifecycleGoldenAvailableImage.StreamID,
	)

	transmitConductorImageLifecycleEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(availableImageEventForConductorTest(logPath)),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.Equal(t, 1, sub.ImageCount())
}

func TestConductor_handleAvailableImage_unknownSubscriptionIgnored(t *testing.T) {
	t.Parallel()

	dir, logPath := setupConductorImageLifecycleLogFile(t, "image.log")

	conductorInstance := newConductorWithImageLifecycleForTest(t, noopConductorErrorHandler, dir)
	transmitConductorImageLifecycleEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(availableImageEventForConductorTest(logPath)),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func TestConductor_handleAvailableImage_duplicateCorrelationIgnored(t *testing.T) {
	t.Parallel()

	dir, logPath := setupConductorImageLifecycleLogFile(t, "image.log")

	conductorInstance := newConductorWithImageLifecycleForTest(t, noopConductorErrorHandler, dir)
	conductorInstance.SetCountersReaderForTest(subscriberPositionCountersReaderForConductorTest(t))
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)

	sub := registerConductorImageLifecycleSubscriptionForTest(
		conductorInstance,
		conductorImageLifecycleGoldenAvailableImage.StreamID,
	)
	var availableHandlerCalls int
	SubscriptionSetAvailableImageHandlerForTest(sub, func(*Image) {
		availableHandlerCalls++
	})

	event := availableImageEventForConductorTest(logPath)
	adapter := conductorInstance.EventsAdapterForTest()
	for range 2 {
		transmitConductorImageLifecycleEvent(
			t,
			adapter,
			conductor.OnAvailableImage,
			conductor.AvailableImageBytes(event),
		)
		require.Equal(t, 1, conductorInstance.DoWork())
	}

	require.Equal(t, 1, sub.ImageCount())
	require.Equal(t, 1, availableHandlerCalls)
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
}

func TestConductor_handleAvailableImage_newImageFailureReleasesLog(t *testing.T) {
	t.Parallel()

	dir, logPath := setupConductorImageLifecycleLogFile(t, "image.log")

	var gotErrors []error
	conductorInstance := newConductorWithImageLifecycleForTest(
		t,
		func(err error) { gotErrors = append(gotErrors, err) },
		dir,
	)
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)

	sub := registerConductorImageLifecycleSubscriptionForTest(conductorInstance, 999)

	transmitConductorImageLifecycleEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(availableImageEventForConductorTest(logPath)),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.Len(t, gotErrors, 1)
	require.EqualError(
		t,
		gotErrors[0],
		"aeron: subscription stream 999 does not match available image 7",
	)
	require.Zero(t, sub.ImageCount())
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func TestConductor_handleAvailableImage_failureOnLingeringLogKeepsLinger(t *testing.T) {
	t.Parallel()

	const (
		startNs  = int64(1_000)
		lingerNs = int64(time.Second)
	)

	dir, logPath := setupConductorImageLifecycleLogFile(t, "image.log")

	var gotErrors []error
	nanoNs := startNs
	conductorInstance := newConductorWithImageLifecycleForTest(
		t,
		func(err error) { gotErrors = append(gotErrors, err) },
		dir,
		func(config *conductor.Config) {
			config.NanoClock = func() int64 { return nanoNs }
			config.ResourceLingerNs = lingerNs
		},
	)
	conductorInstance.SetCountersReaderForTest(subscriberPositionCountersReaderForConductorTest(t))
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)

	registerConductorImageLifecycleSubscriptionForTest(
		conductorInstance,
		conductorImageLifecycleGoldenAvailableImage.StreamID,
	)
	adapter := conductorInstance.EventsAdapterForTest()

	// Setup: the first image leaves, so its log lingers.
	transmitConductorImageLifecycleEvent(
		t,
		adapter,
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(availableImageEventForConductorTest(logPath)),
	)
	require.Equal(t, 1, conductorInstance.DoWork())
	transmitConductorImageLifecycleEvent(
		t,
		adapter,
		conductor.OnUnavailableImage,
		conductor.UnavailableImageBytes(conductorImageLifecycleGoldenUnavailableImage),
	)
	require.Equal(t, 1, conductorInstance.DoWork())
	firstDeadline := startNs + lingerNs
	require.Equal(t, firstDeadline, conductor.LingerDeadlineNsForTest(registry, logPath))

	// Failed attach: an inactive subscriber counter fails a new image on the
	// same log.
	nanoNs = startNs + int64(time.Millisecond)
	event := availableImageEventForConductorTest(logPath)
	event.CorrelationID++
	event.SubscriberPositionID = 99
	transmitConductorImageLifecycleEvent(
		t,
		adapter,
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(event),
	)
	require.Equal(t, 1, conductorInstance.DoWork())
	require.Len(t, gotErrors, 1)

	// Each mapping lingers to its own deadline.
	require.Equal(t, 2, conductor.LogBuffersRegistryCountScheduledLingersForTest(registry))

	nanoNs = firstDeadline - 1
	require.Zero(t, conductorInstance.DrainLingeringLogBuffers())

	nanoNs = firstDeadline
	require.Equal(t, 1, conductorInstance.DrainLingeringLogBuffers())
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))

	nanoNs = startNs + int64(time.Millisecond) + lingerNs
	require.Equal(t, 1, conductorInstance.DrainLingeringLogBuffers())
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func TestConductor_handleAvailableImage_acquireFailureReportsError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	missingPath := filepath.Join(dir, "missing.log")

	var gotErrors []error
	conductorInstance := newConductorWithImageLifecycleForTest(
		t,
		func(err error) { gotErrors = append(gotErrors, err) },
		dir,
	)
	conductorInstance.SetCountersReaderForTest(subscriberPositionCountersReaderForConductorTest(t))

	sub := registerConductorImageLifecycleSubscriptionForTest(
		conductorInstance,
		conductorImageLifecycleGoldenAvailableImage.StreamID,
	)
	transmitConductorImageLifecycleEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(availableImageEventForConductorTest(missingPath)),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.Len(t, gotErrors, 1)
	require.ErrorIs(t, gotErrors[0], conductor.ErrLogBufferMapFailed)
	require.Zero(t, sub.ImageCount())
}

func TestConductor_handleAvailableImage_inactiveSubscriberCounter(t *testing.T) {
	t.Parallel()

	dir, logPath := setupConductorImageLifecycleLogFile(t, "image.log")

	var gotErrors []error
	conductorInstance := newConductorWithImageLifecycleForTest(
		t,
		func(err error) { gotErrors = append(gotErrors, err) },
		dir,
	)
	conductorInstance.SetCountersReaderForTest(
		NewTestCountersReaderWithSubscriberPositionForTest(t, TestSubscriberPositionAtJoin),
	)

	sub := registerConductorImageLifecycleSubscriptionForTest(
		conductorInstance,
		conductorImageLifecycleGoldenAvailableImage.StreamID,
	)

	event := availableImageEventForConductorTest(logPath)
	event.SubscriberPositionID = 99
	transmitConductorImageLifecycleEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(event),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.Len(t, gotErrors, 1)
	require.EqualError(
		t,
		gotErrors[0],
		"aeron: subscriber position counter 99 is not active for registration 5005",
	)
	require.Zero(t, sub.ImageCount())
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func TestConductor_handleAvailableImage_rejectsNonASCIIPath(t *testing.T) {
	t.Parallel()

	dir, logPath := setupConductorImageLifecycleLogFile(t, "image.log")

	var gotErrors []error
	conductorInstance := newConductorWithImageLifecycleForTest(
		t,
		func(err error) { gotErrors = append(gotErrors, err) },
		dir,
	)
	conductorInstance.SetCountersReaderForTest(subscriberPositionCountersReaderForConductorTest(t))

	sub := registerConductorImageLifecycleSubscriptionForTest(
		conductorInstance,
		conductorImageLifecycleGoldenAvailableImage.StreamID,
	)

	event := availableImageEventForConductorTest(logPath)
	nonASCIIPath := filepath.Join(dir, "\xff.log")
	transmitConductorImageLifecycleEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnAvailableImage,
		availableImageBytesWithRawLogPathForConductorTest(event, nonASCIIPath),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.Len(t, gotErrors, 1)
	require.ErrorIs(t, gotErrors[0], conductor.ErrLogFileNameNotASCII)
	require.Zero(t, sub.ImageCount())
}

func TestConductor_handleAvailableImage_passesSubscriberErrorHandler(t *testing.T) {
	t.Parallel()

	dir, logPath := setupConductorImageLifecycleLogFile(t, "image.log")

	var handlerCalls int
	subscriberHandler := func(error) { handlerCalls++ }
	conductorInstance := newConductorWithImageLifecycleForTest(
		t,
		noopConductorErrorHandler,
		dir,
		func(config *conductor.Config) {
			config.SubscriberErrorHandler = subscriberHandler
		},
	)
	conductorInstance.SetCountersReaderForTest(subscriberPositionCountersReaderForConductorTest(t))

	sub := registerConductorImageLifecycleSubscriptionForTest(
		conductorInstance,
		conductorImageLifecycleGoldenAvailableImage.StreamID,
	)

	transmitConductorImageLifecycleEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(availableImageEventForConductorTest(logPath)),
	)
	require.Equal(t, 1, conductorInstance.DoWork())
	require.Equal(t, 1, sub.ImageCount())

	image, ok := SubscriptionImageAtIndexForTest(sub, 0)
	require.True(t, ok)
	require.NotNil(t, ImageSubscriberErrorHandlerForTest(image))
	ImageInvokeSubscriberErrorHandlerForTest(image, errors.New("subscriber-side fault"))
	require.Equal(t, 1, handlerCalls)
}

func TestConductor_handleAvailableImage_rejectsPathOutsideMediaDriverDir(t *testing.T) {
	t.Parallel()

	_, logPath := setupConductorImageLifecycleLogFile(t, "image.log")

	var gotErrors []error
	conductorInstance := newConductorWithImageLifecycleForTest(
		t,
		func(err error) { gotErrors = append(gotErrors, err) },
		filepath.Join(t.TempDir(), "other"),
	)

	registerConductorImageLifecycleSubscriptionForTest(
		conductorInstance,
		conductorImageLifecycleGoldenAvailableImage.StreamID,
	)

	transmitConductorImageLifecycleEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(availableImageEventForConductorTest(logPath)),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.Len(t, gotErrors, 1)
	require.ErrorIs(t, gotErrors[0], conductor.ErrLogFileOutsideMediaDriverDir)
}

func TestConductor_handleUnavailableImage_closesImage(t *testing.T) {
	t.Parallel()

	dir, logPath := setupConductorImageLifecycleLogFile(t, "image.log")

	conductorInstance := newConductorWithImageLifecycleForTest(t, noopConductorErrorHandler, dir)
	conductorInstance.SetCountersReaderForTest(subscriberPositionCountersReaderForConductorTest(t))

	sub := registerConductorImageLifecycleSubscriptionForTest(
		conductorInstance,
		conductorImageLifecycleGoldenAvailableImage.StreamID,
	)
	var detachedImage *Image
	SubscriptionSetUnavailableImageHandlerForTest(sub, func(image *Image) {
		detachedImage = image
	})

	adapter := conductorInstance.EventsAdapterForTest()
	transmitConductorImageLifecycleEvent(
		t,
		adapter,
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(availableImageEventForConductorTest(logPath)),
	)
	require.Equal(t, 1, conductorInstance.DoWork())
	require.Equal(t, 1, sub.ImageCount())

	transmitConductorImageLifecycleEvent(
		t,
		adapter,
		conductor.OnUnavailableImage,
		conductor.UnavailableImageBytes(conductorImageLifecycleGoldenUnavailableImage),
	)
	require.Equal(t, 1, conductorInstance.DoWork())

	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	require.Zero(t, sub.ImageCount())
	require.False(t, sub.IsConnected())
	require.NotNil(t, detachedImage)
	require.True(t, detachedImage.IsClosed())
	require.Zero(t, conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
	require.Zero(t, detachedImage.Poll(func(
		atomicbuffer.AtomicBuffer,
		int,
		int,
		*Header,
	) {
	}, 1))
}

func TestConductor_handleUnavailableImage_refcountRetainedWhilePublicationOpen(t *testing.T) {
	t.Parallel()

	dir, logPath := setupConductorImageLifecycleLogFile(t, "shared.log")

	conductorInstance := newConductorWithImageLifecycleForTest(t, noopConductorErrorHandler, dir)
	conductorInstance.SetCountersReaderForTest(subscriberPositionCountersReaderForConductorTest(t))
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)

	registerConductorImageLifecycleSubscriptionForTest(
		conductorInstance,
		conductorImageLifecycleGoldenAvailableImage.StreamID,
	)

	_, err := registry.Acquire(logPath)
	require.NoError(t, err)
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))

	adapter := conductorInstance.EventsAdapterForTest()
	transmitConductorImageLifecycleEvent(
		t,
		adapter,
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(availableImageEventForConductorTest(logPath)),
	)
	require.Equal(t, 1, conductorInstance.DoWork())
	require.Equal(t, int32(2), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))

	transmitConductorImageLifecycleEvent(
		t,
		adapter,
		conductor.OnUnavailableImage,
		conductor.UnavailableImageBytes(conductorImageLifecycleGoldenUnavailableImage),
	)
	require.Equal(t, 1, conductorInstance.DoWork())

	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func TestConductor_handleUnavailableImage_unknownCorrelationIgnored(t *testing.T) {
	t.Parallel()

	dir, logPath := setupConductorImageLifecycleLogFile(t, "image.log")

	conductorInstance := newConductorWithImageLifecycleForTest(t, noopConductorErrorHandler, dir)
	conductorInstance.SetCountersReaderForTest(subscriberPositionCountersReaderForConductorTest(t))
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)

	sub := registerConductorImageLifecycleSubscriptionForTest(
		conductorInstance,
		conductorImageLifecycleGoldenAvailableImage.StreamID,
	)
	var unavailableHandlerCalls int
	SubscriptionSetUnavailableImageHandlerForTest(sub, func(*Image) {
		unavailableHandlerCalls++
	})

	adapter := conductorInstance.EventsAdapterForTest()
	transmitConductorImageLifecycleEvent(
		t,
		adapter,
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(availableImageEventForConductorTest(logPath)),
	)
	require.Equal(t, 1, conductorInstance.DoWork())
	require.Equal(t, 1, sub.ImageCount())
	attachedImage, ok := SubscriptionImageAtIndexForTest(sub, 0)
	require.True(t, ok)
	require.False(t, attachedImage.IsClosed())
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))

	unknown := conductorImageLifecycleGoldenUnavailableImage
	unknown.CorrelationID = 99999
	transmitConductorImageLifecycleEvent(
		t,
		adapter,
		conductor.OnUnavailableImage,
		conductor.UnavailableImageBytes(unknown),
	)
	require.Equal(t, 1, conductorInstance.DoWork())
	require.Equal(t, 1, sub.ImageCount())
	require.False(t, attachedImage.IsClosed())
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.Zero(t, unavailableHandlerCalls)
}

func TestConductor_handleUnavailableImage_unknownSubscriptionIgnored(t *testing.T) {
	t.Parallel()

	conductorInstance := newConductorWithImageLifecycleForTest(t, noopConductorErrorHandler, "/")
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	transmitConductorImageLifecycleEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnUnavailableImage,
		conductor.UnavailableImageBytes(conductorImageLifecycleGoldenUnavailableImage),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.False(t, conductor.LogBuffersRegistryContainsForTest(
		registry,
		conductorImageLifecycleGoldenAvailableImage.LogFileName,
	))
}
