package conductor_test

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron/conductor"
)

const resourceMapTestEpochMs int64 = 1_700_000_000_000

type stubPublication struct {
	registrationID         int64
	originalRegistrationID int64
}

func (publication stubPublication) RegistrationID() int64 {
	return publication.registrationID
}

func (publication stubPublication) OriginalRegistrationID() int64 {
	return publication.originalRegistrationID
}

type stubPublicationPtr struct {
	registrationID         int64
	originalRegistrationID int64
}

func (publication *stubPublicationPtr) RegistrationID() int64 {
	return publication.registrationID
}

func (publication *stubPublicationPtr) OriginalRegistrationID() int64 {
	return publication.originalRegistrationID
}

func newBareConductorForResourceMapTest(tb testing.TB) *conductor.Conductor {
	tb.Helper()
	return newConductorForTest(tb, conductorTestConfig(tb, func(error) {}, fixedClocks{}))
}

func newConductorWithZeroNanoClockAndMediaDir(
	tb testing.TB,
	mediaDriverDir string,
) *conductor.Conductor {
	tb.Helper()

	config := conductorTestConfig(tb, func(error) {}, fixedClocks{epochMs: resourceMapTestEpochMs})
	config.MediaDriverDir = mediaDriverDir
	return newConductorForTest(tb, config)
}

func scheduleLingerForConductorLog(
	t *testing.T,
	conductorInstance *conductor.Conductor,
	logPath string,
	lingerNs int64,
) {
	t.Helper()

	_, err := conductorInstance.AcquireLogBuffersForTest(logPath)
	require.NoError(t, err)
	conductorInstance.ReleaseLogBuffersForTest(logPath, lingerNs)
}

type imageHandlerLingerFixtures struct {
	conductor     *conductor.Conductor
	nanoNs        *int64
	lifecycle     *recordingImageLifecycle
	registry      *conductor.LogBuffersRegistry
	lingerLogPath string
	startNs       int64
	lingerNs      int64
}

func acquireLogOnConductorRegistry(
	t *testing.T,
	conductorInstance *conductor.Conductor,
	logFileName string,
) (*conductor.LogBuffersRegistry, string) {
	t.Helper()

	dir := t.TempDir()
	logPath := filepath.Join(dir, logFileName)
	writeMinimalLogFileForRegistryTest(t, logPath)
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	_, err := registry.Acquire(logPath)
	require.NoError(t, err)
	return registry, logPath
}

func setupImageHandlerLingerFixtures(
	t *testing.T,
	startNs int64,
	logFileName string,
) imageHandlerLingerFixtures {
	t.Helper()

	const lingerNs = int64(time.Second)

	dir := t.TempDir()
	lingerLogPath := filepath.Join(dir, logFileName)
	writeMinimalLogFileForRegistryTest(t, lingerLogPath)

	lifecycle := &recordingImageLifecycle{
		hasImage:           true,
		removedLogFileName: goldenAvailableImageEvent.LogFileName,
	}
	conductorInstance, nanoNs := newConductorWithRecordingLifecycleMutableNano(
		t,
		lifecycle,
		func(error) {},
		resourceMapTestEpochMs,
		"/",
	)
	conductorInstance.SeedServiceTimestamps(startNs)
	*nanoNs = startNs
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)

	scheduleLingerForConductorLog(t, conductorInstance, lingerLogPath, lingerNs)

	registerGoldenImageSubscription(conductorInstance)
	seedGoldenAvailableLogBuffers(t, conductorInstance)

	return imageHandlerLingerFixtures{
		conductor:     conductorInstance,
		nanoNs:        nanoNs,
		lifecycle:     lifecycle,
		registry:      registry,
		lingerLogPath: lingerLogPath,
		startNs:       startNs,
		lingerNs:      lingerNs,
	}
}

// startDoWorkUntilHandlerEntered starts DoWork on a goroutine and returns the
// result channel after the suspended image handler has entered.
//
// Handler entry is the wait condition. The helper does not Sleep for readiness.
func startDoWorkUntilHandlerEntered(
	t *testing.T,
	conductorInstance *conductor.Conductor,
	handlerEntered <-chan struct{},
) chan int {
	t.Helper()

	doWorkDone := make(chan int, 1)
	go func() {
		doWorkDone <- conductorInstance.DoWork()
	}()

	select {
	case <-handlerEntered:
	case workCount := <-doWorkDone:
		t.Fatalf("DoWork finished before suspended handler entered: workCount=%d", workCount)
	}
	return doWorkDone
}

func TestConductor_publicationMap_overwriteDuplicateRegistrationID(t *testing.T) {
	t.Parallel()

	conductorInstance := newBareConductorForResourceMapTest(t)

	conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})
	conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 99})

	got, ok := conductorInstance.PublicationByRegID(42)
	require.True(t, ok)
	require.Equal(t, int64(99), got.RegistrationID())
}

func TestConductor_publicationMap_insertRemove(t *testing.T) {
	t.Parallel()

	conductorInstance := newBareConductorForResourceMapTest(t)

	_, ok := conductorInstance.PublicationByRegID(99)
	require.False(t, ok)

	publication := stubPublication{registrationID: 42}
	conductorInstance.InsertPublicationForTest(42, publication)
	require.Equal(t, 1, conductorInstance.PublicationCountForTest())

	got, ok := conductorInstance.PublicationByRegID(42)
	require.True(t, ok)
	require.Equal(t, int64(42), got.RegistrationID())

	conductorInstance.RemovePublicationForTest(42)
	require.Equal(t, 0, conductorInstance.PublicationCountForTest())

	_, ok = conductorInstance.PublicationByRegID(42)
	require.False(t, ok)
}

func TestConductor_publicationMap_nilEntryMiss(t *testing.T) {
	t.Parallel()

	conductorInstance := newBareConductorForResourceMapTest(t)
	conductorInstance.InsertNilPublicationForTest(42)

	require.Equal(t, 0, conductorInstance.PublicationCountForTest())

	got, ok := conductorInstance.PublicationByRegID(42)
	require.False(t, ok)
	require.Nil(t, got)
}

func TestConductor_publicationMap_typedNilEntryMiss(t *testing.T) {
	t.Parallel()

	conductorInstance := newBareConductorForResourceMapTest(t)

	var nilPublication conductor.PublicationHandle = (*stubPublicationPtr)(nil)
	conductorInstance.InsertPublicationForTest(42, nilPublication)

	require.Equal(t, 0, conductorInstance.PublicationCountForTest())

	got, ok := conductorInstance.PublicationByRegID(42)
	require.False(t, ok)
	require.Nil(t, got)
}

func TestConductor_PublicationByRegID_concurrentWithDoWork(t *testing.T) {
	t.Parallel()

	conductorInstance, _ := newConductorForMutableNanoTest(
		t,
		func(error) {},
		resourceMapTestEpochMs,
	)
	conductorInstance.SeedServiceTimestamps(1)
	conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})

	var wg sync.WaitGroup
	wg.Go(func() {
		for range 500 {
			got, ok := conductorInstance.PublicationByRegID(42)
			require.True(t, ok)
			require.Equal(t, int64(42), got.RegistrationID())
		}
	})

	for range 500 {
		conductorInstance.DoWork()
	}

	wg.Wait()
}

func runConcurrentPubSubLookup(
	t *testing.T,
	wg *sync.WaitGroup,
	conductorInstance *conductor.Conductor,
	iterations int,
) {
	t.Helper()

	wg.Go(func() {
		for range iterations {
			publication, ok := conductorInstance.PublicationByRegID(42)
			require.True(t, ok)
			require.Equal(t, int64(42), publication.RegistrationID())

			subscription, ok := conductorInstance.SubscriptionByRegID(77)
			require.True(t, ok)
			require.Equal(t, int64(77), subscription.RegistrationID())
			require.Equal(t, int32(9), subscription.StreamID())
		}
	})
}

func TestConductor_DoWork_idleNoLockConcurrentLookup(t *testing.T) {
	t.Parallel()

	conductorInstance, _ := newConductorForMutableNanoTest(
		t,
		func(error) {},
		resourceMapTestEpochMs,
	)
	conductorInstance.SeedServiceTimestamps(1)
	conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})
	conductorInstance.InsertSubscriptionForTest(
		77,
		stubSubscription{registrationID: 77, streamID: 9},
	)
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	require.False(t, registry.HasScheduledLinger())

	var wg sync.WaitGroup
	runConcurrentPubSubLookup(t, &wg, conductorInstance, 1000)

	for range 1000 {
		require.Zero(t, conductorInstance.DoWork())
		require.False(t, registry.HasScheduledLinger())
	}

	wg.Wait()
}

func TestConductor_DoWork_lingerScheduledConcurrentLookup_race(t *testing.T) {
	t.Parallel()

	const startNs int64 = 12_000_000_000
	const lingerNs int64 = int64(time.Second)

	conductorInstance, nanoNs := newConductorForMutableNanoTest(
		t,
		func(error) {},
		resourceMapTestEpochMs,
	)
	conductorInstance.SeedServiceTimestamps(startNs)
	conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})
	conductorInstance.InsertSubscriptionForTest(
		77,
		stubSubscription{registrationID: 77, streamID: 9},
	)

	registry, logPath := acquireLogOnConductorRegistry(t, conductorInstance, "linger-concurrent.log")
	scheduleLingerForRegistryLog(t, registry, logPath, startNs, lingerNs)
	require.True(t, registry.HasScheduledLinger())
	*nanoNs = startNs + lingerNs - 1

	var wg sync.WaitGroup
	runConcurrentPubSubLookup(t, &wg, conductorInstance, 1000)

	for range 1000 {
		conductorInstance.DoWork()
	}

	wg.Wait()
}

func TestConductor_DoWork_lingerScheduledConcurrentLookup_blocksDuringSweep(t *testing.T) {
	t.Parallel()

	sweepEntered := make(chan struct{})
	releaseSweep := make(chan struct{})
	var sweepEnteredOnce sync.Once

	const startNs int64 = 12_500_000_000
	const lingerNs int64 = int64(time.Second)

	conductorInstance, nanoNs := newConductorForMutableNanoTest(
		t,
		func(error) {},
		resourceMapTestEpochMs,
	)
	conductorInstance.SeedServiceTimestamps(startNs)
	conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})
	conductorInstance.InsertSubscriptionForTest(
		77,
		stubSubscription{registrationID: 77, streamID: 9},
	)

	registry, logPath := acquireLogOnConductorRegistry(t, conductorInstance, "linger-sweep-block.log")
	scheduleLingerForRegistryLog(t, registry, logPath, startNs, lingerNs)
	require.True(t, registry.HasScheduledLinger())
	*nanoNs = startNs + lingerNs - 1

	// Release the sweep on failure. The conductor cleanup needs the write lock.
	releaseBlocker := sync.OnceFunc(func() { close(releaseSweep) })
	t.Cleanup(releaseBlocker)

	conductorInstance.SetLingerSweepBlockerForTest(t, func() {
		sweepEnteredOnce.Do(func() { close(sweepEntered) })
		<-releaseSweep
	})

	doWorkDone := make(chan int, 1)
	go func() {
		doWorkDone <- conductorInstance.DoWork()
	}()

	<-sweepEntered
	// Write lock is held under the linger-sweep blocker. Assert via non-blocking
	// RLock. Do not use wall-clock waits.
	require.False(t, conductorInstance.TryResourceRLockForTest(),
		"resource write lock must be held during linger sweep")

	lookupDone := make(chan bool, 1)
	go func() {
		_, ok := conductorInstance.PublicationByRegID(42)
		lookupDone <- ok
	}()

	// Neither channel may be ready while the sweep holds the write lock. The
	// select is non-blocking and adds no wall wait. It fails only if lookup
	// stopped taking resourceMu or DoWork stopped waiting on releaseSweep.
	select {
	case ok := <-lookupDone:
		t.Fatalf("lookup completed during linger sweep write lock: ok=%v", ok)
	case workCount := <-doWorkDone:
		t.Fatalf("DoWork finished before sweep released: workCount=%d", workCount)
	default:
	}

	releaseBlocker()
	require.True(t, <-lookupDone)
	<-doWorkDone
}

func TestConductor_DoWork_concurrentDrainLingeringLogBuffers_race(t *testing.T) {
	t.Parallel()

	const startNs int64 = 13_000_000_000
	const lingerNs int64 = int64(time.Second)

	conductorInstance, nanoNs := newConductorForMutableNanoTest(
		t,
		func(error) {},
		resourceMapTestEpochMs,
	)
	conductorInstance.SeedServiceTimestamps(startNs)
	registry, logPath := acquireLogOnConductorRegistry(t, conductorInstance, "drain-race.log")
	scheduleLingerForRegistryLog(t, registry, logPath, startNs, lingerNs)
	require.True(t, registry.HasScheduledLinger())
	*nanoNs = startNs + lingerNs/2

	var wg sync.WaitGroup
	wg.Go(func() {
		for range 500 {
			conductorInstance.DoWork()
		}
	})
	wg.Go(func() {
		for range 500 {
			conductorInstance.DrainLingeringLogBuffers()
		}
	})
	wg.Wait()

	assertRegistryLingerScheduledMatchesScan(t, registry)
	require.True(t, registry.HasScheduledLinger())
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
	require.Less(t, *nanoNs, startNs+lingerNs)
}

type concurrentImageHandlerLookupCase struct {
	available bool
	setup     func(*conductor.Conductor, *recordingImageLifecycle)
	trigger   func(*testing.T, *conductor.Conductor)
	setBlock  func(*recordingImageLifecycle, chan struct{}, chan struct{})
}

func runConcurrentLookupDuringImageHandlerTest(
	t *testing.T,
	testCase concurrentImageHandlerLookupCase,
	lookup func(*conductor.Conductor) error,
) {
	t.Helper()

	handlerEntered := make(chan struct{})
	releaseHandler := make(chan struct{})

	lifecycle := &recordingImageLifecycle{addImageOK: true}
	if !testCase.available {
		lifecycle = &recordingImageLifecycle{}
	}
	conductorInstance := newConductorWithRecordingLifecycle(t, lifecycle, func(error) {})
	testCase.setup(conductorInstance, lifecycle)
	testCase.setBlock(lifecycle, handlerEntered, releaseHandler)

	testCase.trigger(t, conductorInstance)

	doWorkDone := startDoWorkUntilHandlerEntered(t, conductorInstance, handlerEntered)

	// Suspended handler has dropped resourceMu. Lookup must finish while DoWork
	// still waits on releaseHandler.
	lookupDone := make(chan error, 1)
	go func() {
		lookupDone <- lookup(conductorInstance)
	}()

	select {
	case err := <-lookupDone:
		require.NoError(t, err)
	case workCount := <-doWorkDone:
		t.Fatalf("DoWork finished before lookup during suspended handler: workCount=%d", workCount)
	}

	select {
	case workCount := <-doWorkDone:
		t.Fatalf("DoWork finished before handler released: workCount=%d", workCount)
	default:
	}
	close(releaseHandler)
	require.Equal(t, 1, <-doWorkDone)
}

func TestConductor_PublicationByRegID_concurrentDuringImageHandler(t *testing.T) {
	t.Parallel()

	runConcurrentLookupDuringImageHandlerTest(
		t,
		concurrentImageHandlerLookupCase{
			available: true,
			setup: func(conductorInstance *conductor.Conductor, _ *recordingImageLifecycle) {
				conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})
				registerGoldenImageSubscription(conductorInstance)
				seedGoldenAvailableLogBuffers(t, conductorInstance)
			},
			trigger: func(t *testing.T, conductorInstance *conductor.Conductor) {
				t.Helper()
				transmitConductorEvent(
					t,
					conductorInstance.EventsAdapterForTest(),
					conductor.OnAvailableImage,
					conductor.AvailableImageBytes(goldenAvailableImageEvent),
				)
			},
			setBlock: func(
				lifecycle *recordingImageLifecycle,
				handlerEntered chan struct{},
				releaseHandler chan struct{},
			) {
				lifecycle.availableImageHandler = func(conductor.ImageHandle) {
					close(handlerEntered)
					<-releaseHandler
				}
			},
		},
		lookupPublication(42),
	)
}

func TestConductor_PublicationByRegID_concurrentDuringUnavailableImageHandler(t *testing.T) {
	t.Parallel()

	runConcurrentLookupDuringImageHandlerTest(
		t,
		concurrentImageHandlerLookupCase{
			available: false,
			setup: func(conductorInstance *conductor.Conductor, _ *recordingImageLifecycle) {
				conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})
				registerGoldenImageSubscription(conductorInstance)
			},
			trigger: func(t *testing.T, conductorInstance *conductor.Conductor) {
				t.Helper()
				triggerGoldenUnavailableImage(t, conductorInstance)
			},
			setBlock: func(
				lifecycle *recordingImageLifecycle,
				handlerEntered chan struct{},
				releaseHandler chan struct{},
			) {
				lifecycle.unavailableImageHandler = func(conductor.ImageHandle) {
					close(handlerEntered)
					<-releaseHandler
				}
			},
		},
		lookupPublication(42),
	)
}

func TestConductor_SubscriptionByRegID_concurrentDuringImageHandler(t *testing.T) {
	t.Parallel()

	runConcurrentLookupDuringImageHandlerTest(
		t,
		concurrentImageHandlerLookupCase{
			available: true,
			setup: func(conductorInstance *conductor.Conductor, _ *recordingImageLifecycle) {
				registerGoldenImageSubscription(conductorInstance)
				seedGoldenAvailableLogBuffers(t, conductorInstance)
			},
			trigger: func(t *testing.T, conductorInstance *conductor.Conductor) {
				t.Helper()
				transmitConductorEvent(
					t,
					conductorInstance.EventsAdapterForTest(),
					conductor.OnAvailableImage,
					conductor.AvailableImageBytes(goldenAvailableImageEvent),
				)
			},
			setBlock: func(
				lifecycle *recordingImageLifecycle,
				handlerEntered chan struct{},
				releaseHandler chan struct{},
			) {
				lifecycle.availableImageHandler = func(conductor.ImageHandle) {
					close(handlerEntered)
					<-releaseHandler
				}
			},
		},
		lookupGoldenSubscription(true),
	)
}

func TestConductor_SubscriptionByRegID_concurrentDuringUnavailableImageHandler(t *testing.T) {
	t.Parallel()

	runConcurrentLookupDuringImageHandlerTest(
		t,
		concurrentImageHandlerLookupCase{
			available: false,
			setup: func(conductorInstance *conductor.Conductor, _ *recordingImageLifecycle) {
				registerGoldenImageSubscription(conductorInstance)
			},
			trigger: func(t *testing.T, conductorInstance *conductor.Conductor) {
				t.Helper()
				triggerGoldenUnavailableImage(t, conductorInstance)
			},
			setBlock: func(
				lifecycle *recordingImageLifecycle,
				handlerEntered chan struct{},
				releaseHandler chan struct{},
			) {
				lifecycle.unavailableImageHandler = func(conductor.ImageHandle) {
					close(handlerEntered)
					<-releaseHandler
				}
			},
		},
		lookupGoldenSubscription(false),
	)
}

func runForeignWriteLockDuringSuspendedImageHandlerTest(t *testing.T, available bool) {
	t.Helper()

	handlerEntered := make(chan struct{})
	releaseHandler := make(chan struct{})

	lifecycle := &recordingImageLifecycle{addImageOK: true}
	if !available {
		lifecycle = &recordingImageLifecycle{}
	}
	conductorInstance := newConductorWithRecordingLifecycle(t, lifecycle, func(error) {})
	registerGoldenImageSubscription(conductorInstance)
	if available {
		seedGoldenAvailableLogBuffers(t, conductorInstance)
	}

	blockHandler := func(conductor.ImageHandle) {
		close(handlerEntered)
		<-releaseHandler
	}
	if available {
		lifecycle.availableImageHandler = blockHandler
		transmitConductorEvent(
			t,
			conductorInstance.EventsAdapterForTest(),
			conductor.OnAvailableImage,
			conductor.AvailableImageBytes(goldenAvailableImageEvent),
		)
	} else {
		lifecycle.unavailableImageHandler = blockHandler
		triggerGoldenUnavailableImage(t, conductorInstance)
	}

	doWorkDone := startDoWorkUntilHandlerEntered(t, conductorInstance, handlerEntered)

	// Suspended handler has dropped resourceMu. Foreign write lock must finish
	// before releaseHandler closes.
	insertDone := make(chan error, 1)
	go func() {
		insertDone <- insertAndLookupPublication(77)(conductorInstance)
	}()

	select {
	case err := <-insertDone:
		require.NoError(t, err)
	case workCount := <-doWorkDone:
		t.Fatalf(
			"DoWork finished before foreign write lock during suspended handler: workCount=%d",
			workCount,
		)
	}

	select {
	case workCount := <-doWorkDone:
		t.Fatalf("DoWork finished before handler released: workCount=%d", workCount)
	default:
	}
	close(releaseHandler)
	require.GreaterOrEqual(t, <-doWorkDone, 1)

	got, ok := conductorInstance.PublicationByRegID(77)
	require.True(t, ok)
	require.Equal(t, int64(77), got.RegistrationID())
}

func TestConductor_foreignWriteLockDuringSuspendedImageHandler(t *testing.T) {
	t.Parallel()
	runForeignWriteLockDuringSuspendedImageHandlerTest(t, true)
}

func TestConductor_foreignWriteLockDuringSuspendedUnavailableImageHandler(t *testing.T) {
	t.Parallel()
	runForeignWriteLockDuringSuspendedImageHandlerTest(t, false)
}

func TestConductor_foreignDrainLingeringDuringSuspendedImageHandler(t *testing.T) {
	t.Parallel()

	const startNs int64 = 14_000_000_000

	fixtures := setupImageHandlerLingerFixtures(t, startNs, "foreign-drain-handler.log")
	conductorInstance := fixtures.conductor
	nanoNs := fixtures.nanoNs
	lifecycle := fixtures.lifecycle
	registry := fixtures.registry
	lingerLogPath := fixtures.lingerLogPath
	lingerNs := fixtures.lingerNs

	handlerEntered := make(chan struct{})
	releaseHandler := make(chan struct{})

	lifecycle.unavailableImageHandler = func(conductor.ImageHandle) {
		close(handlerEntered)
		<-releaseHandler
	}

	*nanoNs = startNs + lingerNs - 1
	triggerGoldenUnavailableImage(t, conductorInstance)

	doWorkDone := startDoWorkUntilHandlerEntered(t, conductorInstance, handlerEntered)

	// Suspended handler has dropped resourceMu. Foreign drain must finish before
	// releaseHandler closes. The linger deadline is still in the future, so the
	// drain work count stays zero.
	drainDone := make(chan int, 1)
	go func() {
		drainDone <- conductorInstance.DrainLingeringLogBuffers()
	}()

	select {
	case drainWork := <-drainDone:
		require.Zero(t, drainWork)
	case workCount := <-doWorkDone:
		t.Fatalf("DoWork finished before foreign drain during suspended handler: workCount=%d", workCount)
	}
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, lingerLogPath))

	select {
	case workCount := <-doWorkDone:
		t.Fatalf("DoWork finished before handler released: workCount=%d", workCount)
	default:
	}
	close(releaseHandler)
	require.GreaterOrEqual(t, <-doWorkDone, 1)
}

func runImageHandlerActionTest(
	t *testing.T,
	available bool,
	setup func(*conductor.Conductor, *recordingImageLifecycle),
	trigger func(*testing.T, *conductor.Conductor),
	action func(*conductor.Conductor) error,
) *conductor.Conductor {
	t.Helper()

	handlerDone := make(chan error, 1)
	lifecycle := &recordingImageLifecycle{addImageOK: true}
	if !available {
		lifecycle = &recordingImageLifecycle{}
	}
	conductorInstance := newConductorWithRecordingLifecycle(t, lifecycle, func(error) {})
	setup(conductorInstance, lifecycle)

	if available {
		lifecycle.availableImageHandler = func(conductor.ImageHandle) {
			handlerDone <- action(conductorInstance)
		}
	} else {
		lifecycle.unavailableImageHandler = func(conductor.ImageHandle) {
			handlerDone <- action(conductorInstance)
		}
	}

	trigger(t, conductorInstance)

	require.Equal(t, 1, conductorInstance.DoWork())
	select {
	case err := <-handlerDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("image handler action did not complete")
	}
	return conductorInstance
}

func insertAndLookupPublication(registrationID int64) func(*conductor.Conductor) error {
	return func(conductorInstance *conductor.Conductor) error {
		conductorInstance.InsertPublicationForTest(
			registrationID,
			stubPublication{registrationID: registrationID},
		)
		got, ok := conductorInstance.PublicationByRegID(registrationID)
		if !ok {
			return fmt.Errorf("publication %d not found", registrationID)
		}
		if got.RegistrationID() != registrationID {
			return fmt.Errorf("got registration ID %d, want %d", got.RegistrationID(), registrationID)
		}
		return nil
	}
}

func lookupPublication(registrationID int64) func(*conductor.Conductor) error {
	return func(conductorInstance *conductor.Conductor) error {
		got, ok := conductorInstance.PublicationByRegID(registrationID)
		if !ok {
			return fmt.Errorf("publication %d not found", registrationID)
		}
		if got.RegistrationID() != registrationID {
			return fmt.Errorf("got registration ID %d, want %d", got.RegistrationID(), registrationID)
		}
		return nil
	}
}

func lookupGoldenSubscription(available bool) func(*conductor.Conductor) error {
	return func(conductorInstance *conductor.Conductor) error {
		var registrationID int64
		var wantStreamID int32
		if available {
			registrationID = goldenAvailableImageEvent.SubscriptionRegistrationID
			wantStreamID = goldenAvailableImageEvent.StreamID
		} else {
			registrationID = goldenUnavailableImageEvent.SubscriptionRegistrationID
		}
		got, ok := conductorInstance.SubscriptionByRegID(registrationID)
		if !ok {
			return fmt.Errorf("subscription %d not found", registrationID)
		}
		if got.RegistrationID() != registrationID {
			return fmt.Errorf("got registration ID %d, want %d", got.RegistrationID(), registrationID)
		}
		if available && got.StreamID() != wantStreamID {
			return fmt.Errorf("got stream ID %d, want %d", got.StreamID(), wantStreamID)
		}
		return nil
	}
}

func runReentrantDoWorkFromImageHandlerTest(
	t *testing.T,
	available bool,
	enqueueFollowUpEvent func(*testing.T, *conductor.Conductor),
) {
	t.Helper()

	var nestedDoWorkCalls int
	handlerDone := make(chan error, 1)
	lifecycle := &recordingImageLifecycle{addImageOK: true}
	if !available {
		lifecycle = &recordingImageLifecycle{}
	}
	conductorInstance := newConductorWithRecordingLifecycle(t, lifecycle, func(error) {})
	registerGoldenImageSubscription(conductorInstance)
	if available {
		seedGoldenAvailableLogBuffers(t, conductorInstance)
	}

	handlerAction := func(conductor.ImageHandle) {
		nestedDoWorkCalls++
		if enqueueFollowUpEvent != nil {
			enqueueFollowUpEvent(t, conductorInstance)
		}
		if conductorInstance.DoWork() < 1 {
			handlerDone <- fmt.Errorf("nested DoWork processed no work")
			return
		}
		handlerDone <- nil
	}
	if available {
		lifecycle.availableImageHandler = handlerAction
		transmitConductorEvent(
			t,
			conductorInstance.EventsAdapterForTest(),
			conductor.OnAvailableImage,
			conductor.AvailableImageBytes(goldenAvailableImageEvent),
		)
	} else {
		lifecycle.unavailableImageHandler = handlerAction
		triggerGoldenUnavailableImage(t, conductorInstance)
	}

	require.Equal(t, 1, conductorInstance.DoWork())
	select {
	case err := <-handlerDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("reentrant DoWork from image handler did not complete")
	}
	require.Equal(t, 1, nestedDoWorkCalls)
}

func TestConductor_imageHandler_acquiresWriteLockForMapMutation(t *testing.T) {
	t.Parallel()

	conductorInstance := runImageHandlerActionTest(
		t,
		true,
		func(conductorInstance *conductor.Conductor, _ *recordingImageLifecycle) {
			registerGoldenImageSubscription(conductorInstance)
			seedGoldenAvailableLogBuffers(t, conductorInstance)
		},
		func(t *testing.T, conductorInstance *conductor.Conductor) {
			t.Helper()
			transmitConductorEvent(
				t,
				conductorInstance.EventsAdapterForTest(),
				conductor.OnAvailableImage,
				conductor.AvailableImageBytes(goldenAvailableImageEvent),
			)
		},
		insertAndLookupPublication(99),
	)

	got, ok := conductorInstance.PublicationByRegID(99)
	require.True(t, ok)
	require.Equal(t, int64(99), got.RegistrationID())
}

func enqueueGoldenAvailableImage(
	t *testing.T,
	conductorInstance *conductor.Conductor,
) {
	t.Helper()
	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(goldenAvailableImageEvent),
	)
}

func TestConductor_imageHandler_reentrantDoWork_fromAvailableImageHandler(t *testing.T) {
	t.Parallel()

	runReentrantDoWorkFromImageHandlerTest(t, true, enqueueGoldenAvailableImage)
}

func TestConductor_imageHandler_reentrantDoWork_fromUnavailableImageHandler(t *testing.T) {
	t.Parallel()

	runReentrantDoWorkFromImageHandlerTest(t, false, triggerGoldenUnavailableImage)
}

func TestConductor_imageHandler_mapMutation_fromUnavailableImageHandler(t *testing.T) {
	t.Parallel()

	conductorInstance := runImageHandlerActionTest(
		t,
		false,
		func(conductorInstance *conductor.Conductor, _ *recordingImageLifecycle) {
			registerGoldenImageSubscription(conductorInstance)
		},
		triggerGoldenUnavailableImage,
		insertAndLookupPublication(88),
	)

	got, ok := conductorInstance.PublicationByRegID(88)
	require.True(t, ok)
	require.Equal(t, int64(88), got.RegistrationID())
}

func TestConductor_PublicationByRegID_fromImageHandler(t *testing.T) {
	t.Parallel()

	runImageHandlerActionTest(
		t,
		true,
		func(conductorInstance *conductor.Conductor, _ *recordingImageLifecycle) {
			conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})
			registerGoldenImageSubscription(conductorInstance)
			seedGoldenAvailableLogBuffers(t, conductorInstance)
		},
		func(t *testing.T, conductorInstance *conductor.Conductor) {
			t.Helper()
			transmitConductorEvent(
				t,
				conductorInstance.EventsAdapterForTest(),
				conductor.OnAvailableImage,
				conductor.AvailableImageBytes(goldenAvailableImageEvent),
			)
		},
		lookupPublication(42),
	)
}

func TestConductor_PublicationByRegID_fromUnavailableImageHandler(t *testing.T) {
	t.Parallel()

	runImageHandlerActionTest(
		t,
		false,
		func(conductorInstance *conductor.Conductor, _ *recordingImageLifecycle) {
			conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})
			registerGoldenImageSubscription(conductorInstance)
		},
		triggerGoldenUnavailableImage,
		lookupPublication(42),
	)
}

func TestConductor_SubscriptionByRegID_fromImageHandler(t *testing.T) {
	t.Parallel()

	runImageHandlerActionTest(
		t,
		true,
		func(conductorInstance *conductor.Conductor, _ *recordingImageLifecycle) {
			registerGoldenImageSubscription(conductorInstance)
			seedGoldenAvailableLogBuffers(t, conductorInstance)
		},
		func(t *testing.T, conductorInstance *conductor.Conductor) {
			t.Helper()
			transmitConductorEvent(
				t,
				conductorInstance.EventsAdapterForTest(),
				conductor.OnAvailableImage,
				conductor.AvailableImageBytes(goldenAvailableImageEvent),
			)
		},
		lookupGoldenSubscription(true),
	)
}

func TestConductor_SubscriptionByRegID_fromUnavailableImageHandler(t *testing.T) {
	t.Parallel()

	runImageHandlerActionTest(
		t,
		false,
		func(conductorInstance *conductor.Conductor, _ *recordingImageLifecycle) {
			registerGoldenImageSubscription(conductorInstance)
		},
		triggerGoldenUnavailableImage,
		lookupGoldenSubscription(false),
	)
}

func TestConductor_subscriptionMap_lookup(t *testing.T) {
	t.Parallel()

	conductorInstance := newBareConductorForResourceMapTest(t)
	subscription := stubSubscription{registrationID: 7, streamID: 1001}

	conductor.RegisterSubscriptionForTest(conductorInstance, 7, subscription)
	require.Equal(t, 1, conductorInstance.SubscriptionCountForTest())

	got, ok := conductorInstance.SubscriptionByRegID(7)
	require.True(t, ok)
	require.Equal(t, int64(7), got.RegistrationID())
	require.Equal(t, int32(1001), got.StreamID())

	_, ok = conductorInstance.SubscriptionByRegID(99)
	require.False(t, ok)

	conductorInstance.RemoveSubscriptionForTest(7)
	require.Equal(t, 0, conductorInstance.SubscriptionCountForTest())
	_, ok = conductorInstance.SubscriptionByRegID(7)
	require.False(t, ok)
}

func TestConductor_subscriptionMap_nilEntryMiss(t *testing.T) {
	t.Parallel()

	conductorInstance := newBareConductorForResourceMapTest(t)
	conductorInstance.InsertNilSubscriptionForTest(7)

	require.Equal(t, 0, conductorInstance.SubscriptionCountForTest())

	got, ok := conductorInstance.SubscriptionByRegID(7)
	require.False(t, ok)
	require.Nil(t, got)
}

func TestConductor_acquireLogBuffers_sharedIPCPathRefcount(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "shared.log")
	writeMinimalLogFileForRegistryTest(t, logPath)
	aliasPath := filepath.Join(dir, ".", "shared.log")

	conductorInstance := newConductorWithZeroNanoClockAndMediaDir(t, dir)
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)

	first, err := conductorInstance.AcquireLogBuffersForTest(logPath)
	require.NoError(t, err)
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))

	second, err := conductorInstance.AcquireLogBuffersForTest(aliasPath)
	require.NoError(t, err)
	require.Same(t, first.MappedLogBuffer(), second.MappedLogBuffer())
	require.Equal(t, int32(2), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.Equal(t, int32(2), conductor.LogBuffersRegistryRefCountForTest(registry, aliasPath))

	conductorInstance.ReleaseLogBuffersForTest(logPath, 0)
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))

	conductorInstance.ReleaseLogBuffersForTest(aliasPath, 0)
	require.Zero(t, conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func TestConductor_AcquireLogBuffers_propagatesRegistryErrors(t *testing.T) {
	t.Parallel()

	conductorInstance := newConductorWithZeroNanoClockAndMediaDir(t, t.TempDir())
	_, err := conductorInstance.AcquireLogBuffersForTest("")
	require.ErrorIs(t, err, conductor.ErrLogFileNameEmpty)
}

func TestConductor_ReleaseLogBuffers_positiveLinger_sweptByDoWork(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "wrapper-linger.log")
	writeMinimalLogFileForRegistryTest(t, logPath)

	const startNs int64 = 6_000_000_000
	const lingerNs int64 = int64(time.Second)

	conductorInstance, nanoNs := newConductorForMutableNanoTest(
		t,
		func(error) {},
		resourceMapTestEpochMs,
	)
	conductorInstance.SeedServiceTimestamps(startNs)
	*nanoNs = startNs
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)

	scheduleLingerForConductorLog(t, conductorInstance, logPath, lingerNs)
	assertRegistryLingerScheduled(t, registry, logPath, startNs, lingerNs)

	*nanoNs = startNs + lingerNs - 1
	conductorInstance.DoWork()
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
	require.True(t, registry.HasScheduledLinger())

	*nanoNs = startNs + lingerNs
	work := conductorInstance.DoWork()
	require.GreaterOrEqual(t, work, 1)
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
	require.False(t, registry.HasScheduledLinger())
}

func TestConductor_ReleaseLogBuffers_zeroNanoClock_usesSharedTimeDomain(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "zero-clock.log")
	writeMinimalLogFileForRegistryTest(t, logPath)

	const lingerNs int64 = int64(time.Hour)

	conductorInstance := newConductorWithZeroNanoClockAndMediaDir(t, dir)
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)

	scheduleLingerForConductorLog(t, conductorInstance, logPath, lingerNs)
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))

	deadline := conductor.LingerDeadlineNsForTest(registry, logPath)
	require.NotZero(t, deadline)
	assertLingerRetainedAt(
		t,
		registry,
		logPath,
		conductorInstance.SweepLingeringLogBuffersAtForTest,
		0,
	)
	assertLingerSweptAt(
		t,
		registry,
		logPath,
		conductorInstance.SweepLingeringLogBuffersAtForTest,
		deadline,
	)
}

func TestConductor_ReleaseLogBuffers_zeroNanoClockAdvance_sweptByDoWork(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "zero-clock-advance.log")
	writeMinimalLogFileForRegistryTest(t, logPath)

	const lingerNs int64 = int64(time.Second)

	conductorInstance, nanoNs := newConductorForMutableNanoTest(
		t,
		func(error) {},
		resourceMapTestEpochMs,
	)
	conductorInstance.SeedServiceTimestamps(0)
	*nanoNs = 0
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)

	scheduleLingerForConductorLog(t, conductorInstance, logPath, lingerNs)
	assertRegistryLingerScheduled(t, registry, logPath, 0, lingerNs)

	*nanoNs = lingerNs - 1
	conductorInstance.DoWork()
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
	require.True(t, registry.HasScheduledLinger())

	*nanoNs = lingerNs
	work := conductorInstance.DoWork()
	require.GreaterOrEqual(t, work, 1)
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
	require.False(t, registry.HasScheduledLinger())
}

func TestConductor_acquireAfterLingerScheduled_keepsLinger(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "conductor-reacquire.log")
	writeMinimalLogFileForRegistryTest(t, logPath)

	const startNs int64 = 7_000_000_000
	const lingerNs int64 = int64(time.Second)

	conductorInstance, nanoNs := newConductorForMutableNanoTest(
		t,
		func(error) {},
		resourceMapTestEpochMs,
	)
	*nanoNs = startNs
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)

	scheduleLingerForConductorLog(t, conductorInstance, logPath, lingerNs)
	deadline := conductor.LingerDeadlineNsForTest(registry, logPath)
	require.NotZero(t, deadline)

	_, err := conductorInstance.AcquireLogBuffersForTest(logPath)
	require.NoError(t, err)
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.Equal(t, deadline, conductor.LingerDeadlineNsForTest(registry, logPath))

	conductorInstance.ReleaseLogBuffersForTest(logPath, 0)
	require.Equal(t, deadline, conductor.LingerDeadlineNsForTest(registry, logPath))
	assertLingerSweptAt(
		t,
		registry,
		logPath,
		conductorInstance.SweepLingeringLogBuffersAtForTest,
		deadline,
	)
}

func TestConductor_sweepLingeringLogBuffers_afterDeadline(t *testing.T) {
	t.Parallel()

	const startNs int64 = 1_000_000_000
	const lingerNs int64 = int64(5 * time.Second)

	conductorInstance, nanoNs := newConductorForMutableNanoTest(
		t,
		func(error) {},
		resourceMapTestEpochMs,
	)
	conductorInstance.SeedServiceTimestamps(startNs)

	registry, logPath := acquireLogOnConductorRegistry(t, conductorInstance, "linger.log")
	scheduleLingerForRegistryLog(t, registry, logPath, startNs, lingerNs)
	assertRegistryLingerScheduled(t, registry, logPath, startNs, lingerNs)

	*nanoNs = startNs + lingerNs - 1
	assertLingerRetainedAt(
		t,
		registry,
		logPath,
		conductorInstance.SweepLingeringLogBuffersAtForTest,
		*nanoNs,
	)

	*nanoNs = startNs + lingerNs
	assertLingerSweptAt(
		t,
		registry,
		logPath,
		conductorInstance.SweepLingeringLogBuffersAtForTest,
		*nanoNs,
	)
	require.Zero(t, conductorInstance.SweepLingeringLogBuffersAtForTest(*nanoNs))
}

func TestConductor_DoWork_sweepsLingeringLogBuffers(t *testing.T) {
	t.Parallel()

	const startNs int64 = 2_000_000_000
	const lingerNs int64 = int64(time.Second)

	conductorInstance, nanoNs := newConductorForMutableNanoTest(
		t,
		func(error) {},
		resourceMapTestEpochMs,
	)
	conductorInstance.SeedServiceTimestamps(startNs)

	registry, logPath := acquireLogOnConductorRegistry(t, conductorInstance, "do-work-linger.log")
	scheduleLingerForRegistryLog(t, registry, logPath, startNs, lingerNs)
	require.True(t, registry.HasScheduledLinger())

	*nanoNs = startNs + lingerNs - 1
	conductorInstance.DoWork()
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
	require.True(t, registry.HasScheduledLinger())

	*nanoNs = startNs + lingerNs
	work := conductorInstance.DoWork()
	require.GreaterOrEqual(t, work, 1)
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
	require.False(t, registry.HasScheduledLinger())
	require.Zero(t, conductor.LogBuffersRegistryLingerScheduledForTest(registry))
}

func TestConductor_DrainLingeringLogBuffers_closeHook(t *testing.T) {
	t.Parallel()

	const startNs int64 = 3_000_000_000
	const lingerNs int64 = int64(2 * time.Second)

	conductorInstance, nanoNs := newConductorForMutableNanoTest(
		t,
		func(error) {},
		resourceMapTestEpochMs,
	)
	registry, logPath := acquireLogOnConductorRegistry(t, conductorInstance, "drain-linger.log")
	scheduleLingerForRegistryLog(t, registry, logPath, startNs, lingerNs)

	*nanoNs = startNs + lingerNs - 1
	conductorInstance.DrainLingeringLogBuffers()
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))

	*nanoNs = startNs + lingerNs
	require.Equal(t, 1, conductorInstance.DrainLingeringLogBuffers())
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func TestConductor_UnmapLingeringLogBuffers_waitsForSweptUnmap(t *testing.T) {
	t.Parallel()

	const startNs int64 = 4_000_000_000
	const lingerNs int64 = int64(time.Second)

	conductorInstance, nanoNs := newConductorForMutableNanoTest(
		t,
		func(error) {},
		resourceMapTestEpochMs,
	)
	conductorInstance.SeedServiceTimestamps(startNs)

	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	logPath := filepath.Join(t.TempDir(), "unmap-swept.log")
	writeMinimalLogFileForRegistryTest(t, logPath)
	buffers, err := registry.Acquire(logPath)
	require.NoError(t, err)
	require.True(t, conductor.IsLogBuffersRegistryUnmapperRunningForTest(registry))
	mapped := buffers.MappedLogBuffer()
	scheduleLingerForRegistryLog(t, registry, logPath, startNs, lingerNs)

	*nanoNs = startNs + lingerNs
	require.GreaterOrEqual(t, conductorInstance.DoWork(), 1)
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))

	require.Zero(t, conductorInstance.UnmapLingeringLogBuffers())
	require.Nil(t, mapped.Bytes())
	require.False(t, conductor.IsLogBuffersRegistryUnmapperRunningForTest(registry))
}

func TestConductor_DoWork_terminatingSkipsLingerSweep(t *testing.T) {
	t.Parallel()

	const startNs int64 = 8_000_000_000
	const lingerNs int64 = int64(time.Second)

	conductorInstance, nanoNs := newConductorForMutableNanoTest(
		t,
		func(error) {},
		resourceMapTestEpochMs,
	)
	registry, logPath := acquireLogOnConductorRegistry(t, conductorInstance, "terminating-linger.log")
	scheduleLingerForRegistryLog(t, registry, logPath, startNs, lingerNs)

	*nanoNs = startNs + lingerNs
	conductorInstance.SetTerminatingForTest(true)
	require.Zero(t, conductorInstance.DoWork())
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))

	require.Equal(t, 1, conductorInstance.DrainLingeringLogBuffers())
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func TestConductor_DoWork_sweepUsesFreshNanoClock(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "fresh-clock.log")
	writeMinimalLogFileForRegistryTest(t, logPath)

	const startNs int64 = 10_000_000_000
	const lingerNs int64 = int64(time.Second)

	conductorInstance, nanoNs := newConductorForMutableNanoTest(
		t,
		func(error) {},
		resourceMapTestEpochMs,
	)
	conductorInstance.SeedServiceTimestamps(startNs)
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)

	*nanoNs = startNs
	scheduleLingerForConductorLog(t, conductorInstance, logPath, lingerNs)

	*nanoNs = startNs + lingerNs
	work := conductorInstance.DoWork()
	require.GreaterOrEqual(t, work, 1)
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func TestConductor_nestedDoWork_lingerRetainedBeforeDeadlineSweptAfter(t *testing.T) {
	t.Parallel()

	const startNs int64 = 11_000_000_000

	fixtures := setupImageHandlerLingerFixtures(t, startNs, "nested-linger.log")
	conductorInstance := fixtures.conductor
	nanoNs := fixtures.nanoNs
	lifecycle := fixtures.lifecycle
	registry := fixtures.registry
	lingerLogPath := fixtures.lingerLogPath
	lingerNs := fixtures.lingerNs

	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, lingerLogPath))

	*nanoNs = startNs + lingerNs - 1
	var nestedDoWorkCalls int
	lifecycle.unavailableImageHandler = func(conductor.ImageHandle) {
		nestedDoWorkCalls++
		conductorInstance.DoWork()
	}
	triggerGoldenUnavailableImage(t, conductorInstance)
	conductorInstance.DoWork()
	require.Equal(t, 1, nestedDoWorkCalls)
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, lingerLogPath))

	*nanoNs = startNs + lingerNs
	conductorInstance.DoWork()
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, lingerLogPath))
}

func TestConductor_DrainLingeringLogBuffers_fromImageHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		startNs       int64
		atDeadline    bool
		wantDrainWork int
		wantContains  bool
	}{
		{
			name:          "beforeDeadline",
			startNs:       12_000_000_000,
			atDeadline:    false,
			wantDrainWork: 0,
			wantContains:  true,
		},
		{
			name:          "afterDeadline",
			startNs:       12_100_000_000,
			atDeadline:    true,
			wantDrainWork: 1,
			wantContains:  false,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			fixtures := setupImageHandlerLingerFixtures(
				t,
				testCase.startNs,
				"drain-handler-"+testCase.name+".log",
			)
			conductorInstance := fixtures.conductor
			nanoNs := fixtures.nanoNs
			lifecycle := fixtures.lifecycle
			registry := fixtures.registry
			lingerLogPath := fixtures.lingerLogPath
			lingerNs := fixtures.lingerNs

			if testCase.atDeadline {
				*nanoNs = testCase.startNs + lingerNs
			} else {
				*nanoNs = testCase.startNs + lingerNs - 1
			}

			var drainWork int
			lifecycle.unavailableImageHandler = func(conductor.ImageHandle) {
				drainWork = conductorInstance.DrainLingeringLogBuffers()
			}
			triggerGoldenUnavailableImage(t, conductorInstance)
			conductorInstance.DoWork()
			require.Equal(t, testCase.wantDrainWork, drainWork)
			require.Equal(
				t,
				testCase.wantContains,
				conductor.LogBuffersRegistryContainsForTest(registry, lingerLogPath),
			)
		})
	}
}

func TestConductor_DoWork_sweepAdvancesNanoClockMidService(t *testing.T) {
	t.Parallel()

	const startNs int64 = 13_000_000_000
	const lingerNs int64 = int64(time.Second)

	lifecycle := &recordingImageLifecycle{
		hasImage:           true,
		removedLogFileName: goldenAvailableImageEvent.LogFileName,
	}
	conductorInstance, nanoNs := newConductorWithRecordingLifecycleMutableNano(
		t,
		lifecycle,
		func(error) {},
		resourceMapTestEpochMs,
		"/",
	)
	conductorInstance.SeedServiceTimestamps(startNs)

	dir := t.TempDir()
	logPath := filepath.Join(dir, "mid-service-clock.log")
	writeMinimalLogFileForRegistryTest(t, logPath)
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)

	_, err := conductorInstance.AcquireLogBuffersForTest(logPath)
	require.NoError(t, err)

	*nanoNs = startNs
	conductorInstance.ReleaseLogBuffersForTest(logPath, lingerNs)

	*nanoNs = startNs + lingerNs - 1
	lifecycle.unavailableImageHandler = func(conductor.ImageHandle) {
		*nanoNs = startNs + lingerNs
	}
	registerGoldenImageSubscription(conductorInstance)
	seedGoldenAvailableLogBuffers(t, conductorInstance)
	triggerGoldenUnavailableImage(t, conductorInstance)
	conductorInstance.DoWork()
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}
