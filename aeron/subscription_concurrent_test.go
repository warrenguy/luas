package aeron

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/aeron/conductor"
	"github.com/warrenguy/luas/aeron/logbuffer"
	"github.com/warrenguy/luas/atomicbuffer"
	"github.com/warrenguy/luas/broadcast"
)

type subscriptionChurnMode int

const (
	churnCloseFromPoll subscriptionChurnMode = iota
	churnSwapUnavailableImageHandler
)

func TestSubscription_Poll_concurrentImageChurn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mode subscriptionChurnMode
	}{
		{name: "closeFromPollGoroutine", mode: churnCloseFromPoll},
		{name: "setUnavailableImageHandler", mode: churnSwapUnavailableImageHandler},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runSubscriptionPollConcurrentImageChurn(t, tc.mode)
		})
	}
}

func runSubscriptionPollConcurrentImageChurn(t testing.TB, mode subscriptionChurnMode) {
	t.Helper()

	closeFromPoll := mode == churnCloseFromPoll
	fixture := newSubscriptionChurnFixture(t, closeFromPoll)

	var wg sync.WaitGroup
	wg.Go(func() { fixture.pollUntilStopped(closeFromPoll) })

	if mode == churnSwapUnavailableImageHandler {
		wg.Go(fixture.swapUnavailableImageHandlerUntilStopped)
	}

	wg.Go(func() { fixture.churnImages(t, 80) })
	wg.Wait()

	require.Positive(t, fixture.fragments.Load())

	if closeFromPoll {
		require.True(t, fixture.sub.IsClosed())
		require.Zero(t, fixture.sub.Poll(noopFragmentHandler, 1))
	}
}

type subscriptionChurnFixture struct {
	conductor *conductor.Conductor
	counters  *cnc.CountersReader
	sub       *Subscription
	tx        *broadcast.BroadcastTransmitter
	logPath   string
	recordErr func(error)
	fragments atomic.Int32
	stop      chan struct{}
}

func newSubscriptionChurnFixture(t testing.TB, closeFromPoll bool) *subscriptionChurnFixture {
	t.Helper()

	dir, logPath := setupConductorImageLifecycleLogFile(t, "image.log")
	commitSubscriptionChurnFrames(t, logPath)

	recordErr := func(err error) {
		if err != nil {
			t.Error(err)
		}
	}

	conductorInstance := newConductorWithImageLifecycleForTest(
		t,
		recordErr,
		dir,
		func(config *conductor.Config) {
			config.ResourceLingerNs = int64(time.Hour)
			config.ResourceLifecycle = ConductorResourceLifecycle
		},
	)
	counters := subscriberPositionCountersReaderForConductorTest(t)
	conductorInstance.SetCountersReaderForTest(counters)

	sub := registerConductorImageLifecycleSubscriptionForTest(
		conductorInstance,
		conductorImageLifecycleGoldenAvailableImage.StreamID,
	)
	if closeFromPoll {
		sub.conductor = conductorInstance
	}

	tx, err := broadcast.NewBroadcastTransmitter(
		conductorInstance.EventsAdapterForTest().CopyReceiverForTest().BroadcastReceiver().Buffer(),
	)
	require.NoError(t, err)

	return &subscriptionChurnFixture{
		conductor: conductorInstance,
		counters:  counters,
		sub:       sub,
		tx:        tx,
		logPath:   logPath,
		recordErr: recordErr,
		stop:      make(chan struct{}),
	}
}

// pollUntilStopped keeps polling after Close, until churnImages returns.
func (fixture *subscriptionChurnFixture) pollUntilStopped(closeFromPoll bool) {
	closedFromPoll := false
	handler := func(buffer atomicbuffer.AtomicBuffer, _, _ int, header *Header) {
		fixture.fragments.Add(1)
		_ = header.SessionID()
		_ = buffer.Capacity()

		// Close on the first fragment, while churn still calls DoWork.
		if !closeFromPoll || closedFromPoll {
			return
		}

		closedFromPoll = true
		fixture.recordErr(fixture.sub.Close())
	}

	for {
		fixture.sub.Poll(handler, 24)

		_ = fixture.sub.ImageCount()
		_ = fixture.sub.IsConnected()
		_, _ = fixture.sub.ImageAt(0)

		select {
		case <-fixture.stop:
			return
		default:
		}
	}
}

func (fixture *subscriptionChurnFixture) swapUnavailableImageHandlerUntilStopped() {
	handlers := []UnavailableImageHandler{nil, func(*Image) {}}
	var turn int

	for {
		select {
		case <-fixture.stop:
			return
		default:
		}

		fixture.sub.SetUnavailableImageHandler(handlers[turn%2])
		turn++
	}
}

// churnImages adds and removes one image per cycle, then closes stop.
func (fixture *subscriptionChurnFixture) churnImages(t testing.TB, cycles int) {
	t.Helper()

	defer close(fixture.stop)

	for step := range cycles {
		if fixture.sub.IsClosed() {
			return
		}

		correlation := conductorImageLifecycleTestCorrelationID + int64(step)
		fixture.addImage(correlation)

		// Hold the first image until the poller reads from it. Without the
		// wait, churn can finish every cycle before the poller runs.
		if step == 0 {
			fixture.awaitFirstFragment(t)
		}

		fixture.removeImage(correlation)
	}
}

func (fixture *subscriptionChurnFixture) addImage(correlation int64) {
	fixture.recordErr(fixture.counters.SetCounterValue(
		conductorImageLifecycleTestSubscriberCounter,
		TestSubscriberPositionAtJoin,
	))
	available := availableImageEventForConductorTest(fixture.logPath)
	available.CorrelationID = correlation
	fixture.recordErr(fixture.tx.Transmit(
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(available),
	))
	fixture.conductor.DoWork()
}

func (fixture *subscriptionChurnFixture) removeImage(correlation int64) {
	unavailable := conductorImageLifecycleGoldenUnavailableImage
	unavailable.CorrelationID = correlation
	fixture.recordErr(fixture.tx.Transmit(
		conductor.OnUnavailableImage,
		conductor.UnavailableImageBytes(unavailable),
	))
	fixture.conductor.DoWork()
}

func (fixture *subscriptionChurnFixture) awaitFirstFragment(t testing.TB) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for fixture.fragments.Load() == 0 && !fixture.sub.IsClosed() {
		if time.Now().After(deadline) {
			t.Error("poller did not read a fragment from the first image")
			return
		}

		runtime.Gosched()
	}
}

func commitSubscriptionChurnFrames(t testing.TB, logPath string) {
	t.Helper()

	mapped, err := logbuffer.MapLogFile(logPath)
	require.NoError(t, err)

	term, err := mapped.TermBuffer(0)
	require.NoError(t, err)

	frames := make([]FragmentedFrameForTest, 24)
	for i := range frames {
		frames[i] = FragmentedFrameForTest{Flags: logbuffer.Unfragmented, Payload: []byte("churn")}
	}
	WriteCommittedDataFramesForStreamForTest(
		term,
		int(TestSubscriberPositionAtJoin),
		conductorImageLifecycleGoldenAvailableImage.SessionID,
		conductorImageLifecycleGoldenAvailableImage.StreamID,
		TestPublicationInitialTermID,
		frames,
	)
	require.NoError(t, mapped.Close())
}
