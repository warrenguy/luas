//go:build testhooks

package aeron

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron/conductor"
	"github.com/warrenguy/luas/aeron/internal/testutil"
	"github.com/warrenguy/luas/idlestrategy"
)

// TestClient_backgroundConductor_ticksUntilClose asserts that the conductor
// loop ticks without a test DoWork call and that Close stops the ticks.
func TestClient_backgroundConductor_ticksUntilClose(t *testing.T) {
	client := NewClientForTest(nil, func() int64 { return 0 })
	var ticks atomic.Int64
	client.SetDoWorkAfterHookForTest(t, func() {
		ticks.Add(1)
	})

	startNanosecondConductor(t, client)
	require.False(t, client.HasConductorInvoker())

	require.Eventually(t, func() bool {
		return ticks.Load() > 0
	}, time.Second, time.Millisecond)

	require.NoError(t, client.Close())
	stopped := ticks.Load()
	time.Sleep(5 * time.Millisecond)
	require.Equal(t, stopped, ticks.Load())
}

func newBackgroundConductorClient(
	tb testing.TB,
	handler func(error),
) (*Client, *atomic.Int64) {
	tb.Helper()

	var nano atomic.Int64
	nano.Store(testutil.TestStartNanoNs)
	clock := func() int64 { return nano.Load() }
	config := testutil.DefaultConductorConfig(nil)
	config.NanoClock = clock
	config.ErrorHandler = handler
	fixture := testutil.NewConductorFixture(tb, config)
	client := NewClientForTest(fixture.Conductor, clock)
	return client, &nano
}

func startNanosecondConductor(tb testing.TB, client *Client) {
	tb.Helper()

	StartBackgroundConductorForTest(
		client,
		idlestrategy.NewSleepingMillis(time.Nanosecond),
	)
}

// awaitConductorTick waits until the conductor loop runs one DoWork.
//
// The first Client.DoWork sets the service timestamps to the current clock.
// A clock step before that tick does not expire the timeout.
func awaitConductorTick(tb testing.TB, client *Client) {
	tb.Helper()

	var ticks atomic.Int64
	client.SetDoWorkAfterHookForTest(tb, func() { ticks.Add(1) })
	require.Eventually(tb, func() bool {
		return ticks.Load() > 0
	}, time.Second, time.Millisecond)
}

// expireServiceTimeout waits for one conductor tick, then steps the clock
// past the inter-service timeout.
func expireServiceTimeout(
	tb testing.TB,
	client *Client,
	nano *atomic.Int64,
) {
	tb.Helper()

	awaitConductorTick(tb, client)
	nano.Add(testutil.TestInterServiceTimeout.Nanoseconds() + 1)
}

func awaitConductorLoopExit(tb testing.TB, client *Client) {
	tb.Helper()

	select {
	case <-ConductorLoopDoneForTest(client):
	case <-time.After(time.Second):
		require.FailNow(tb, "conductor loop did not exit")
	}
}

func TestClient_backgroundConductor_exitsOnTermination(t *testing.T) {
	client, nano := newBackgroundConductorClient(t, nil)
	startNanosecondConductor(t, client)
	expireServiceTimeout(t, client, nano)
	awaitConductorLoopExit(t, client)
	require.True(t, client.IsTerminating())

	var closeErr error
	testutil.RequireReturnsWithin(t, time.Second, func() { closeErr = client.Close() })
	require.NoError(t, closeErr)
}

func TestClient_backgroundConductor_terminationCause(t *testing.T) {
	client, nano := newBackgroundConductorClient(t, nil)
	client.conductor.ReportError(errors.New("non-fatal"))
	require.False(t, client.IsTerminating())
	require.NoError(t, client.TerminationCause())

	startNanosecondConductor(t, client)
	expireServiceTimeout(t, client, nano)

	// Read the cause as soon as IsTerminating returns true. A later read
	// misses a terminating flag that becomes visible before the cause.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if client.IsTerminating() {
			require.ErrorIs(
				t,
				client.TerminationCause(),
				conductor.ErrConductorServiceTimeout,
			)
			require.NoError(t, client.Close())
			require.NoError(t, client.TerminationCause())
			return
		}
		time.Sleep(time.Millisecond)
	}
	require.FailNow(t, "IsTerminating stayed false")
}

func TestClient_backgroundConductor_closeFromHandler(t *testing.T) {
	closeErrs := make(chan error, 1)
	var client *Client
	client, nano := newBackgroundConductorClient(t, func(error) {
		closeErrs <- client.Close()
	})
	StartBackgroundConductorForTest(
		client,
		idlestrategy.NewSleepingMillis(time.Millisecond),
	)
	expireServiceTimeout(t, client, nano)

	select {
	case err := <-closeErrs:
		require.ErrorIs(t, err, ErrReentrantCall)
	case <-time.After(time.Second):
		t.Fatal("handler did not call Close")
	}
	require.NoError(t, client.Close())
}

func TestClient_DoWork_besideBackgroundConductor(t *testing.T) {
	client, _ := newBackgroundConductorClient(t, nil)
	startNanosecondConductor(t, client)
	t.Cleanup(func() { _ = client.Close() })

	for range 200 {
		require.GreaterOrEqual(t, client.DoWork(), 0)
	}
}

func TestClient_HasConductorInvoker(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		var client *Client
		require.False(t, client.HasConductorInvoker())
	})

	t.Run("fixture", func(t *testing.T) {
		client := NewClientForTest(nil, func() int64 { return 0 })
		require.True(t, client.HasConductorInvoker())
	})

	t.Run("started", func(t *testing.T) {
		client, _ := newBackgroundConductorClient(t, nil)
		startNanosecondConductor(t, client)
		t.Cleanup(func() { _ = client.Close() })
		require.False(t, client.HasConductorInvoker())
	})

	t.Run("after loop exit", func(t *testing.T) {
		client, nano := newBackgroundConductorClient(t, nil)
		startNanosecondConductor(t, client)
		t.Cleanup(func() { _ = client.Close() })
		expireServiceTimeout(t, client, nano)
		awaitConductorLoopExit(t, client)
		require.False(t, client.HasConductorInvoker())
	})

	t.Run("after close", func(t *testing.T) {
		client, _ := newBackgroundConductorClient(t, nil)
		startNanosecondConductor(t, client)
		require.NoError(t, client.Close())
		require.False(t, client.HasConductorInvoker())
	})
}
