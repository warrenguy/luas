//go:build rapid

package aeron_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/aeron/conductor"
	"github.com/warrenguy/luas/aeron/internal/testutil"
	"github.com/warrenguy/luas/idlestrategy"
	checkutil "github.com/warrenguy/luas/internal/testutil"
)

func rapidDoWorkNTimes(rt *rapid.T, client *aeron.Client, count int) {
	rt.Helper()

	for range count {
		client.DoWork()
	}
}

func rapidPreCloseDoWorkWithQueries(rt *rapid.T, client *aeron.Client, clientID int64, count int) {
	rt.Helper()

	for i := range count {
		if rapid.Bool().Draw(rt, fmt.Sprintf("isTerminating%d", i)) {
			require.False(rt, client.IsTerminating())
		}
		if rapid.Bool().Draw(rt, fmt.Sprintf("clientId%d", i)) {
			require.Equal(rt, clientID, client.ClientId())
		}
		client.DoWork()
	}
}

func TestClient_closeDoWorkGuards_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		clientID := int64(rapid.IntRange(1, 1_000_000).Draw(t, "clientID"))
		client := newRapidTestClient(t, clientID)
		require.Equal(t, clientID, client.ClientId())

		rapidPreCloseDoWorkWithQueries(
			t,
			client,
			clientID,
			rapid.IntRange(0, 32).Draw(t, "doWorkBeforeClose"),
		)
		require.Equal(t, clientID, client.ClientId())

		require.NoError(t, client.Close())

		for range rapid.IntRange(0, 16).Draw(t, "doWorkAfterClose") {
			require.Zero(t, client.DoWork())
		}
		require.False(t, client.IsTerminating())
	})
}

func rapidInterleaveDoWorkAndIsTerminating(rt *rapid.T, client *aeron.Client, count int) {
	rt.Helper()

	for i := range count {
		if rapid.Bool().Draw(rt, fmt.Sprintf("doWork%d", i)) {
			client.DoWork()
		} else {
			_ = client.IsTerminating()
		}
	}
}

func TestClient_doubleClose_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		client := newRapidTestClient(t, 0)

		rapidDoWorkNTimes(t, client, rapid.IntRange(0, 8).Draw(t, "doWorkBeforeClose"))

		require.NoError(t, client.Close())
		rapidInterleaveDoWorkAndIsTerminating(
			t,
			client,
			rapid.IntRange(0, 12).Draw(t, "betweenCloseOps"),
		)
		require.NoError(t, client.Close())
		require.Zero(t, client.DoWork())
		require.False(t, client.IsTerminating())
	})
}

type backgroundLifeOp int

const (
	lifeOpDoWork backgroundLifeOp = iota
	lifeOpStepClock
	lifeOpQuery
	lifeOpClose
	lifeOpAdd
)

func TestClient_backgroundConductorLifecycle_rapid(t *testing.T) {
	t.Parallel()

	opGen := rapid.SampledFrom([]backgroundLifeOp{
		lifeOpDoWork,
		lifeOpStepClock,
		lifeOpQuery,
		lifeOpClose,
		lifeOpAdd,
	})
	rapid.Check(t, func(rt *rapid.T) {
		tb := checkutil.IterationTB(t, rt)
		var nano atomic.Int64
		start := time.Now()
		nano.Store(testutil.TestStartNanoNs)
		clock := func() int64 {
			return nano.Load() + time.Since(start).Nanoseconds()
		}
		config := testutil.DefaultConductorConfig(nil)
		config.NanoClock = clock

		// The clock adds wall time. A scheduler stall between two conductor
		// ticks that outlasts InterServiceTimeout terminates the client. The
		// model does not expect that, so the timeout is one minute.
		config.InterServiceTimeout = time.Minute
		fixture := testutil.NewConductorFixture(tb, config)
		client := aeron.NewClientForTestWithDriverProxy(
			fixture.Conductor,
			fixture.DriverProxy,
			clock,
			testutil.TestClientID,
			time.Millisecond,
			200*time.Microsecond,
		)
		var ticks atomic.Int64
		client.SetDoWorkAfterHookForTest(tb, func() { ticks.Add(1) })
		aeron.StartBackgroundConductorForTest(
			client,
			idlestrategy.NewSleepingMillis(time.Nanosecond),
		)
		loopDone := aeron.ConductorLoopDoneForTest(client)
		rt.Cleanup(func() { _ = client.Close() })

		var closed, terminating bool
		steps := rapid.IntRange(1, 8).Draw(rt, "steps")
		for step := range steps {
			switch opGen.Draw(rt, fmt.Sprintf("op%d", step)) {
			case lifeOpDoWork:
				require.GreaterOrEqual(rt, client.DoWork(), 0)

			case lifeOpStepClock:
				if closed || terminating {
					break
				}

				require.Eventually(rt, func() bool {
					return ticks.Load() > 0
				}, time.Second, time.Millisecond)
				nano.Add(config.InterServiceTimeout.Nanoseconds() + 1)

				select {
				case <-loopDone:
				case <-time.After(time.Second):
					rt.Fatal("conductor loop did not exit")
				}

				terminating = true

			case lifeOpClose:
				require.NoError(rt, client.Close())
				stopped := ticks.Load()
				time.Sleep(2 * time.Millisecond)
				require.Equal(rt, stopped, ticks.Load())
				require.NoError(rt, client.Close())
				closed = true

			case lifeOpAdd:
				_, err := client.AddPublication(1, "aeron:ipc")
				require.Error(rt, err)

			case lifeOpQuery:
			}

			require.False(rt, client.HasConductorInvoker())
			wantTerminating := terminating && !closed
			require.Equal(rt, wantTerminating, client.IsTerminating())

			if wantTerminating {
				require.ErrorIs(
					rt,
					client.TerminationCause(),
					conductor.ErrConductorServiceTimeout,
				)
			} else {
				require.NoError(rt, client.TerminationCause())
			}

			loopExited := terminating || closed
			select {
			case <-loopDone:
				require.True(rt, loopExited)

			default:
				require.False(rt, loopExited)
			}
		}
	})
}
