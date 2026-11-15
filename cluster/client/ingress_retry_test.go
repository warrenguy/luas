//go:build testhooks

package client

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/idlestrategy"
)

func requireIngressTerminal(tb testing.TB, err error, wantStatus int64) {
	tb.Helper()

	var terminal *IngressTerminalError
	require.ErrorAs(tb, err, &terminal)
	require.Equal(tb, wantStatus, terminal.Status)
}

func requireIngressTimeout(tb testing.TB, err error, wantStatus int64, wantTimeout time.Duration) {
	tb.Helper()

	var timeoutErr *IngressTimeoutError
	require.ErrorAs(tb, err, &timeoutErr)
	require.Equal(tb, wantStatus, timeoutErr.Status)
	require.Equal(tb, wantTimeout, timeoutErr.Timeout)
}

// setupIngressRetryDoWorkRecorder installs a global DoWork counter.
//
// Do not use from t.Parallel() subtests; the recorder is process-wide and unsynchronized.
func setupIngressRetryDoWorkRecorder(t *testing.T) *int {
	t.Helper()

	var count int
	SetIngressRetryDoWorkRecorderForTest(t, func() { count++ })
	return &count
}

func clusterWithSequentialIngress(
	t *testing.T,
	results ...int64,
) (*AeronCluster, *sequentialOfferResultIngressPublication) {
	t.Helper()

	var seqPub *sequentialOfferResultIngressPublication
	ac := newAeronClusterFromConnectFixture(t, func(result *ConnectResult) {
		seqPub = withSequentialOfferResultIngress(t, result, results...)
	})
	return ac, seqPub
}

// clusterWithAtomicNanoClock wires session.nanoClock to nowNs so tests advance the
// retry deadline from inside offerFn.
func clusterWithAtomicNanoClock(
	t *testing.T,
	startNs int64,
	ingressRetryTimeout time.Duration,
) (*AeronCluster, *atomic.Int64) {
	t.Helper()

	nowNs := &atomic.Int64{}
	nowNs.Store(startNs)
	ac := newAeronClusterFromConnectFixture(t, func(result *ConnectResult) {
		withAtomicNanoClock(result, nowNs)
		mutateConnectConfig(result, func(cfg *Config) {
			cfg.IngressRetryTimeout = ingressRetryTimeout
		})
	})
	return ac, nowNs
}

// recordingIdleStrategy counts Idle/Reset invocations (AdminAction + timeout oracle).
//
// totalIdleCalls is cumulative across Reset; idleCalls is the current epoch.
type recordingIdleStrategy struct {
	idleCalls      int
	totalIdleCalls int
	lastWorkCount  int
	resetCalls     int
}

func (r *recordingIdleStrategy) Idle(workCount int) {
	r.idleCalls++
	r.totalIdleCalls++
	r.lastWorkCount = workCount
}

func (r *recordingIdleStrategy) Reset() {
	r.resetCalls++
	r.idleCalls = 0
}

func TestIngressOfferUntilPositive_backPressuredThenSuccess(t *testing.T) {
	tests := []struct {
		name       string
		background bool
		wantDoWork int
	}{
		{name: "invoker", wantDoWork: 5},
		{name: "background", background: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idle := &recordingIdleStrategy{}
			ac, seqPub := clusterWithSequentialIngress(
				t,
				aeron.BackPressured,
				aeron.BackPressured,
				100,
			)

			if tc.background {
				aeron.StartBackgroundConductorForTest(
					ac.Aeron(),
					idlestrategy.NewSleepingMillis(time.Nanosecond),
				)
				t.Cleanup(func() { _ = ac.Aeron().Close() })
			}

			doWorkCount := setupIngressRetryDoWorkRecorder(t)

			pos, err := ac.IngressOfferUntilPositive(t.Context(), IngressOfferConfig{
				IdleStrategy: idle,
			}, ac.SendKeepAlive)
			require.NoError(t, err)
			require.Positive(t, pos)
			require.Equal(t, 3, seqPub.calls)
			require.Equal(t, tc.wantDoWork, *doWorkCount)
			require.Equal(t, 2, idle.totalIdleCalls)
			require.Equal(t, 1, idle.resetCalls)
		})
	}
}

func TestIngressOfferUntilPositive_immediateSuccess(t *testing.T) {
	idle := &recordingIdleStrategy{}
	ac, seqPub := clusterWithSequentialIngress(t, 100)
	doWorkCount := setupIngressRetryDoWorkRecorder(t)

	pos, err := ac.IngressOfferUntilPositive(t.Context(), IngressOfferConfig{
		IdleStrategy: idle,
	}, ac.SendKeepAlive)
	require.NoError(t, err)
	require.Equal(t, int64(100), pos)
	require.Equal(t, 1, seqPub.calls)
	require.Equal(t, 1, *doWorkCount)
	require.Zero(t, idle.totalIdleCalls)
	require.Equal(t, 1, idle.resetCalls)
}

func TestIngressOfferUntilPositive_adminActionRetry(t *testing.T) {
	tests := []struct {
		name         string
		results      []int64
		wantSeqCalls int
		wantDoWork   int
	}{
		{
			name:         "singleRetry",
			results:      []int64{aeron.AdminAction, 100},
			wantSeqCalls: 2,
			wantDoWork:   2,
		},
		{
			name:         "multiRetry",
			results:      []int64{aeron.AdminAction, aeron.AdminAction, 100},
			wantSeqCalls: 3,
			wantDoWork:   3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idle := &recordingIdleStrategy{}
			ac, seqPub := clusterWithSequentialIngress(t, tc.results...)
			doWorkCount := setupIngressRetryDoWorkRecorder(t)

			pos, err := ac.IngressOfferUntilPositive(t.Context(), IngressOfferConfig{
				IdleStrategy: idle,
			}, ac.SendKeepAlive)
			require.NoError(t, err)
			require.Positive(t, pos)
			require.Equal(t, tc.wantSeqCalls, seqPub.calls)
			require.Equal(t, tc.wantDoWork, *doWorkCount)
			require.Zero(t, idle.totalIdleCalls)
			require.Equal(t, 1, idle.resetCalls)
		})
	}
}

func TestIngressOfferUntilPositive_terminalNoRetry(t *testing.T) {
	tests := []struct {
		name string
		code int64
	}{
		{name: "closed", code: aeron.Closed},
		{name: "maxPositionExceeded", code: aeron.MaxPositionExceeded},
		{name: "unsupportedMessageLength", code: aeron.UnsupportedMessageLength},
		{name: "notConnected", code: aeron.NotConnected},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ac, seqPub := clusterWithSequentialIngress(t, tc.code)

			_, err := ac.IngressOfferUntilPositive(
				t.Context(),
				IngressOfferConfig{},
				ac.SendKeepAlive,
			)
			requireIngressTerminal(t, err, tc.code)
			require.Equal(t, 1, seqPub.calls)
		})
	}
}

// Connect fixtures use a fixed nanoClock that never advances; a real 1ms timeout would
// spin forever.
//
// Advancing time inside offerFn after each BackPressured preserves the timeout oracle
// while still exercising IdleStrategy pacing and defer Reset.
func TestIngressOfferUntilPositive_timeout(t *testing.T) {
	t.Run("singleAdvance", func(t *testing.T) {
		idle := &recordingIdleStrategy{}
		ac, nowNs := clusterWithAtomicNanoClock(t, 5_000_000_000, time.Millisecond)
		doWorkCount := setupIngressRetryDoWorkRecorder(t)
		var offerCalls int
		offerFn := func() int64 {
			offerCalls++
			nowNs.Add(int64(time.Millisecond))
			return aeron.BackPressured
		}

		_, err := ac.IngressOfferUntilPositive(t.Context(), IngressOfferConfig{
			IdleStrategy: idle,
		}, offerFn)
		requireIngressTimeout(t, err, aeron.BackPressured, time.Millisecond)
		require.Equal(t, 1, offerCalls)
		require.Equal(t, 2, *doWorkCount)
		require.Positive(t, idle.totalIdleCalls)
		require.Zero(t, idle.idleCalls)
		require.Equal(t, 1, idle.resetCalls)
	})

	t.Run("multiIteration", func(t *testing.T) {
		const timeout = time.Millisecond
		stepNs := timeout.Nanoseconds() / 4

		idle := &recordingIdleStrategy{}
		ac, nowNs := clusterWithAtomicNanoClock(t, 5_000_000_000, timeout)
		doWorkCount := setupIngressRetryDoWorkRecorder(t)
		var offerCalls int
		offerFn := func() int64 {
			offerCalls++
			nowNs.Add(stepNs)
			return aeron.BackPressured
		}

		_, err := ac.IngressOfferUntilPositive(t.Context(), IngressOfferConfig{
			IdleStrategy: idle,
		}, offerFn)
		requireIngressTimeout(t, err, aeron.BackPressured, timeout)
		require.Equal(t, 4, offerCalls)
		require.Equal(t, 8, *doWorkCount)
		require.Positive(t, idle.totalIdleCalls)
		require.Equal(t, 1, idle.resetCalls)
	})
}

func TestIngressOfferUntilPositive_cfgNanoClockOverride(t *testing.T) {
	const startNs int64 = 1_000_000_000
	const frozenNs int64 = 42_000_000_000
	var cfgNowNs atomic.Int64
	cfgNowNs.Store(startNs)

	idle := &recordingIdleStrategy{}
	ac := newAeronClusterFromConnectFixture(t, func(result *ConnectResult) {
		mutateConnectConfig(result, func(cfg *Config) {
			cfg.AeronConfig.NanoClock = func() int64 { return frozenNs }
			cfg.IngressRetryTimeout = time.Millisecond
		})
	})
	doWorkCount := setupIngressRetryDoWorkRecorder(t)
	offerFn := func() int64 {
		cfgNowNs.Add(int64(time.Millisecond))
		return aeron.BackPressured
	}

	_, err := ac.IngressOfferUntilPositive(t.Context(), IngressOfferConfig{
		IdleStrategy: idle,
		NanoClock:    func() int64 { return cfgNowNs.Load() },
	}, offerFn)
	requireIngressTimeout(t, err, aeron.BackPressured, time.Millisecond)
	require.Equal(t, 2, *doWorkCount)
	require.Positive(t, idle.totalIdleCalls)
	require.Equal(t, 1, idle.resetCalls)
}

func TestIngressOfferUntilPositive_timeoutZeroUsesDefault(t *testing.T) {
	step := DefaultIngressRetryTimeout.Nanoseconds() / 2
	ac, nowNs := clusterWithAtomicNanoClock(t, 3_000_000_000, DefaultIngressRetryTimeout)
	var offerCalls int
	offerFn := func() int64 {
		offerCalls++
		nowNs.Add(step)
		return aeron.BackPressured
	}

	_, err := ac.IngressOfferUntilPositive(t.Context(), IngressOfferConfig{}, offerFn)
	requireIngressTimeout(t, err, aeron.BackPressured, DefaultIngressRetryTimeout)
	require.Equal(t, 2, offerCalls)
}

func TestIngressOfferUntilPositive_expiredDeadlineAttemptsOnce(t *testing.T) {
	const startNs int64 = 1_000_000_000
	var clockReads int
	clockFn := func() int64 {
		clockReads++
		if clockReads == 1 {
			return startNs
		}
		return startNs + int64(time.Hour)
	}
	ac := newAeronClusterFromConnectFixture(t, func(result *ConnectResult) {
		mutateConnectConfig(result, func(cfg *Config) {
			cfg.IngressRetryTimeout = time.Millisecond
		})
	})
	var offerCalls int
	offerFn := func() int64 {
		offerCalls++
		return aeron.BackPressured
	}

	_, err := ac.IngressOfferUntilPositive(t.Context(), IngressOfferConfig{
		NanoClock: clockFn,
	}, offerFn)
	requireIngressTimeout(t, err, aeron.BackPressured, time.Millisecond)
	require.Equal(t, 1, offerCalls)
}

func TestIngressOfferUntilPositive_nilGuards(t *testing.T) {
	ac := newAeronClusterFromConnectFixture(t, nil)
	var offerCalls int
	offerFn := func() int64 {
		offerCalls++
		return 0
	}

	tests := []struct {
		name    string
		session *AeronCluster
		offerFn func() int64
		wantErr error
	}{
		{
			name:    "nilAeronClient",
			session: closedOwnedNilAeronCluster(t, nil),
			offerFn: offerFn,
			wantErr: errNilIngressAeronClient,
		},
		{
			name:    "nilSession",
			session: nil,
			offerFn: offerFn,
			wantErr: errNilIngressSession,
		},
		{
			name:    "nilOfferFn",
			session: ac,
			offerFn: nil,
			wantErr: errNilIngressOfferFn,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pos, err := ingressOfferUntilPositive(
				t.Context(),
				tc.session,
				IngressOfferConfig{},
				tc.offerFn,
			)
			require.Zero(t, pos)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
	require.Zero(t, offerCalls)
}

// Simulates post-PollEgress terminal NotConnected on the second offer (BP then NC).
//
// Fixed NanoClock never advances; if the helper kept retrying BackPressured it would
// spin until test timeout rather than return NotConnected.
func TestIngressOfferUntilPositive_terminalAfterRetriableRetry(t *testing.T) {
	const startNs int64 = 9_000_000_000
	ac, nowNs := clusterWithAtomicNanoClock(t, startNs, DefaultIngressRetryTimeout)
	var offerCalls int
	offerFn := func() int64 {
		offerCalls++
		if offerCalls == 1 {
			return aeron.BackPressured
		}
		return aeron.NotConnected
	}

	_, err := ac.IngressOfferUntilPositive(t.Context(), IngressOfferConfig{}, offerFn)
	requireIngressTerminal(t, err, aeron.NotConnected)
	require.Equal(t, 2, offerCalls)
	require.Equal(t, startNs, nowNs.Load(), "returned on terminal code, not deadline expiry")
}

func TestIngressOfferUntilPositive_terminalAfterCancel(t *testing.T) {
	ac := newAeronClusterFromConnectFixture(t, nil)

	cancelCtx, cancel := context.WithCancel(t.Context())
	pos, err := ac.IngressOfferUntilPositive(cancelCtx, IngressOfferConfig{}, func() int64 {
		cancel()
		return aeron.Closed
	})
	require.Zero(t, pos)
	require.ErrorIs(t, err, context.Canceled)
}

func TestIngressOfferUntilPositive_usesConfigIdleStrategy(t *testing.T) {
	idle := &recordingIdleStrategy{}
	var seqPub *sequentialOfferResultIngressPublication
	ac := newAeronClusterFromConnectFixture(t, func(result *ConnectResult) {
		result.Config.IdleStrategy = idle
		seqPub = withSequentialOfferResultIngress(
			t,
			result,
			aeron.BackPressured,
			aeron.BackPressured,
			100,
		)
	})

	pos, err := ac.IngressOfferUntilPositive(
		t.Context(),
		IngressOfferConfig{},
		ac.SendKeepAlive,
	)

	require.NoError(t, err)
	require.Positive(t, pos)
	require.Equal(t, 3, seqPub.calls)
	require.Equal(t, 2, idle.totalIdleCalls)
	require.Equal(t, 1, idle.resetCalls)
}

func TestSendKeepAliveUntilPositive_delegates(t *testing.T) {
	ac, seqPub := clusterWithSequentialIngress(t, aeron.AdminAction, 100)

	pos, err := ac.SendKeepAliveUntilPositive(t.Context(), IngressOfferConfig{})
	require.NoError(t, err)
	require.Positive(t, pos)
	require.Equal(t, 2, seqPub.calls)
}

func TestAeronCluster_IngressOfferUntilPositive_closedNilAeron(t *testing.T) {
	t.Run("offerFn", func(t *testing.T) {
		ac := closedOwnedNilAeronCluster(t, nil)
		var offerCalls int

		pos, err := ac.IngressOfferUntilPositive(t.Context(), IngressOfferConfig{}, func() int64 {
			offerCalls++
			return 1
		})

		require.ErrorIs(t, err, errNilIngressAeronClient)
		require.Zero(t, pos)
		require.Zero(t, offerCalls)
	})

	t.Run("sendKeepAlive", func(t *testing.T) {
		var seqPub *sequentialOfferResultIngressPublication
		ac := closedOwnedNilAeronCluster(t, func(result *ConnectResult) {
			seqPub = withSequentialOfferResultIngress(t, result, 100)
		})

		callsBefore := seqPub.calls
		pos, err := ac.SendKeepAliveUntilPositive(t.Context(), IngressOfferConfig{})

		require.ErrorIs(t, err, errNilIngressAeronClient)
		require.Zero(t, pos)
		require.Equal(t, callsBefore, seqPub.calls)
	})

	t.Run("borrowedClient", func(t *testing.T) {
		var seqPub *sequentialOfferResultIngressPublication
		ac := newAeronClusterFromConnectFixture(t, func(result *ConnectResult) {
			seqPub = withSequentialOfferResultIngress(t, result, 100)
		})
		require.NoError(t, ac.Close())
		require.NotNil(t, ac.Aeron())
		require.True(t, ac.IsClosed())

		pos, err := ac.SendKeepAliveUntilPositive(t.Context(), IngressOfferConfig{})
		requireIngressTerminal(t, err, aeron.Closed)
		require.Zero(t, pos)
		require.Equal(t, 1, seqPub.calls)
	})
}

func TestIngressOfferUntilPositive_contextCanceled(t *testing.T) {
	t.Run("preCanceled", func(t *testing.T) {
		idle := &recordingIdleStrategy{}
		ac, _ := clusterWithAtomicNanoClock(t, 5_000_000_000, DefaultIngressRetryTimeout)

		cancelCtx, cancel := context.WithCancel(t.Context())
		cancel()

		var offerCalls int
		pos, err := ac.IngressOfferUntilPositive(cancelCtx, IngressOfferConfig{
			IdleStrategy: idle,
		}, func() int64 {
			offerCalls++
			return 0
		})
		require.Zero(t, pos)
		require.Zero(t, offerCalls)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, idle.resetCalls)
	})

	t.Run("midLoop", func(t *testing.T) {
		ac, nowNs := clusterWithAtomicNanoClock(t, 5_000_000_000, DefaultIngressRetryTimeout)
		doWorkCount := setupIngressRetryDoWorkRecorder(t)

		cancelCtx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		var offerCalls int
		offerFn := func() int64 {
			offerCalls++
			nowNs.Add(int64(time.Millisecond))
			if offerCalls >= 2 {
				cancel()
			}
			return aeron.BackPressured
		}

		pos, err := ac.IngressOfferUntilPositive(cancelCtx, IngressOfferConfig{}, offerFn)
		require.Zero(t, pos)
		require.Equal(t, 2, offerCalls)
		require.Equal(t, 4, *doWorkCount)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestIngressOfferUntilPositive_contextDeadline(t *testing.T) {
	t.Run("preExpired", func(t *testing.T) {
		idle := &recordingIdleStrategy{}
		ac, _ := clusterWithAtomicNanoClock(t, 5_000_000_000, DefaultIngressRetryTimeout)

		deadlineCtx, cancel := context.WithDeadline(
			t.Context(),
			time.Now().Add(-time.Second),
		)
		defer cancel()

		var offerCalls int
		pos, err := ac.IngressOfferUntilPositive(deadlineCtx, IngressOfferConfig{
			IdleStrategy: idle,
		}, func() int64 {
			offerCalls++
			return aeron.BackPressured
		})
		require.Zero(t, pos)
		require.Zero(t, offerCalls)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 1, idle.resetCalls)
	})

	t.Run("ctxDeadlineSooner", func(t *testing.T) {
		ac := newAeronClusterFromConnectFixture(t, nil)

		// A shorter deadline can expire before the first offer on a loaded runner.
		deadlineCtx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()

		var offerCalls int
		pos, err := ac.IngressOfferUntilPositive(deadlineCtx, IngressOfferConfig{
			NanoClock: func() int64 {
				return time.Now().UnixNano()
			},
		}, func() int64 {
			offerCalls++

			// The offer must wait for ctx.Done. The wait returns after ctx.Err() is set,
			// before the retry loop expiry check.
			<-deadlineCtx.Done()
			return aeron.BackPressured
		})

		require.Zero(t, pos)
		require.Equal(t, 1, offerCalls)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("cfgTimeoutSooner", func(t *testing.T) {
		const timeout = time.Millisecond
		stepNs := timeout.Nanoseconds() / 4

		ac, nowNs := clusterWithAtomicNanoClock(t, 5_000_000_000, timeout)
		deadlineCtx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		var offerCalls int
		offerFn := func() int64 {
			offerCalls++
			nowNs.Add(stepNs)
			return aeron.BackPressured
		}

		_, err := ac.IngressOfferUntilPositive(deadlineCtx, IngressOfferConfig{}, offerFn)
		requireIngressTimeout(t, err, aeron.BackPressured, timeout)
		require.Equal(t, 4, offerCalls)
	})
}
