//go:build testhooks

package archive

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/idlestrategy"
)

type sequentialOfferResultControlPublication struct {
	results []int64
	calls   int
}

func (p *sequentialOfferResultControlPublication) Offer([]byte) int64 {
	if p.calls >= len(p.results) {
		return p.results[len(p.results)-1]
	}
	result := p.results[p.calls]
	p.calls++
	return result
}

func newSequentialOfferResultControlPublication(
	t *testing.T,
	results ...int64,
) *sequentialOfferResultControlPublication {
	t.Helper()

	require.NotEmpty(
		t,
		results,
		"sequentialOfferResultControlPublication requires at least one result",
	)

	return &sequentialOfferResultControlPublication{results: results}
}

func controlOfferRetryHarnessWithSequentialOffers(
	t *testing.T,
	results ...int64,
) (*aeron.AsyncConnectTestHarness, *sequentialOfferResultControlPublication) {
	t.Helper()

	return aeron.NewAsyncConnectTestHarness(t),
		newSequentialOfferResultControlPublication(t, results...)
}

func controlOfferRetryHarnessWithNanoClock(
	t *testing.T,
	startNs int64,
) (*aeron.AsyncConnectTestHarness, *atomic.Int64) {
	t.Helper()

	nowNs := &atomic.Int64{}
	nowNs.Store(startNs)
	return aeron.NewAsyncConnectTestHarness(t), nowNs
}

// recordingControlIdleStrategy counts Idle/Reset invocations (AdminAction + timeout oracle).
//
// totalIdleCalls is cumulative across Reset; idleCalls is the current epoch.
type recordingControlIdleStrategy struct {
	idleCalls      int
	totalIdleCalls int
	resetCalls     int
}

func (r *recordingControlIdleStrategy) Idle(_ int) {
	r.idleCalls++
	r.totalIdleCalls++
}

func (r *recordingControlIdleStrategy) Reset() {
	r.resetCalls++
	r.idleCalls = 0
}

// setupControlOfferRetryDoWorkRecorder installs a per-client DoWork counter.
func setupControlOfferRetryDoWorkRecorder(t *testing.T, client *aeron.Client) *int {
	t.Helper()

	var count int
	SetControlOfferRetryDoWorkRecorderForTest(t, client, func() { count++ })
	return &count
}

func requireControlTerminal(tb requireTB, err error, wantStatus int64) {
	tb.Helper()

	var terminal *ControlTerminalError
	require.ErrorAs(tb, err, &terminal)
	require.Equal(tb, wantStatus, terminal.Status)
}

func requireControlTimeout(tb testing.TB, err error, wantStatus int64, wantTimeout time.Duration) {
	tb.Helper()

	var timeoutErr *ControlOfferTimeoutError
	require.ErrorAs(tb, err, &timeoutErr)
	require.Equal(tb, wantStatus, timeoutErr.Status)
	require.Equal(tb, wantTimeout, timeoutErr.Timeout)
}

func TestControlOfferUntilPositive_backPressuredRetry(t *testing.T) {
	cases := []struct {
		name       string
		background bool
		cfg        func(idle *recordingControlIdleStrategy) ControlOfferConfig
		assert     func(
			t *testing.T,
			idle *recordingControlIdleStrategy,
			doWorkCount *int,
			seqPub *sequentialOfferResultControlPublication,
		)
	}{
		{
			name: "then_success",
			cfg: func(idle *recordingControlIdleStrategy) ControlOfferConfig {
				return ControlOfferConfig{IdleStrategy: idle}
			},
			assert: func(
				t *testing.T,
				idle *recordingControlIdleStrategy,
				_ *int,
				seqPub *sequentialOfferResultControlPublication,
			) {
				require.Equal(t, 3, seqPub.calls)
				require.Equal(t, 2, idle.totalIdleCalls)
				require.Equal(t, 1, idle.resetCalls)
			},
		},
		{
			name: "do_work_count",
			cfg: func(*recordingControlIdleStrategy) ControlOfferConfig {
				return ControlOfferConfig{}
			},
			assert: func(
				t *testing.T,
				_ *recordingControlIdleStrategy,
				doWorkCount *int,
				_ *sequentialOfferResultControlPublication,
			) {
				require.Equal(t, 5, *doWorkCount)
			},
		},
		{
			name:       "background_do_work_count",
			background: true,
			cfg: func(*recordingControlIdleStrategy) ControlOfferConfig {
				return ControlOfferConfig{}
			},
			assert: func(
				t *testing.T,
				_ *recordingControlIdleStrategy,
				doWorkCount *int,
				_ *sequentialOfferResultControlPublication,
			) {
				require.Zero(t, *doWorkCount)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			harness, seqPub := controlOfferRetryHarnessWithSequentialOffers(
				t,
				aeron.BackPressured,
				aeron.BackPressured,
				100,
			)

			if tc.background {
				aeron.StartBackgroundConductorForTest(
					harness.Client,
					idlestrategy.NewSleepingMillis(time.Nanosecond),
				)
				t.Cleanup(func() { _ = harness.Client.Close() })
			}

			idle := &recordingControlIdleStrategy{}
			doWorkCount := setupControlOfferRetryDoWorkRecorder(t, harness.Client)

			pos, err := controlOfferUntilPositive(
				t.Context(),
				harness.Client,
				Config{}.Normalize(),
				tc.cfg(idle),
				func() int64 {
					return seqPub.Offer(nil)
				},
			)
			require.NoError(t, err)
			require.Positive(t, pos)
			tc.assert(t, idle, doWorkCount, seqPub)
		})
	}
}

func TestControlOfferUntilPositive_immediateSuccess(t *testing.T) {
	idle := &recordingControlIdleStrategy{}
	harness, seqPub := controlOfferRetryHarnessWithSequentialOffers(t, 100)
	doWorkCount := setupControlOfferRetryDoWorkRecorder(t, harness.Client)

	pos, err := controlOfferUntilPositive(
		t.Context(),
		harness.Client,
		Config{}.Normalize(),
		ControlOfferConfig{IdleStrategy: idle},
		func() int64 { return seqPub.Offer(nil) },
	)
	require.NoError(t, err)
	require.Equal(t, int64(100), pos)
	require.Equal(t, 1, seqPub.calls)
	require.Equal(t, 1, *doWorkCount)
	require.Zero(t, idle.totalIdleCalls)
	require.Equal(t, 1, idle.resetCalls)
}

func TestControlOfferUntilPositive_pollFnOnRetriable(t *testing.T) {
	harness, seqPub := controlOfferRetryHarnessWithSequentialOffers(t, aeron.BackPressured, 100)
	var pollCalls int

	pos, err := controlOfferUntilPositive(
		t.Context(),
		harness.Client,
		Config{}.Normalize(),
		ControlOfferConfig{
			PollFn: func() int {
				pollCalls++
				return 0
			},
		},
		func() int64 { return seqPub.Offer(nil) },
	)
	require.NoError(t, err)
	require.Positive(t, pos)
	require.Equal(t, 1, pollCalls)
}

func TestControlOfferUntilPositive_adminActionThenSuccess(t *testing.T) {
	idle := &recordingControlIdleStrategy{}
	harness, seqPub := controlOfferRetryHarnessWithSequentialOffers(t, aeron.AdminAction, 100)

	pos, err := controlOfferUntilPositive(
		t.Context(),
		harness.Client,
		Config{}.Normalize(),
		ControlOfferConfig{IdleStrategy: idle},
		func() int64 { return seqPub.Offer(nil) },
	)
	require.NoError(t, err)
	require.Positive(t, pos)
	require.Equal(t, 2, seqPub.calls)
	require.Equal(t, 1, idle.totalIdleCalls)
}

func TestControlOfferUntilPositive_terminalNoRetry(t *testing.T) {
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
			harness, seqPub := controlOfferRetryHarnessWithSequentialOffers(t, tc.code)

			_, err := controlOfferUntilPositive(
				t.Context(),
				harness.Client,
				Config{}.Normalize(),
				ControlOfferConfig{},
				func() int64 { return seqPub.Offer(nil) },
			)
			requireControlTerminal(t, err, tc.code)
			require.Equal(t, 1, seqPub.calls)
		})
	}
}

// With a frozen clock the attempt cap, not the deadline, would end the loop. Each
// offerFn call advances the clock, so the deadline ends the loop. IdleStrategy pacing
// and defer Reset still run.
func TestControlOfferUntilPositive_timeout(t *testing.T) {
	t.Run("singleAdvance", func(t *testing.T) {
		idle := &recordingControlIdleStrategy{}
		harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
		doWorkCount := setupControlOfferRetryDoWorkRecorder(t, harness.Client)
		var offerCalls int
		offerFn := func() int64 {
			offerCalls++
			nowNs.Add(int64(time.Millisecond))
			return aeron.BackPressured
		}

		_, err := controlOfferUntilPositive(
			t.Context(),
			harness.Client,
			Config{ControlOfferRetryTimeout: time.Millisecond}.Normalize(),
			ControlOfferConfig{
				IdleStrategy: idle,
				NanoClock:    func() int64 { return nowNs.Load() },
			},
			offerFn,
		)
		requireControlTimeout(t, err, aeron.BackPressured, time.Millisecond)
		require.Equal(t, 1, offerCalls)
		require.Equal(t, 2, *doWorkCount)
		require.Positive(t, idle.totalIdleCalls)
		require.Zero(t, idle.idleCalls)
		require.Equal(t, 1, idle.resetCalls)
	})

	t.Run("multiIteration", func(t *testing.T) {
		const timeout = time.Millisecond
		stepNs := timeout.Nanoseconds() / 4

		idle := &recordingControlIdleStrategy{}
		harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
		doWorkCount := setupControlOfferRetryDoWorkRecorder(t, harness.Client)
		var offerCalls int
		offerFn := func() int64 {
			offerCalls++
			nowNs.Add(stepNs)
			return aeron.BackPressured
		}

		_, err := controlOfferUntilPositive(
			t.Context(),
			harness.Client,
			Config{
				ControlOfferRetryTimeout: timeout,
				MessageRetryAttempts:     5,
			}.Normalize(),
			ControlOfferConfig{
				IdleStrategy: idle,
				NanoClock:    func() int64 { return nowNs.Load() },
			},
			offerFn,
		)
		requireControlTimeout(t, err, aeron.BackPressured, timeout)
		require.Equal(t, 4, offerCalls)
		require.Equal(t, 8, *doWorkCount)
		require.Positive(t, idle.totalIdleCalls)
		require.Equal(t, 1, idle.resetCalls)
	})
}

// The clock steps 1 ms per offer under a 10 ms timeout. The deadline ends the loop at
// offer 10 unless the attempt cap ends it first. A cap of 10 expires on the same
// offer. The cap wins.
func TestControlOfferUntilPositive_attemptsExhausted(t *testing.T) {
	const timeout = 10 * time.Millisecond

	tests := []struct {
		name          string
		attempts      int
		results       []int64
		wantOffers    int
		wantPollCalls int
		wantIdleCalls int
		wantDoWork    int
		wantPosition  int64
		wantAttempts  *ControlOfferAttemptsError
		wantTimeout   bool
	}{
		{
			name:          "backPressuredDefaultAttempts",
			results:       []int64{aeron.BackPressured},
			wantOffers:    3,
			wantPollCalls: 2,
			wantIdleCalls: 2,
			wantDoWork:    5,
			wantAttempts:  &ControlOfferAttemptsError{Status: aeron.BackPressured, Attempts: 3},
		},
		{
			name:          "adminActionDefaultAttempts",
			results:       []int64{aeron.AdminAction},
			wantOffers:    3,
			wantPollCalls: 2,
			wantIdleCalls: 2,
			wantDoWork:    3,
			wantAttempts:  &ControlOfferAttemptsError{Status: aeron.AdminAction, Attempts: 3},
		},
		{
			name:         "attemptsOne",
			attempts:     1,
			results:      []int64{aeron.BackPressured},
			wantOffers:   1,
			wantDoWork:   1,
			wantAttempts: &ControlOfferAttemptsError{Status: aeron.BackPressured, Attempts: 1},
		},
		{
			name:         "attemptsNegative",
			attempts:     -1,
			results:      []int64{aeron.BackPressured},
			wantOffers:   1,
			wantDoWork:   1,
			wantAttempts: &ControlOfferAttemptsError{Status: aeron.BackPressured, Attempts: 1},
		},
		{
			name:          "successOnLastAttempt",
			results:       []int64{aeron.BackPressured, aeron.BackPressured, 100},
			wantOffers:    3,
			wantPollCalls: 2,
			wantIdleCalls: 2,
			wantDoWork:    5,
			wantPosition:  100,
		},
		{
			name:          "deadlineBeforeAttempts",
			attempts:      20,
			results:       []int64{aeron.BackPressured},
			wantOffers:    10,
			wantPollCalls: 10,
			wantIdleCalls: 10,
			wantDoWork:    20,
			wantTimeout:   true,
		},
		{
			name:          "attemptsTiesDeadline",
			attempts:      10,
			results:       []int64{aeron.BackPressured},
			wantOffers:    10,
			wantPollCalls: 9,
			wantIdleCalls: 9,
			wantDoWork:    19,
			wantAttempts:  &ControlOfferAttemptsError{Status: aeron.BackPressured, Attempts: 10},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
			seqPub := newSequentialOfferResultControlPublication(t, tc.results...)
			idle := &recordingControlIdleStrategy{}
			doWorkCount := setupControlOfferRetryDoWorkRecorder(t, harness.Client)
			var offerCalls, pollCalls int
			offerFn := func() int64 {
				offerCalls++
				nowNs.Add(int64(time.Millisecond))
				return seqPub.Offer(nil)
			}

			pos, err := controlOfferUntilPositive(
				t.Context(),
				harness.Client,
				Config{
					ControlOfferRetryTimeout: timeout,
					MessageRetryAttempts:     tc.attempts,
				},
				ControlOfferConfig{
					IdleStrategy: idle,
					NanoClock:    func() int64 { return nowNs.Load() },
					PollFn: func() int {
						pollCalls++
						return 0
					},
				},
				offerFn,
			)
			require.Equal(t, tc.wantOffers, offerCalls)
			require.Equal(t, tc.wantPollCalls, pollCalls)
			require.Equal(t, tc.wantIdleCalls, idle.totalIdleCalls)
			require.Equal(t, 1, idle.resetCalls)
			require.Equal(t, tc.wantDoWork, *doWorkCount)
			require.Equal(t, tc.wantPosition, pos)
			switch {
			case tc.wantAttempts != nil:
				var attemptsErr *ControlOfferAttemptsError
				require.ErrorAs(t, err, &attemptsErr)
				require.Equal(t, tc.wantAttempts, attemptsErr)
			case tc.wantTimeout:
				requireControlTimeout(t, err, aeron.BackPressured, timeout)
			default:
				require.NoError(t, err)
			}
		})
	}
}

func TestControlOfferUntilPositive_timeoutZeroUsesDefault(t *testing.T) {
	harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 3_000_000_000)
	step := DefaultControlOfferRetryTimeout.Nanoseconds() / 2
	var offerCalls int
	offerFn := func() int64 {
		offerCalls++
		nowNs.Add(step)
		return aeron.BackPressured
	}

	_, err := controlOfferUntilPositive(
		t.Context(),
		harness.Client,
		Config{},
		ControlOfferConfig{
			NanoClock: func() int64 { return nowNs.Load() },
		},
		offerFn,
	)
	requireControlTimeout(t, err, aeron.BackPressured, DefaultControlOfferRetryTimeout)
	require.Equal(t, 2, offerCalls)
}

func TestControlOfferUntilPositive_expiredDeadlineAttemptsOnce(t *testing.T) {
	const startNs int64 = 1_000_000_000
	var clockReads int
	clockFn := func() int64 {
		clockReads++
		if clockReads == 1 {
			return startNs
		}
		return startNs + int64(time.Hour)
	}
	harness := aeron.NewAsyncConnectTestHarness(t)
	var offerCalls int
	offerFn := func() int64 {
		offerCalls++
		return aeron.BackPressured
	}

	_, err := controlOfferUntilPositive(
		t.Context(),
		harness.Client,
		Config{ControlOfferRetryTimeout: time.Millisecond}.Normalize(),
		ControlOfferConfig{
			NanoClock: clockFn,
		},
		offerFn,
	)
	requireControlTimeout(t, err, aeron.BackPressured, time.Millisecond)
	require.Equal(t, 1, offerCalls)
}

func TestControlOfferUntilPositive_cfgNanoClockOverride(t *testing.T) {
	const startNs int64 = 1_000_000_000
	const frozenNs int64 = 42_000_000_000
	var cfgNowNs atomic.Int64
	cfgNowNs.Store(startNs)

	idle := &recordingControlIdleStrategy{}
	harness := aeron.NewAsyncConnectTestHarness(t)
	doWorkCount := setupControlOfferRetryDoWorkRecorder(t, harness.Client)
	offerFn := func() int64 {
		cfgNowNs.Add(int64(time.Millisecond))
		return aeron.BackPressured
	}

	_, err := controlOfferUntilPositive(
		t.Context(),
		harness.Client,
		Config{
			ControlOfferRetryTimeout: time.Millisecond,
			AeronConfig: aeron.Config{
				NanoClock: func() int64 { return frozenNs },
			},
		}.Normalize(),
		ControlOfferConfig{
			IdleStrategy: idle,
			NanoClock:    func() int64 { return cfgNowNs.Load() },
		},
		offerFn,
	)
	requireControlTimeout(t, err, aeron.BackPressured, time.Millisecond)
	require.Equal(t, 2, *doWorkCount)
	require.Positive(t, idle.totalIdleCalls)
	require.Equal(t, 1, idle.resetCalls)
}

func TestControlOfferUntilPositive_sessionConfigFallback(t *testing.T) {
	const timeout = time.Millisecond
	harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 7_000_000_000)
	sessionIdle := &recordingControlIdleStrategy{}
	stepNs := timeout.Nanoseconds() / 4
	var offerCalls int
	offerFn := func() int64 {
		offerCalls++
		nowNs.Add(stepNs)
		return aeron.BackPressured
	}

	_, err := controlOfferUntilPositive(
		t.Context(),
		harness.Client,
		Config{
			ControlOfferRetryTimeout: timeout,
			MessageRetryAttempts:     5,
			IdleStrategy:             sessionIdle,
			AeronConfig: aeron.Config{
				NanoClock: func() int64 { return nowNs.Load() },
			},
		},
		ControlOfferConfig{},
		offerFn,
	)
	requireControlTimeout(t, err, aeron.BackPressured, timeout)
	require.Equal(t, 4, offerCalls, "empty cfg deadline runs on session AeronConfig.NanoClock")
	require.Equal(t, 4, sessionIdle.totalIdleCalls, "empty cfg paces via session Config.IdleStrategy")
	require.Equal(t, 1, sessionIdle.resetCalls)
}

func TestControlOfferUntilPositive_terminalAfterRetriableRetry(t *testing.T) {
	const startNs int64 = 9_000_000_000
	harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, startNs)
	var offerCalls int
	offerFn := func() int64 {
		offerCalls++
		if offerCalls == 1 {
			return aeron.BackPressured
		}
		return aeron.NotConnected
	}

	_, err := controlOfferUntilPositive(
		t.Context(),
		harness.Client,
		Config{}.Normalize(),
		ControlOfferConfig{},
		offerFn,
	)
	requireControlTerminal(t, err, aeron.NotConnected)
	require.Equal(t, 2, offerCalls)
	require.Equal(t, startNs, nowNs.Load(), "returned on terminal code, not deadline expiry")
}

func TestControlOfferUntilPositive_terminalAfterCancel(t *testing.T) {
	harness := aeron.NewAsyncConnectTestHarness(t)

	cancelCtx, cancel := context.WithCancel(t.Context())
	pos, err := controlOfferUntilPositive(
		cancelCtx,
		harness.Client,
		Config{}.Normalize(),
		ControlOfferConfig{},
		func() int64 {
			cancel()
			return aeron.Closed
		},
	)
	require.Zero(t, pos)
	require.ErrorIs(t, err, context.Canceled)
}

func TestControlOfferUntilPositive_nilGuards(t *testing.T) {
	harness := aeron.NewAsyncConnectTestHarness(t)
	var offerCalls int
	offerFn := func() int64 {
		offerCalls++
		return 0
	}

	tests := []struct {
		name        string
		aeronClient *aeron.Client
		offerFn     func() int64
		wantErr     error
	}{
		{name: "nilAeronClient", aeronClient: nil, offerFn: offerFn, wantErr: errNilControlAeronClient},
		{name: "nilOfferFn", aeronClient: harness.Client, offerFn: nil, wantErr: errNilControlOfferFn},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pos, err := controlOfferUntilPositive(
				t.Context(),
				tc.aeronClient,
				Config{}.Normalize(),
				ControlOfferConfig{},
				tc.offerFn,
			)
			require.Zero(t, pos)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
	require.Zero(t, offerCalls)
}

func TestControlOfferUntilPositive_contextCanceled(t *testing.T) {
	t.Run("preCanceled", func(t *testing.T) {
		idle := &recordingControlIdleStrategy{}
		harness := aeron.NewAsyncConnectTestHarness(t)

		cancelCtx, cancel := context.WithCancel(t.Context())
		cancel()

		var offerCalls int
		pos, err := controlOfferUntilPositive(
			cancelCtx,
			harness.Client,
			Config{}.Normalize(),
			ControlOfferConfig{IdleStrategy: idle},
			func() int64 {
				offerCalls++
				return 0
			},
		)
		require.Zero(t, pos)
		require.Zero(t, offerCalls)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, idle.resetCalls)
	})

	t.Run("midLoop", func(t *testing.T) {
		harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
		doWorkCount := setupControlOfferRetryDoWorkRecorder(t, harness.Client)

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

		pos, err := controlOfferUntilPositive(
			cancelCtx,
			harness.Client,
			Config{}.Normalize(),
			ControlOfferConfig{},
			offerFn,
		)
		require.Zero(t, pos)
		require.Equal(t, 2, offerCalls)
		require.Equal(t, 4, *doWorkCount)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestControlOfferUntilPositive_contextDeadline(t *testing.T) {
	t.Run("preExpired", func(t *testing.T) {
		idle := &recordingControlIdleStrategy{}
		harness := aeron.NewAsyncConnectTestHarness(t)

		deadlineCtx, cancel := context.WithDeadline(
			t.Context(),
			time.Now().Add(-time.Second),
		)
		defer cancel()

		var offerCalls int
		pos, err := controlOfferUntilPositive(
			deadlineCtx,
			harness.Client,
			Config{}.Normalize(),
			ControlOfferConfig{IdleStrategy: idle},
			func() int64 {
				offerCalls++
				return aeron.BackPressured
			},
		)
		require.Zero(t, pos)
		require.Zero(t, offerCalls)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 1, idle.resetCalls)
	})

	t.Run("ctxDeadlineSooner", func(t *testing.T) {
		harness := aeron.NewAsyncConnectTestHarness(t)

		// A shorter deadline can expire before the first offer on a loaded runner.
		deadlineCtx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()

		var offerCalls int
		pos, err := controlOfferUntilPositive(
			deadlineCtx,
			harness.Client,
			Config{}.Normalize(),
			ControlOfferConfig{},
			func() int64 {
				offerCalls++

				// The offer must wait for ctx.Done so ctx.Err() is set before the retry loop
				// expiry check.
				//
				// The loop deadline uses a NanoClock sample taken before ctx.Deadline() is read.
				// The loop deadline is earlier than the ctx deadline. A BackPressured return
				// without this wait can reach the expiry check while ctx is still active.
				// PreferContextError then returns ControlOfferTimeoutError instead of
				// DeadlineExceeded.
				<-deadlineCtx.Done()
				return aeron.BackPressured
			},
		)

		require.Zero(t, pos)
		require.Equal(t, 1, offerCalls)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("cfgTimeoutSooner", func(t *testing.T) {
		const timeout = time.Millisecond
		stepNs := timeout.Nanoseconds() / 4

		harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
		deadlineCtx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		var offerCalls int
		offerFn := func() int64 {
			offerCalls++
			nowNs.Add(stepNs)
			return aeron.BackPressured
		}

		_, err := controlOfferUntilPositive(
			deadlineCtx,
			harness.Client,
			Config{
				ControlOfferRetryTimeout: timeout,
				MessageRetryAttempts:     5,
			}.Normalize(),
			ControlOfferConfig{
				NanoClock: func() int64 { return nowNs.Load() },
			},
			offerFn,
		)
		requireControlTimeout(t, err, aeron.BackPressured, timeout)
		require.Equal(t, 4, offerCalls)
	})
}

func TestClient_Config_nilReceiver(t *testing.T) {
	var client *Client
	require.Equal(t, Config{}, client.Config())
}

func TestClient_Config_returnsSessionConfig(t *testing.T) {
	want := Config{ControlOfferRetryTimeout: 3 * time.Second}.Normalize()
	client := newClient(&ConnectResult{Config: want})
	require.Equal(t, want, client.Config())
}

func TestClient_ControlOfferUntilPositive_nilSession(t *testing.T) {
	var client *Client
	var offerCalls int
	pos, err := client.ControlOfferUntilPositive(
		t.Context(),
		ControlOfferConfig{},
		func() int64 {
			offerCalls++
			return 1
		},
	)
	require.ErrorIs(t, err, errNilSession)
	require.Zero(t, pos)
	require.Zero(t, offerCalls)
}

func TestClient_ControlOfferUntilPositive_emptyCfgUsesSessionIdle(t *testing.T) {
	sessionIdle := &recordingControlIdleStrategy{}
	harness, seqPub := controlOfferRetryHarnessWithSequentialOffers(t, aeron.BackPressured, 100)
	archive := newClient(&ConnectResult{
		Aeron: harness.Client,
		Config: Config{
			IdleStrategy: sessionIdle,
		}.Normalize(),
	})

	pos, err := archive.ControlOfferUntilPositive(
		t.Context(),
		ControlOfferConfig{},
		func() int64 { return seqPub.Offer(nil) },
	)
	require.NoError(t, err)
	require.Positive(t, pos)
	require.Equal(t, 1, sessionIdle.totalIdleCalls, "empty cfg inherits session IdleStrategy")
	require.Equal(t, 1, sessionIdle.resetCalls)
}

func TestClient_ControlOfferUntilPositive_withSessionPoll(t *testing.T) {
	harness, seqPub := controlOfferRetryHarnessWithSequentialOffers(t, aeron.BackPressured, 100)
	archive := newClient(&ConnectResult{
		Aeron:  harness.Client,
		Config: Config{}.Normalize(),
	})
	var pollCalls int
	// Inline poll fn substitutes for archive.Poll so poll invocations are countable;
	// production callers pass archive.Poll or an operation-specific poll wrapper.
	pollFn := func() int {
		pollCalls++
		return 0
	}

	pos, err := archive.ControlOfferUntilPositive(
		t.Context(),
		ControlOfferConfig{PollFn: pollFn},
		func() int64 { return seqPub.Offer(nil) },
	)
	require.NoError(t, err)
	require.Equal(t, int64(100), pos)
	require.Equal(t, 1, pollCalls)
}

func TestClient_ControlOfferUntilPositive_closedSession(t *testing.T) {
	t.Run("ownedNilAeronClient", func(t *testing.T) {
		h := newLifecycleHarness(t)
		archive := h.newClient()
		archive.ownsAeronClient = true
		require.NoError(t, archive.Close())
		require.Nil(t, archive.aeronClient)
		require.True(t, archive.IsClosed())
		var offerCalls int

		pos, err := archive.ControlOfferUntilPositive(
			t.Context(),
			ControlOfferConfig{},
			func() int64 {
				offerCalls++
				return 1
			},
		)
		require.ErrorIs(t, err, errNilControlAeronClient)
		require.Zero(t, pos)
		require.Zero(t, offerCalls)
	})

	t.Run("borrowedClientTerminal", func(t *testing.T) {
		harness, seqPub := controlOfferRetryHarnessWithSequentialOffers(t, aeron.Closed)
		archive := newClient(&ConnectResult{
			Aeron:           harness.Client,
			OwnsAeronClient: false,
			Config:          Config{}.Normalize(),
		})
		require.NoError(t, archive.Close())
		require.NotNil(t, archive.aeronClient)
		require.True(t, archive.IsClosed())

		pos, err := archive.ControlOfferUntilPositive(
			t.Context(),
			ControlOfferConfig{},
			func() int64 { return seqPub.Offer(nil) },
		)
		requireControlTerminal(t, err, aeron.Closed)
		require.Zero(t, pos)
		require.Equal(t, 1, seqPub.calls)
	})
}

func TestControlOfferUntilPositive_transportFault(t *testing.T) {
	faultErr := errors.New("transport fault")

	t.Run("fault_before_first_offer", func(t *testing.T) {
		harness := aeron.NewAsyncConnectTestHarness(t)
		var checks, offers int
		pos, err := controlOfferUntilPositive(
			t.Context(),
			harness.Client,
			Config{},
			ControlOfferConfig{
				transportFault: func() error {
					checks++
					return faultErr
				},
			},
			func() int64 {
				offers++
				return aeron.BackPressured
			},
		)
		require.Zero(t, pos)
		require.ErrorIs(t, err, faultErr)
		require.Equal(t, 1, checks)
		require.Zero(t, offers)
	})

	for _, tc := range []struct {
		name      string
		offer     int64
		moveClock bool
	}{
		{name: "fault_after_back_pressure", offer: aeron.BackPressured},
		{name: "fault_on_deadline_iteration", offer: aeron.BackPressured, moveClock: true},
		{name: "fault_after_admin_action", offer: aeron.AdminAction},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const timeout = time.Millisecond
			harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
			var checks, offers, polls int
			pos, err := controlOfferUntilPositive(
				t.Context(),
				harness.Client,
				Config{ControlOfferRetryTimeout: timeout},
				ControlOfferConfig{
					NanoClock: func() int64 { return nowNs.Load() },
					PollFn: func() int {
						polls++

						if tc.moveClock {
							nowNs.Add(timeout.Nanoseconds())
						}
						return 0
					},
					transportFault: func() error {
						checks++
						if checks >= 2 {
							return faultErr
						}
						return nil
					},
				},
				func() int64 {
					offers++
					return tc.offer
				},
			)
			require.Zero(t, pos)
			require.ErrorIs(t, err, faultErr)
			require.Equal(t, 2, checks)
			require.Equal(t, 1, offers)
			require.Equal(t, 1, polls)
		})
	}

	t.Run("context_error_wins", func(t *testing.T) {
		harness := aeron.NewAsyncConnectTestHarness(t)
		ctx, cancel := context.WithCancel(t.Context())
		var offers int
		pos, err := controlOfferUntilPositive(
			ctx,
			harness.Client,
			Config{},
			ControlOfferConfig{
				transportFault: func() error {
					cancel()
					return faultErr
				},
			},
			func() int64 {
				offers++
				return aeron.BackPressured
			},
		)
		require.Zero(t, pos)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, offers)
	})
}
