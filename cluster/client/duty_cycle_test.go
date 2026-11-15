//go:build testhooks

package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/idlestrategy"
)

func setupSpinOnceOpRecorder(t *testing.T) *egressAdapterOpLog {
	t.Helper()

	recorder := &egressAdapterOpLog{}
	SetSpinOnceOpRecorderForTest(t, func(op string) { recorder.Ops = append(recorder.Ops, op) })
	return recorder
}

func TestSpinOnce_doWorkBeforePollEgress(t *testing.T) {
	tests := []struct {
		name       string
		background bool
		want       []string
	}{
		{name: "invoker", want: []string{"DoWork", "PollEgress"}},
		{name: "background", background: true, want: []string{"PollEgress"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ac := newAeronClusterFromConnectFixture(t, nil)

			if tc.background {
				aeron.StartBackgroundConductorForTest(
					ac.Aeron(),
					idlestrategy.NewSleepingMillis(time.Nanosecond),
				)
				t.Cleanup(func() { _ = ac.Aeron().Close() })
			}

			ops := setupSpinOnceOpRecorder(t)
			ac.spinOnce(nil)

			require.Equal(t, tc.want, ops.Ops)
		})
	}
}

func TestSpinOnce_postClose(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*ConnectResult)
		aeronNil bool
		wantOps  []string
	}{
		{
			name:     "nilAeronSkipsDoWork",
			aeronNil: true,
			wantOps:  []string{"PollEgress"},
		},
		{
			name:    "injectedAeronStillDoWork",
			wantOps: []string{"DoWork", "PollEgress"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ops := setupSpinOnceOpRecorder(t)
			var ac *AeronCluster
			if tc.aeronNil {
				ac = closedOwnedNilAeronCluster(t, tc.mutate)
			} else {
				ac = newAeronClusterFromConnectFixture(t, tc.mutate)
				require.NoError(t, ac.Close())
				require.NotNil(t, ac.Aeron())
				require.True(t, ac.IsClosed())
			}

			require.Zero(t, ac.spinOnce(nil))
			require.Equal(t, tc.wantOps, ops.Ops)
		})
	}
}

func TestSpinOnce_usesConfigIdleStrategy(t *testing.T) {
	sub, image, termBuffer := setupEgressPollFixtures(t)
	appPayload := []byte("spin-once-context-idle")
	writeDefaultSessionMessageToTerm(t, termBuffer, appPayload)

	idle := &recordingIdleStrategy{}
	ac := newAeronClusterWithEgressImage(t, sub, image, func(result *ConnectResult) {
		result.Config.IdleStrategy = idle
	})

	work := ac.SpinOnce()

	require.Equal(t, 1, work)
	requireAeronClusterDispatchedSessionMessage(t, ac, appPayload)
	require.Equal(t, 1, idle.totalIdleCalls)
	require.Equal(t, work, idle.lastWorkCount)
	require.Zero(t, idle.resetCalls)
}

func TestSpinOnce_returnsPollEgressWorkCount(t *testing.T) {
	tests := []struct {
		name        string
		withIdle    bool
		writeEgress bool
		wantWork    int
	}{
		{name: "nilIdle", writeEgress: true, wantWork: 1},
		{name: "recordingIdle", withIdle: true, writeEgress: true, wantWork: 1},
		{name: "zeroWorkRecordingIdle", withIdle: true, wantWork: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sub, image, termBuffer := setupEgressPollFixtures(t)

			var appPayload []byte
			if tc.writeEgress {
				appPayload = []byte("spin-once-payload-" + tc.name)
				writeDefaultSessionMessageToTerm(t, termBuffer, appPayload)
			}

			ac := newAeronClusterWithEgressImage(t, sub, image, nil)

			var recording recordingIdleStrategy
			var idle idlestrategy.IdleStrategy
			if tc.withIdle {
				idle = &recording
			}

			work := ac.spinOnce(idle)

			require.Equal(t, tc.wantWork, work)
			if tc.writeEgress {
				requireAeronClusterDispatchedSessionMessage(t, ac, appPayload)
			}
			if tc.withIdle {
				require.Equal(t, 1, recording.totalIdleCalls)
				require.Equal(t, work, recording.lastWorkCount)
				require.Zero(t, recording.resetCalls)
			}
		})
	}
}
