package aeron_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/idlestrategy"
)

func TestNormalizeConfig_defaults(t *testing.T) {
	normalized := aeron.NormalizeConfigForTest(aeron.Config{})

	require.Equal(t, 10*time.Second, normalized.DriverTimeout)
	require.Equal(t, 500*time.Millisecond, normalized.KeepAliveInterval)
	require.Zero(t, normalized.InterServiceTimeout)
	require.Equal(t, 16*time.Millisecond, normalized.IdleSleepDuration)
	require.Nil(t, normalized.ResourceLingerDuration)
	require.Zero(t, normalized.ClientCloseLingerDuration)
	require.Equal(t, 10*time.Second, normalized.PollTimeout)
	require.Equal(t, 10*time.Second, normalized.PublicationOfferTimeout)
	require.NotNil(t, normalized.EpochClock)
	require.NotNil(t, normalized.NanoClock)
	require.Positive(t, normalized.EpochClock())
	require.Positive(t, normalized.NanoClock())
	require.False(t, normalized.UseConductorInvoker)
	require.Nil(t, normalized.ConductorIdleStrategy)
}

func TestNormalizeConfig_preservesExplicitValues(t *testing.T) {
	epochMs := int64(42)
	nanoNs := int64(99)
	resourceLinger := 7 * time.Second
	strategy := idlestrategy.NewSleepingMillis(time.Millisecond)

	normalized := aeron.NormalizeConfigForTest(aeron.Config{
		DriverTimeout:             3 * time.Second,
		KeepAliveInterval:         250 * time.Millisecond,
		InterServiceTimeout:       2 * time.Second,
		IdleSleepDuration:         8 * time.Millisecond,
		ResourceLingerDuration:    &resourceLinger,
		ClientCloseLingerDuration: 2 * time.Second,
		PollTimeout:               4 * time.Second,
		PublicationOfferTimeout:   3 * time.Second,
		EpochClock: func() int64 {
			return epochMs
		},
		NanoClock: func() int64 {
			return nanoNs
		},
		UseConductorInvoker:   true,
		ConductorIdleStrategy: strategy,
	})

	require.Equal(t, 3*time.Second, normalized.DriverTimeout)
	require.Equal(t, 250*time.Millisecond, normalized.KeepAliveInterval)
	require.Equal(t, 2*time.Second, normalized.InterServiceTimeout)
	require.Equal(t, 8*time.Millisecond, normalized.IdleSleepDuration)
	require.Same(t, &resourceLinger, normalized.ResourceLingerDuration)
	require.Equal(t, 7*time.Second, *normalized.ResourceLingerDuration)
	require.Equal(t, 2*time.Second, normalized.ClientCloseLingerDuration)
	require.Equal(t, 4*time.Second, normalized.PollTimeout)
	require.Equal(t, 3*time.Second, normalized.PublicationOfferTimeout)
	require.Equal(t, epochMs, normalized.EpochClock())
	require.Equal(t, nanoNs, normalized.NanoClock())
	require.True(t, normalized.UseConductorInvoker)
	require.Same(t, strategy, normalized.ConductorIdleStrategy)
}

func TestConfig_conductorIdleStrategy(t *testing.T) {
	cfg := aeron.NormalizeConfigForTest(aeron.Config{})
	first := aeron.ConductorIdleStrategyForTest(cfg)
	second := aeron.ConductorIdleStrategyForTest(cfg)

	require.NotNil(t, first)
	require.NotSame(t, first, second)

	strategy := idlestrategy.NewSleepingMillis(time.Millisecond)
	cfg.ConductorIdleStrategy = strategy
	require.Same(t, strategy, aeron.ConductorIdleStrategyForTest(cfg))
}

func TestResolveInterServiceTimeout(t *testing.T) {
	tests := []struct {
		name                string
		interServiceTimeout time.Duration
		livenessTimeout     time.Duration
		want                time.Duration
		wantErr             string
	}{
		{
			name:            "zero takes the driver value",
			livenessTimeout: 10 * time.Second,
			want:            10 * time.Second,
		},
		{
			name:                "explicit value wins",
			interServiceTimeout: 2 * time.Second,
			livenessTimeout:     10 * time.Second,
			want:                2 * time.Second,
		},
		{
			name:            "driver value equal to keepalive interval fails",
			livenessTimeout: 500 * time.Millisecond,
			wantErr: "aeron: driver client liveness timeout 500ms must exceed " +
				"KeepAliveInterval 500ms",
		},
		{
			name:            "zero driver value fails",
			livenessTimeout: 0,
			wantErr: "aeron: driver client liveness timeout 0s must exceed " +
				"KeepAliveInterval 500ms",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			cfg := aeron.NormalizeConfigForTest(aeron.Config{
				AeronDir:            "unused",
				InterServiceTimeout: testCase.interServiceTimeout,
			})
			metadata := cnc.Metadata{
				ClientLivenessTimeoutNs: testCase.livenessTimeout.Nanoseconds(),
			}

			resolved, err := aeron.ResolveInterServiceTimeoutForTest(cfg, metadata)
			if testCase.wantErr != "" {
				require.EqualError(t, err, testCase.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, testCase.want, resolved)
		})
	}
}
