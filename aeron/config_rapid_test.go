//go:build rapid

package aeron_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/aeron/cnc"
)

const rapidMaxDuration = time.Hour

func drawOptionalDuration(t *rapid.T, name string) time.Duration {
	if rapid.Bool().Draw(t, name+"_zero") {
		return 0
	}
	n := rapid.Int64Range(1, int64(rapidMaxDuration)).Draw(t, name)
	return time.Duration(n)
}

func drawResourceLinger(t *rapid.T) *time.Duration {
	t.Helper()

	if rapid.Bool().Draw(t, "resource_linger_nil") {
		return nil
	}
	n := rapid.Int64Range(0, int64(rapidMaxDuration)).Draw(t, "resource_linger")
	return new(time.Duration(n))
}

func drawPartialConfig(t *rapid.T) (
	cfg aeron.Config,
	epochMs int64,
	nanoNs int64,
	customEpoch bool,
	customNano bool,
) {
	cfg.AeronDir = rapid.StringN(0, 32, 64).Draw(t, "aeron_dir")
	cfg.DriverTimeout = drawOptionalDuration(t, "driver_timeout")
	cfg.KeepAliveInterval = drawOptionalDuration(t, "keep_alive")
	cfg.InterServiceTimeout = drawOptionalDuration(t, "inter_service")
	cfg.IdleSleepDuration = drawOptionalDuration(t, "idle_sleep")
	cfg.ResourceLingerDuration = drawResourceLinger(t)
	cfg.ClientCloseLingerDuration = drawOptionalDuration(t, "client_close_linger")

	customEpoch = rapid.Bool().Draw(t, "custom_epoch")
	if customEpoch {
		epochMs = rapid.Int64Range(1, 1<<40).Draw(t, "epoch_ms")
		cfg.EpochClock = func() int64 {
			return epochMs
		}
	}

	customNano = rapid.Bool().Draw(t, "custom_nano")
	if customNano {
		nanoNs = rapid.Int64Range(1, 1<<40).Draw(t, "nano_ns")
		cfg.NanoClock = func() int64 {
			return nanoNs
		}
	}

	cfg.UseConductorInvoker = rapid.Bool().Draw(t, "use_conductor_invoker")

	return cfg, epochMs, nanoNs, customEpoch, customNano
}

func TestNormalizeConfig_durationFields_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		cfg, _, _, _, _ := drawPartialConfig(t)
		normalized := aeron.NormalizeConfigForTest(cfg)
		want := aeron.NormalizeConfigForTest(aeron.Config{
			DriverTimeout:     cfg.DriverTimeout,
			KeepAliveInterval: cfg.KeepAliveInterval,
			IdleSleepDuration: cfg.IdleSleepDuration,
		})

		require.Equal(t, want.DriverTimeout, normalized.DriverTimeout)
		require.Equal(t, want.KeepAliveInterval, normalized.KeepAliveInterval)
		require.Equal(t, cfg.InterServiceTimeout, normalized.InterServiceTimeout)
		require.Equal(t, want.IdleSleepDuration, normalized.IdleSleepDuration)
		require.Same(t, cfg.ResourceLingerDuration, normalized.ResourceLingerDuration)
		require.Equal(t, cfg.ClientCloseLingerDuration, normalized.ClientCloseLingerDuration)
	})
}

func TestNormalizeConfig_clocks_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		cfg, epochMs, nanoNs, customEpoch, customNano := drawPartialConfig(t)
		normalized := aeron.NormalizeConfigForTest(cfg)

		require.NotNil(t, normalized.EpochClock)
		require.NotNil(t, normalized.NanoClock)

		if customEpoch {
			require.Equal(t, epochMs, normalized.EpochClock())
		} else {
			require.Positive(t, normalized.EpochClock())
		}

		if customNano {
			require.Equal(t, nanoNs, normalized.NanoClock())
		} else {
			require.Positive(t, normalized.NanoClock())
		}
	})
}

func requireNormalizedConfigsEqual(
	t *rapid.T,
	want, got aeron.Config,
	customEpoch, customNano bool,
	epochMs, nanoNs int64,
) {
	require.Equal(t, want.AeronDir, got.AeronDir)
	require.Equal(t, want.DriverTimeout, got.DriverTimeout)
	require.Equal(t, want.KeepAliveInterval, got.KeepAliveInterval)
	require.Equal(t, want.InterServiceTimeout, got.InterServiceTimeout)
	require.Equal(t, want.IdleSleepDuration, got.IdleSleepDuration)
	require.Same(t, want.ResourceLingerDuration, got.ResourceLingerDuration)
	require.Equal(t, want.ClientCloseLingerDuration, got.ClientCloseLingerDuration)
	require.Equal(t, want.UseConductorInvoker, got.UseConductorInvoker)
	require.NotNil(t, want.EpochClock)
	require.NotNil(t, got.EpochClock)
	require.NotNil(t, want.NanoClock)
	require.NotNil(t, got.NanoClock)
	require.Equal(
		t,
		reflect.ValueOf(want.EpochClock).Pointer(),
		reflect.ValueOf(got.EpochClock).Pointer(),
	)
	require.Equal(
		t,
		reflect.ValueOf(want.NanoClock).Pointer(),
		reflect.ValueOf(got.NanoClock).Pointer(),
	)
	if customEpoch {
		require.Equal(t, epochMs, want.EpochClock())
		require.Equal(t, epochMs, got.EpochClock())
	}
	if customNano {
		require.Equal(t, nanoNs, want.NanoClock())
		require.Equal(t, nanoNs, got.NanoClock())
	}
}

func TestNormalizeConfig_idempotent_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		cfg, epochMs, nanoNs, customEpoch, customNano := drawPartialConfig(t)
		once := aeron.NormalizeConfigForTest(cfg)
		twice := aeron.NormalizeConfigForTest(once)
		requireNormalizedConfigsEqual(t, once, twice, customEpoch, customNano, epochMs, nanoNs)
	})
}

// drawBoundDuration returns bound on one branch so the at-or-below check sees
// its edge.
func drawBoundDuration(t *rapid.T, name string, bound time.Duration) time.Duration {
	t.Helper()
	if rapid.Bool().Draw(t, name+"_at_bound") {
		return bound
	}
	return time.Duration(rapid.Int64Range(
		int64(-time.Second),
		int64(rapidMaxDuration),
	).Draw(t, name))
}

func TestResolveInterServiceTimeout_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		cfg := aeron.NormalizeConfigForTest(aeron.Config{
			AeronDir:          "unused",
			KeepAliveInterval: drawOptionalDuration(t, "keep_alive"),
		})
		keepAlive := cfg.KeepAliveInterval
		explicit := drawOptionalDuration(t, "inter_service")
		if rapid.Bool().Draw(t, "inter_service_at_bound") {
			explicit = keepAlive
		}
		cfg.InterServiceTimeout = explicit
		livenessTimeout := drawBoundDuration(t, "liveness", keepAlive)

		err := aeron.ValidateConnectConfigForTest(cfg)
		var resolved time.Duration
		if err == nil {
			resolved, err = aeron.ResolveInterServiceTimeoutForTest(cfg, cnc.Metadata{
				ClientLivenessTimeoutNs: livenessTimeout.Nanoseconds(),
			})
		}
		switch {
		case explicit != 0 && explicit <= keepAlive:
			require.EqualError(t, err, fmt.Sprintf(
				"aeron: InterServiceTimeout %v must exceed KeepAliveInterval %v",
				explicit,
				keepAlive,
			))
		case explicit == 0 && livenessTimeout <= keepAlive:
			require.EqualError(t, err, fmt.Sprintf(
				"aeron: driver client liveness timeout %v must exceed KeepAliveInterval %v",
				livenessTimeout,
				keepAlive,
			))
		case explicit != 0:
			require.NoError(t, err)
			require.Equal(t, explicit, resolved)
		default:
			require.NoError(t, err)
			require.Equal(t, livenessTimeout, resolved)
		}
	})
}
