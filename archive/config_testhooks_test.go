//go:build testhooks

package archive_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/archive"
)

func concludeConfig(
	t *testing.T,
	cfg archive.Config,
) (archive.Config, *aeron.Client, bool) {
	t.Helper()
	concluded, clientOut, owns, err := archive.ConcludeConfigForTest(t.Context(), cfg)
	require.NoError(t, err)
	return concluded, clientOut, owns
}

func concludedConfig(
	t *testing.T,
	mutate func(*archive.Config),
) archive.Config {
	t.Helper()
	cfg := normalizedConfig(t, mutate)
	cfg.Aeron = &aeron.Client{}
	concluded, _, _ := concludeConfig(t, cfg)
	return concluded
}

func TestConcludeConfig_injectedAeron(t *testing.T) {
	injected := &aeron.Client{}
	cfg := normalizedConfig(t, func(cfg *archive.Config) {
		cfg.Aeron = injected
	})

	concluded, clientOut, owns := concludeConfig(t, cfg)
	require.False(t, owns)
	require.Same(t, injected, clientOut)
	require.Same(t, injected, concluded.Aeron)
}

func TestConcludeConfig_ownedClientFactory(t *testing.T) {
	tests := []struct {
		name          string
		callerHandler bool
	}{
		{name: "default_handler"},
		{name: "caller_handler", callerHandler: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			aeronDir := t.TempDir()
			owned := &aeron.Client{}
			var factoryHandler func(error)
			archive.SetConnectConstructionHooksForTest(
				t,
				aeronDir,
				func(cfg archive.Config) (*aeron.Client, error) {
					factoryHandler = cfg.AeronConfig.ErrorHandler
					return owned, nil
				},
			)

			var callerErrs []error
			var callerHandler func(error)
			if tc.callerHandler {
				callerHandler = func(err error) { callerErrs = append(callerErrs, err) }
			}
			cfg := normalizedConfig(t, func(cfg *archive.Config) {
				cfg.AeronDir = ""
				cfg.AeronConfig = aeron.Config{
					AeronDir:     aeronDir,
					ErrorHandler: callerHandler,
				}
			})

			concluded, clientOut, owns := concludeConfig(t, cfg)
			require.True(t, owns)
			require.Same(t, owned, clientOut)
			require.Same(t, owned, concluded.Aeron)

			// Phase: the factory and the returned Config see the caller
			// handler unchanged.
			require.Equal(t, tc.callerHandler, factoryHandler != nil)
			require.Equal(t, tc.callerHandler, concluded.AeronConfig.ErrorHandler != nil)

			if !tc.callerHandler {
				return
			}

			fault := errors.New("fault")
			factoryHandler(fault)
			require.Equal(t, []error{fault}, callerErrs)
		})
	}
}

func TestConcludeConfig_invalidResponseChannel(t *testing.T) {
	cfg := normalizedConfig(t, func(cfg *archive.Config) {
		withRemoteControl(cfg)
		cfg.ControlResponseChannel = "not-a-channel"
		cfg.Aeron = &aeron.Client{}
	})

	_, _, _, err := archive.ConcludeConfigForTest(t.Context(), cfg)
	require.EqualError(t, err, "archive: invalid control response channel: not-a-channel")
}

func TestConcludeConfig_udpRejoinFalseRemote(t *testing.T) {
	concluded := concludedConfig(t, withRemoteControl)

	require.Contains(t, concluded.EffectiveControlResponseChannel(), "rejoin=false")
}

func TestConcludeConfig_ipcSkipsRejoinFalse(t *testing.T) {
	concluded := concludedConfig(t, nil)

	channel := concluded.EffectiveControlResponseChannel()
	require.Equal(t, archive.DefaultLocalControlChannel, channel)
	require.NotContains(t, channel, "rejoin=false")
}
