package aeron_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
)

func TestConnect_validationErrors(t *testing.T) {
	tests := []struct {
		name        string
		cfg         aeron.Config
		wantErr     string
		fullConnect bool
	}{
		{
			name:    "requires aeron dir",
			cfg:     aeron.Config{},
			wantErr: "aeron: AeronDir is required",
		},
		{
			name: "rejects negative resource linger duration",
			cfg: aeron.Config{
				AeronDir:               "unused",
				ResourceLingerDuration: new(-time.Second),
			},
			wantErr: "aeron: ResourceLingerDuration must not be negative",
		},
		{
			name: "rejects negative client close linger duration",
			cfg: aeron.Config{
				AeronDir:                  "unused",
				ClientCloseLingerDuration: -time.Second,
			},
			wantErr: "aeron: ClientCloseLingerDuration must not be negative",
		},
		{
			name: "rejects negative keepalive interval",
			cfg: aeron.Config{
				AeronDir:          "unused",
				KeepAliveInterval: -time.Second,
			},
			wantErr: "aeron: KeepAliveInterval must not be negative",
		},
		{
			name: "rejects negative inter-service timeout",
			cfg: aeron.Config{
				AeronDir:            "unused",
				InterServiceTimeout: -time.Second,
			},
			wantErr: "aeron: InterServiceTimeout must not be negative",
		},
		{
			name: "rejects inter-service timeout equal to keepalive interval",
			cfg: aeron.Config{
				AeronDir:            "unused",
				InterServiceTimeout: 500 * time.Millisecond,
			},
			wantErr: "aeron: InterServiceTimeout 500ms must exceed KeepAliveInterval 500ms",
		},
		{
			name: "rejects inter-service timeout below keepalive interval",
			cfg: aeron.Config{
				AeronDir:            "unused",
				KeepAliveInterval:   time.Second,
				InterServiceTimeout: 500 * time.Millisecond,
			},
			wantErr: "aeron: InterServiceTimeout 500ms must exceed KeepAliveInterval 1s",
		},
		{
			name: "rejects negative poll timeout",
			cfg: aeron.Config{
				AeronDir:    "unused",
				PollTimeout: -time.Second,
			},
			wantErr: "aeron: PollTimeout must not be negative",
		},
		{
			name: "rejects negative publication offer timeout",
			cfg: aeron.Config{
				AeronDir:                "unused",
				PublicationOfferTimeout: -time.Second,
			},
			wantErr: "aeron: PublicationOfferTimeout must not be negative",
		},
		{
			name: "rejects negative driver timeout",
			cfg: aeron.Config{
				AeronDir:      "unused",
				DriverTimeout: -time.Second,
			},
			wantErr: "aeron: DriverTimeout must not be negative",
		},
		{
			name: "rejects negative idle sleep duration",
			cfg: aeron.Config{
				AeronDir:          "unused",
				IdleSleepDuration: -time.Second,
			},
			wantErr: "aeron: IdleSleepDuration must not be negative",
		},
		{
			name: "rejects idle sleep duration above one second",
			cfg: aeron.Config{
				AeronDir:          "unused",
				IdleSleepDuration: time.Second + time.Nanosecond,
			},
			wantErr: "aeron: IdleSleepDuration must not exceed 1s",
		},
		{
			name: "wait cnc ready timeout",
			cfg: aeron.Config{
				AeronDir:      t.TempDir(),
				DriverTimeout: 10 * time.Millisecond,
			},
			wantErr:     "aeron: wait for cnc ready: cnc: timeout waiting for ready",
			fullConnect: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var err error
			if testCase.fullConnect {
				_, err = aeron.Connect(t.Context(), testCase.cfg)
			} else {
				err = aeron.ValidateConnectConfigForTest(
					aeron.NormalizeConfigForTest(testCase.cfg),
				)
			}
			require.EqualError(t, err, testCase.wantErr)
		})
	}
}

func TestConnect_contextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := aeron.Connect(ctx, aeron.Config{
		AeronDir:      t.TempDir(),
		DriverTimeout: time.Second,
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestConnect_zeroDriverTimeoutKeepsDefault(t *testing.T) {
	cfg := aeron.NormalizeConfigForTest(aeron.Config{AeronDir: "unused"})
	require.Equal(t, 10*time.Second, cfg.DriverTimeout)
	require.NoError(t, aeron.ValidateConnectConfigForTest(cfg))
}

func TestConnect_contextDeadlineDuringCncWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Millisecond)
	defer cancel()

	_, err := aeron.Connect(ctx, aeron.Config{
		AeronDir:      t.TempDir(),
		DriverTimeout: 5 * time.Second,
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
}
