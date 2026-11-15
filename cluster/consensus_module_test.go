package cluster_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/cluster"
	"github.com/warrenguy/luas/idlestrategy"
)

func TestOpenConsensusModule_configErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(cfg *cluster.Config)
		wantErr string
	}{
		{
			name: "member id absent",
			mutate: func(cfg *cluster.Config) {
				cfg.MemberID = 7
				cfg.Election.MemberID = 7
			},
			wantErr: "cluster: MemberID 7 is not present in ClusterMembers",
		},
		{
			name:    "cluster dir empty",
			mutate:  func(cfg *cluster.Config) { cfg.ClusterDir = "" },
			wantErr: "cluster: ClusterDir is required",
		},
		{
			name:    "log channel not udp",
			mutate:  func(cfg *cluster.Config) { cfg.LogChannel = "aeron:ipc" },
			wantErr: "cluster: LogChannel must be a udp channel",
		},
		{
			name:    "consensus channel not udp",
			mutate:  func(cfg *cluster.Config) { cfg.ConsensusChannel = "aeron:ipc" },
			wantErr: "cluster: ConsensusChannel must be a udp channel",
		},
		{
			name:    "negative service count",
			mutate:  func(cfg *cluster.Config) { cfg.ServiceCount = -1 },
			wantErr: "cluster: ServiceCount must not be negative",
		},
		{
			name: "archive request channel empty",
			mutate: func(cfg *cluster.Config) {
				cfg.ArchiveControlRequestChannel = ""
			},
			wantErr: "cluster: ArchiveControlRequestChannel is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := recoveryConfig(t, t.TempDir())
			tt.mutate(&cfg)

			module, err := cluster.OpenConsensusModule(context.Background(), cluster.ConsensusModuleDeps{
				Config: cfg,
			})
			require.Nil(t, module)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestOpenConsensusModule_memberAeronConfigCopiesInvoker(t *testing.T) {
	t.Parallel()

	linger := time.Second
	strategy := idlestrategy.NewSleepingMillis(time.Millisecond)
	cfg := aeron.Config{
		AeronDir:               "/tmp/aeron",
		DriverTimeout:          3 * time.Second,
		KeepAliveInterval:      250 * time.Millisecond,
		IdleSleepDuration:      8 * time.Millisecond,
		ResourceLingerDuration: &linger,
		UseConductorInvoker:    false,
		ConductorIdleStrategy:  strategy,
	}

	got := cluster.MemberAeronConfigForTest(cfg)
	want := cfg
	want.UseConductorInvoker = true
	require.Equal(t, want, got)
	require.Same(t, cfg.ResourceLingerDuration, got.ResourceLingerDuration)
	require.Same(t, strategy, got.ConductorIdleStrategy)
}

func TestConsensusModule_CloseNilReceiver(t *testing.T) {
	t.Parallel()

	var module *cluster.ConsensusModule
	require.NoError(t, module.Close())
}
