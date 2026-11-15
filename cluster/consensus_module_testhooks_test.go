//go:build testhooks

package cluster_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/cluster"
)

func TestOpenConsensusModule_configErrors_backgroundAeron(t *testing.T) {
	t.Parallel()

	client := aeron.NewStartedBackgroundClientForTest(t)
	t.Cleanup(func() { _ = client.Close() })

	cfg := recoveryConfig(t, t.TempDir())
	cfg.Aeron = client

	module, err := cluster.OpenConsensusModule(context.Background(), cluster.ConsensusModuleDeps{
		Config: cfg,
	})
	require.Nil(t, module)
	require.EqualError(t, err, "cluster: Aeron must run with UseConductorInvoker true")

	_, countersErr := client.CountersReader()
	require.EqualError(t, countersErr, "aeron: client has no counters reader")
}
