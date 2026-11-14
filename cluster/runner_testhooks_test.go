//go:build testhooks

package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/cluster/consensus"
)

func TestServiceFailureGraceSpansSlowTicks(t *testing.T) {
	require.GreaterOrEqual(t, serviceFailureGraceNs, 10*consensus.SlowTickIntervalNsForTest())
}
