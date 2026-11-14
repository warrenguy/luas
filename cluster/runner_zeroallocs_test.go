//go:build testhooks

package cluster

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/cluster/consensus"
	"github.com/warrenguy/luas/internal/testutil"
)

const (
	runnerTickWarmup = 10
	runnerTickRuns   = 250

	// runnerTickPair is the ticks per run. AllocsPerRun floors its average.
	// With one tick per run, an allocation on every second tick reads as 0.
	runnerTickPair = 2
)

// fixedAgent returns fixed work. Unlike fakeAgent, fixedAgent does not
// allocate.
type fixedAgent struct{ work int }

func (c *fixedAgent) DoWork(int64) int { return c.work }

func (c *fixedAgent) TerminationError() error { return nil }

func (c *fixedAgent) SetServiceLiveness(int32, consensus.ServiceLiveness) error {
	return nil
}

// countingService counts ticks. Unlike fakeService, countingService does not
// allocate.
type countingService struct {
	fakeStoppedService
	id    int32
	ticks int
	work  int
}

func (c *countingService) ServiceID() int32 { return c.id }

func (c *countingService) DoWork(int64) int {
	c.ticks++
	return c.work
}

func TestRunnerTickZeroAllocs(t *testing.T) {
	tests := []struct {
		name    string
		failure error
	}{
		{name: "steady"},
		// The clock stays at 0, so every tick stays inside the grace window.
		{name: "service failure grace", failure: errors.New("service: boom")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc0 := &countingService{id: 0, work: 4}
			svc1 := &countingService{id: 1, work: 5}
			svc1.failure = tc.failure
			services := []ServiceAgent{svc0, svc1}
			agent := &fixedAgent{work: 3}
			runner := newTestRunner(&fakeTransport{}, &fakeArchive{}, agent, nil, func() int64 { return 0 })

			var watch serviceFailureWatch

			for range runnerTickWarmup {
				work, err := runner.tick(services, &watch)
				require.NoError(t, err)
				require.Equal(t, 12, work)
			}

			require.Equal(t, tc.failure != nil, watch.seen)

			calls := 0
			misses := 0
			testutil.AssertZeroAllocs(t, "Runner.tick", runnerTickRuns, func() {
				calls++

				for range runnerTickPair {
					work, err := runner.tick(services, &watch)
					if err != nil || work != 12 {
						misses++
					}
				}
			})
			require.Zero(t, misses)

			ticks := runnerTickWarmup + calls*runnerTickPair
			require.Equal(t, ticks, svc0.ticks)
			require.Equal(t, ticks, svc1.ticks)
		})
	}
}
