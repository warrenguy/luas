package aeron

import (
	"testing"
	"time"

	"github.com/warrenguy/luas/idlestrategy"
	"github.com/warrenguy/luas/internal/testutil"
)

func TestClient_conductorLoopStep_ZeroAllocs(t *testing.T) {
	client := NewClientForTest(nil, func() int64 { return 0 })
	strategy := idlestrategy.NewSleepingMillis(time.Nanosecond)
	testutil.AssertZeroAllocs(t, "conductorLoopStep", 1000, func() {
		client.conductorLoopStep(strategy)
	})
}
