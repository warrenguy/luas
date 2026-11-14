//go:build rapid

package cluster

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/luas/cluster/consensus"
)

// runnerModel is the reference duty cycle that Run must match.
type runnerModel struct {
	ticks      int
	idleCounts []int
	resets     int
	err        error
}

func workAt(schedule []int, index int) int {
	if index < len(schedule) {
		return schedule[index]
	}
	return 0
}

// runnerSchedule is one drawn member schedule. A negative terminateAt or
// memberEndAt never fires.
type runnerSchedule struct {
	transportWork, archiveWork, agentWork []int
	cancelAt, terminateAt, memberEndAt    int
	memberEnd                             error
}

func drawRunnerSchedule(rt *rapid.T, length int) runnerSchedule {
	rt.Helper()

	return runnerSchedule{
		transportWork: rapid.SliceOfN(rapid.IntRange(0, 8), length, length).Draw(rt, "transportWork"),
		archiveWork:   rapid.SliceOfN(rapid.IntRange(0, 8), length, length).Draw(rt, "archiveWork"),
		agentWork:     rapid.SliceOfN(rapid.IntRange(0, 8), length, length).Draw(rt, "agentWork"),
		cancelAt:      rapid.IntRange(0, length-1).Draw(rt, "cancelAt"),
		terminateAt:   rapid.IntRange(-1, length-1).Draw(rt, "terminateAt"),
		memberEndAt:   rapid.IntRange(-1, length-1).Draw(rt, "memberEndAt"),
		memberEnd:     &consensus.TerminationError{Cause: errors.New("boom")},
	}
}

func (s runnerSchedule) workAt(tick int) int {
	return workAt(s.transportWork, tick) + workAt(s.archiveWork, tick) + workAt(s.agentWork, tick)
}

// newScheduledFakes builds the transport, archive, and agent fakes that
// follow s.
func newScheduledFakes(
	s runnerSchedule,
	cancel context.CancelFunc,
) (*scheduledTransport, *fakeArchive, *fakeAgent) {
	transport := &scheduledTransport{
		work:        s.transportWork,
		terminateAt: s.terminateAt,
		cancelAt:    s.cancelAt,
		cancel:      cancel,
	}
	agent := &fakeAgent{work: s.agentWork}

	if s.memberEndAt >= 0 {
		agent.endAt = s.memberEndAt
		agent.endErr = s.memberEnd
	}
	return transport, &fakeArchive{work: s.archiveWork}, agent
}

// modelRun walks the drawn schedule in Run's check order.
//
// scheduledTransport cancels the context inside tick cancelAt. Run sees the
// cancel at the top of the next tick. Run sees each termination on the tick
// that raises it.
func modelRun(s runnerSchedule) runnerModel {
	model := runnerModel{resets: 1}
	canceled := false
	for tick := 0; ; tick++ {
		if canceled {
			model.err = context.Canceled
			return model
		}
		model.ticks++
		if tick >= s.cancelAt {
			canceled = true
		}
		if s.terminateAt >= 0 && tick >= s.terminateAt {
			model.err = ErrTransportTerminating
			return model
		}
		if s.memberEndAt >= 0 && tick >= s.memberEndAt {
			model.err = s.memberEnd
			return model
		}
		model.idleCounts = append(model.idleCounts, s.workAt(tick))
	}
}

// scheduledTransport terminates from a drawn tick index onward.
// scheduledTransport cancels the run context from another drawn tick index.
type scheduledTransport struct {
	work        []int
	ticks       int
	terminateAt int
	cancelAt    int
	cancel      context.CancelFunc
}

func (s *scheduledTransport) DoWork() int {
	value := workAt(s.work, s.ticks)
	s.ticks++
	if s.ticks > s.cancelAt {
		s.cancel()
	}
	return value
}

func (s *scheduledTransport) IsTerminating() bool {
	return s.terminateAt >= 0 && s.ticks > s.terminateAt
}

func TestRunnerRunMatchesScheduleModel(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		length := rapid.IntRange(1, 40).Draw(rt, "length")
		schedule := drawRunnerSchedule(rt, length)
		want := modelRun(schedule)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		transport, archive, agent := newScheduledFakes(schedule, cancel)
		idle := &recordingIdle{}
		runner := &Runner{
			client:  transport,
			archive: archive,
			agent:   agent,
			idle:    idle,
			clock:   func() int64 { return int64(transport.ticks) },
		}

		err := runner.Run(ctx)

		require.Same(rt, want.err, err, "Run returns the stop value unchanged")
		require.Equal(rt, want.ticks, transport.ticks, "transport ticks")
		require.Equal(rt, want.ticks, archive.ticks, "archive ticks")
		require.Equal(rt, want.ticks, agent.ticks, "agent ticks")
		require.Equal(rt, want.idleCounts, idle.counts, "idle work counts")
		require.Equal(rt, want.resets, idle.resets, "resets")
	})
}

// TestRunnerDoWorkSumsParts asserts DoWork equals the sum of transport, archive,
// and agent work counts for every drawn schedule. Asserts the idle strategy is
// untouched.
func TestRunnerDoWorkSumsParts(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		transportWork := rapid.SliceOf(rapid.IntRange(0, 1000)).Draw(rt, "transportWork")
		archiveWork := rapid.SliceOf(rapid.IntRange(0, 1000)).Draw(rt, "archiveWork")
		agentWork := rapid.SliceOf(rapid.IntRange(0, 1000)).Draw(rt, "agentWork")
		length := min(len(transportWork), len(archiveWork), len(agentWork))
		if length == 0 {
			return
		}
		now := rapid.Int64().Draw(rt, "now")

		transport := &fakeTransport{work: transportWork}
		archive := &fakeArchive{work: archiveWork}
		agent := &fakeAgent{work: agentWork}
		idle := &recordingIdle{}
		runner := &Runner{
			client:  transport,
			archive: archive,
			agent:   agent,
			idle:    idle,
			clock:   func() int64 { return now },
		}

		for i := range length {
			require.Equal(rt, transportWork[i]+archiveWork[i]+agentWork[i], runner.DoWork(), "tick %d", i)
		}
		require.Equal(rt, length, transport.ticks)
		require.Equal(rt, length, archive.ticks)
		require.Equal(rt, length, agent.ticks)
		for _, seen := range agent.times {
			require.Equal(rt, now, seen, "agent reads the injected clock")
		}
		require.Empty(rt, idle.counts)
		require.Zero(rt, idle.resets)
	})
}

type addingClock struct {
	now   int64
	step  int64
	calls int
}

func (c *addingClock) read() int64 {
	c.calls++
	c.now += c.step
	return c.now
}

type serviceRunModel struct {
	ticks        int
	clockCalls   int
	idleCounts   []int
	resets       int
	err          error
	cause        error
	agentTimes   []int64
	serviceTimes []int64
}

// serviceSchedule is one drawn service. The cause shows from tick
// failureAfter until tick clearAt. A negative failureAfter or clearAt never
// fires.
type serviceSchedule struct {
	id                    int32
	work                  []int
	cause                 error
	failureAfter, clearAt int
}

func (s serviceSchedule) failureAt(tick int) error {
	if s.cause == nil || tick < s.failureAfter {
		return nil
	}

	if s.clearAt >= 0 && tick >= s.clearAt {
		return nil
	}
	return s.cause
}

func modelLowestFailure(tick int, services []serviceSchedule) (int32, error) {
	var id int32
	var cause error

	for _, svc := range services {
		next := svc.failureAt(tick)
		if next == nil {
			continue
		}

		if cause != nil && svc.id >= id {
			continue
		}

		id, cause = svc.id, next
	}
	return id, cause
}

// modelDeadline returns nowNs plus the 100ms grace, saturated at math.MaxInt64.
func modelDeadline(nowNs int64) int64 {
	sum := new(big.Int).Add(big.NewInt(nowNs), big.NewInt(int64(100*time.Millisecond)))
	if !sum.IsInt64() {
		return math.MaxInt64
	}
	return sum.Int64()
}

// modelServiceRun walks one drawn schedule in RunWithServices check order.
func modelServiceRun(
	s runnerSchedule,
	services []serviceSchedule,
	clockStart, clockStep int64,
) serviceRunModel {
	model := serviceRunModel{resets: 1}
	canceled := false
	seen := false
	var deadline int64
	now := clockStart
	read := func() int64 {
		model.clockCalls++
		now += clockStep
		return now
	}

	for tick := 0; ; tick++ {
		if canceled {
			model.err = context.Canceled
			return model
		}

		model.ticks++
		model.agentTimes = append(model.agentTimes, read())
		var serviceNow int64

		if len(services) > 0 {
			serviceNow = read()
			model.serviceTimes = append(model.serviceTimes, serviceNow)
		}

		if tick >= s.cancelAt {
			canceled = true
		}

		if s.terminateAt >= 0 && tick >= s.terminateAt {
			model.err = ErrTransportTerminating
			return model
		}

		if s.memberEndAt >= 0 && tick >= s.memberEndAt {
			model.err = s.memberEnd
			return model
		}

		id, cause := modelLowestFailure(tick, services)

		switch {
		case cause == nil:
			seen = false
		case !seen:
			seen = true
			deadline = modelDeadline(serviceNow)
		case serviceNow >= deadline:
			model.err = fmt.Errorf("cluster: service %d: %w", id, cause)
			model.cause = cause
			return model
		}

		work := s.workAt(tick)

		for _, svc := range services {
			work += workAt(svc.work, tick)
		}

		model.idleCounts = append(model.idleCounts, work)
	}
}

func TestRunnerRunWithServicesMatchesScheduleModel(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		length := rapid.IntRange(1, 24).Draw(rt, "length")
		serviceCount := rapid.IntRange(0, 3).Draw(rt, "serviceCount")
		var ids []int32

		if serviceCount > 0 {
			ids = rapid.SliceOfNDistinct(
				rapid.Int32Range(0, 9),
				serviceCount,
				serviceCount,
				rapid.ID[int32],
			).Draw(rt, "ids")
		}

		schedule := drawRunnerSchedule(rt, length)
		draws := make([]serviceSchedule, serviceCount)
		services := make([]*fakeService, serviceCount)
		args := make([]ServiceAgent, serviceCount)

		for i := range serviceCount {
			draw := serviceSchedule{
				id: ids[i],
				work: rapid.SliceOfN(rapid.IntRange(0, 8), length, length).
					Draw(rt, fmt.Sprintf("serviceWork-%d", i)),
				failureAfter: rapid.IntRange(-1, length-1).Draw(rt, fmt.Sprintf("failureAt-%d", i)),
			}

			if draw.failureAfter >= 0 {
				draw.cause = fmt.Errorf("cause-%d", draw.id)
			}

			draw.clearAt = rapid.IntRange(-1, length-1).Draw(rt, fmt.Sprintf("clearAt-%d", i))
			draws[i] = draw

			terminatedAt := rapid.IntRange(-1, length-1).Draw(rt, fmt.Sprintf("terminatedAt-%d", i))
			services[i] = &fakeService{
				id:           draw.id,
				work:         draw.work,
				failure:      draw.cause,
				failureAfter: draw.failureAfter,
				clears:       draw.clearAt >= 0,
				clearAfter:   draw.clearAt,
			}

			if terminatedAt >= 0 {
				services[i].reportTerminated = true
				services[i].terminatedAfter = terminatedAt
			}

			args[i] = services[i]
		}

		clockStart := rapid.Int64().Draw(rt, "clockStart")
		clockStep := rapid.Int64().Draw(rt, "clockStep")
		want := modelServiceRun(schedule, draws, clockStart, clockStep)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		transport, archive, agent := newScheduledFakes(schedule, cancel)
		clock := &addingClock{now: clockStart, step: clockStep}
		idle := &recordingIdle{}
		runner := newTestRunner(transport, archive, agent, idle, clock.read)

		err := runner.RunWithServices(ctx, args...)

		if want.cause != nil {
			require.EqualError(rt, err, want.err.Error())
			require.ErrorIs(rt, err, want.cause)
		} else {
			require.Same(rt, want.err, err)
		}

		require.Equal(rt, want.ticks, transport.ticks, "transport ticks")
		require.Equal(rt, want.ticks, archive.ticks, "archive ticks")
		require.Equal(rt, want.ticks, agent.ticks, "agent ticks")
		require.Equal(rt, want.clockCalls, clock.calls, "clock calls")
		require.Equal(rt, want.agentTimes, agent.times, "agent clock")
		require.Equal(rt, want.idleCounts, idle.counts, "idle work counts")
		require.Equal(rt, want.resets, idle.resets, "resets")
		require.Len(rt, agent.liveness, serviceCount)

		for i := range services {
			require.Equal(rt, want.ticks, services[i].ticks, "service %d ticks", i)
			require.Equal(rt, want.serviceTimes, services[i].times, "service %d clock", i)
			require.Equal(rt, ids[i], agent.liveness[i].id)
			require.Same(rt, services[i], agent.liveness[i].source)
		}
	})
}
