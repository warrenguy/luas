package cluster

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/cluster/codecs/clusterwire"
	"github.com/warrenguy/luas/cluster/consensus"
	"github.com/warrenguy/luas/cluster/service"
	"github.com/warrenguy/luas/idlestrategy"
)

var _ ServiceAgent = (*service.Agent)(nil)

// fakeTransport records ticks and reports termination when set.
type fakeTransport struct {
	work        []int
	ticks       int
	terminating bool
	steps       *[]string
}

func (f *fakeTransport) DoWork() int {
	if f.steps != nil {
		*f.steps = append(*f.steps, "transport")
	}
	value := 0
	if f.ticks < len(f.work) {
		value = f.work[f.ticks]
	}
	f.ticks++
	return value
}

func (f *fakeTransport) IsTerminating() bool { return f.terminating }

// fakeArchive records Poll ticks and work counts.
type fakeArchive struct {
	work  []int
	ticks int
	steps *[]string
}

func (f *fakeArchive) Poll() int {
	if f.steps != nil {
		*f.steps = append(*f.steps, "archive")
	}
	value := 0
	if f.ticks < len(f.work) {
		value = f.work[f.ticks]
	}
	f.ticks++
	return value
}

// fakeAgent records the clock values each DoWork saw.
type fakeAgent struct {
	work   []int
	ticks  int
	times  []int64
	steps  *[]string
	endAt  int
	endErr error

	liveness       []livenessSet
	livenessErr    error
	failLivenessID int32
}

func (f *fakeAgent) DoWork(nowNs int64) int {
	if f.steps != nil {
		*f.steps = append(*f.steps, "agent")
	}
	f.times = append(f.times, nowNs)
	value := 0
	if f.ticks < len(f.work) {
		value = f.work[f.ticks]
	}
	f.ticks++
	return value
}

func (f *fakeAgent) TerminationError() error {
	if f.ticks > f.endAt {
		return f.endErr
	}
	return nil
}

type livenessSet struct {
	id     int32
	source consensus.ServiceLiveness
}

func (f *fakeAgent) SetServiceLiveness(serviceID int32, source consensus.ServiceLiveness) error {
	if f.livenessErr != nil && serviceID == f.failLivenessID {
		return f.livenessErr
	}

	f.liveness = append(f.liveness, livenessSet{id: serviceID, source: source})
	return nil
}

// recordingIdle tallies Idle calls, Reset calls, and work counts.
type recordingIdle struct {
	counts []int
	resets int
	steps  *[]string
}

func (r *recordingIdle) Idle(workCount int) {
	if r.steps != nil {
		*r.steps = append(*r.steps, "idle")
	}
	r.counts = append(r.counts, workCount)
}

func (r *recordingIdle) Reset() { r.resets++ }

// fakeService is one co-hosted service in a runner test.
type fakeService struct {
	id      int32
	work    []int
	ticks   int
	times   []int64
	steps   *[]string
	label   string
	failure error
	// ServiceFailure returns failure once the DoWork count passes
	// failureAfter.
	failureAfter int
	// clears hides failure again once the DoWork count passes clearAfter.
	clears     bool
	clearAfter int

	reportTerminated bool
	// IsTerminated returns true once the DoWork count passes
	// terminatedAfter.
	terminatedAfter int
}

func (f *fakeService) ServiceID() int32 { return f.id }

func (f *fakeService) DoWork(nowNs int64) int {
	if f.steps != nil {
		*f.steps = append(*f.steps, f.label)
	}

	f.times = append(f.times, nowNs)
	value := 0

	if f.ticks < len(f.work) {
		value = f.work[f.ticks]
	}

	f.ticks++
	return value
}

func (f *fakeService) ServiceFailure() error {
	if f.failure == nil || f.ticks <= f.failureAfter {
		return nil
	}

	if f.clears && f.ticks > f.clearAfter {
		return nil
	}
	return f.failure
}

func (f *fakeService) IsTerminated() bool {
	if !f.reportTerminated {
		return false
	}
	return f.ticks > f.terminatedAfter
}

func (f *fakeService) IsClosed() bool { return false }

func newTestRunner(
	transport transportInvoker,
	archive archiveInvoker,
	agent agentInvoker,
	idle idlestrategy.IdleStrategy,
	clock func() int64,
) *Runner {
	if archive == nil {
		archive = noArchiveInvoker{}
	}
	return &Runner{
		client:  transport,
		archive: archive,
		agent:   agent,
		idle:    idle,
		clock:   clock,
	}
}

func TestRunnerDoWorkOrderAndSum(t *testing.T) {
	var steps []string
	transport := &fakeTransport{work: []int{3}, steps: &steps}
	arch := &fakeArchive{work: []int{2}, steps: &steps}
	agent := &fakeAgent{work: []int{5}, steps: &steps}
	idle := &recordingIdle{steps: &steps}
	runner := newTestRunner(transport, arch, agent, idle, func() int64 { return 42 })

	require.Equal(t, 10, runner.DoWork())
	require.Equal(t, []string{"transport", "archive", "agent"}, steps)
	require.Equal(t, []int64{42}, agent.times)
	require.Empty(t, idle.counts, "DoWork never idles")
	require.Zero(t, idle.resets, "DoWork never resets")
}

func TestRunnerRunIdlesOncePerTick(t *testing.T) {
	var steps []string
	transport := &fakeTransport{work: []int{1, 0, 0}, steps: &steps}
	agent := &fakeAgent{work: []int{0, 2, 0}, steps: &steps}
	idle := &recordingIdle{steps: &steps}

	ctx, cancel := context.WithCancel(context.Background())
	tick := int64(0)
	clock := func() int64 {
		tick++
		if tick == 3 {
			cancel()
		}
		return tick
	}
	runner := newTestRunner(transport, nil, agent, idle, clock)

	require.ErrorIs(t, runner.Run(ctx), context.Canceled)
	require.Equal(t, 3, transport.ticks)
	require.Equal(t, 3, agent.ticks)
	require.Equal(t, []int{1, 2, 0}, idle.counts)
	require.Equal(t, 1, idle.resets)
	require.Equal(t, []string{
		"transport", "agent", "idle",
		"transport", "agent", "idle",
		"transport", "agent", "idle",
	}, steps)
}

func TestRunnerRunExitPaths(t *testing.T) {
	memberEnd := &consensus.TerminationError{Cause: errors.New("boom")}
	for _, tc := range []struct {
		name        string
		terminating bool
		memberEnd   error
		cancelled   bool
		wantErr     error
		wantTicks   int
	}{
		{
			name:      "alreadyCancelledRunsNoTick",
			cancelled: true,
			wantErr:   context.Canceled,
			wantTicks: 0,
		},
		{
			name:        "terminationBeatsIdle",
			terminating: true,
			wantErr:     ErrTransportTerminating,
			wantTicks:   1,
		},
		{
			name:      "memberTerminationEndsRun",
			memberEnd: memberEnd,
			wantErr:   memberEnd,
			wantTicks: 1,
		},
		{
			name:        "transportBeatsMember",
			terminating: true,
			memberEnd:   memberEnd,
			wantErr:     ErrTransportTerminating,
			wantTicks:   1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := &fakeTransport{terminating: tc.terminating}
			agent := &fakeAgent{endErr: tc.memberEnd}
			idle := &recordingIdle{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelled {
				cancel()
			}
			runner := newTestRunner(transport, nil, agent, idle, func() int64 { return 0 })

			err := runner.Run(ctx)
			require.ErrorIs(t, err, tc.wantErr)
			if tc.wantErr == memberEnd {
				require.Same(t, memberEnd, err, "Run returns the agent value unchanged")
				require.ErrorIs(t, err, consensus.ErrTerminated)
			}
			require.Equal(t, tc.wantTicks, transport.ticks)
			require.Empty(t, idle.counts, "Run returns before Idle on every exit path")
			require.Equal(t, 1, idle.resets, "Run always resets when it returns")
		})
	}
}

func TestNewRunnerValidatesDeps(t *testing.T) {
	complete := RunnerDeps{
		Client: &aeron.Client{},
		Agent:  consensus.NewAgent(consensus.AgentConfig{}),
		Idle:   &recordingIdle{},
		Clock:  func() int64 { return 0 },
	}

	for _, tc := range []struct {
		name string
		edit func(*RunnerDeps)
		want string
	}{
		{"nilClient", func(d *RunnerDeps) { d.Client = nil }, "cluster: Runner requires a Client"},
		{"nilAgent", func(d *RunnerDeps) { d.Agent = nil }, "cluster: Runner requires an Agent"},
		{"nilClock", func(d *RunnerDeps) { d.Clock = nil }, "cluster: Runner requires a Clock"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := complete
			tc.edit(&deps)
			runner, err := NewRunner(deps)
			require.EqualError(t, err, tc.want)
			require.Nil(t, runner)
		})
	}

	t.Run("nilIdleDefaults", func(t *testing.T) {
		deps := complete
		deps.Idle = nil
		runner, err := NewRunner(deps)
		require.NoError(t, err)
		require.NotNil(t, runner.idle)
	})

	t.Run("suppliedIdleIsKept", func(t *testing.T) {
		runner, err := NewRunner(complete)
		require.NoError(t, err)
		require.Same(t, complete.Idle, runner.idle)
	})

	t.Run("nilArchiveAccepted", func(t *testing.T) {
		deps := complete
		deps.Archive = nil
		runner, err := NewRunner(deps)
		require.NoError(t, err)
		require.NotNil(t, runner)
		require.Equal(t, noArchiveInvoker{}, runner.archive)
	})

	t.Run("nilArchiveZeroWork", func(t *testing.T) {
		transport := &fakeTransport{work: []int{1}}
		agent := &fakeAgent{work: []int{2}}
		runner := newTestRunner(transport, nil, agent, &recordingIdle{}, func() int64 { return 0 })
		require.Equal(t, 3, runner.DoWork())
		require.Equal(t, noArchiveInvoker{}, runner.archive)
	})
}

// TestRunnerRunWithServicesOrderAndSum asserts one tick runs transport,
// archive, agent, then each service. Idle sees the summed work.
func TestRunnerRunWithServicesOrderAndSum(t *testing.T) {
	var steps []string
	transport := &fakeTransport{work: []int{3}, steps: &steps}
	arch := &fakeArchive{work: []int{2}, steps: &steps}
	agent := &fakeAgent{work: []int{5}, steps: &steps}
	idle := &recordingIdle{steps: &steps}
	svc0 := &fakeService{id: 4, work: []int{7}, steps: &steps, label: "service 0"}
	svc1 := &fakeService{id: 9, work: []int{11}, steps: &steps, label: "service 1"}
	services := []ServiceAgent{svc0, svc1}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var reads int
	clock := func() int64 {
		reads++
		if reads == 2 {
			cancel()
		}
		return int64(40 + reads)
	}
	runner := newTestRunner(transport, arch, agent, idle, clock)

	require.ErrorIs(t, runner.RunWithServices(ctx, services...), context.Canceled)
	require.Equal(t, []string{
		"transport", "archive", "agent", "service 0", "service 1", "idle",
	}, steps)
	require.Equal(t, []int64{41}, agent.times)
	require.Equal(t, []int64{42}, svc0.times)
	require.Equal(t, []int64{42}, svc1.times)
	require.Equal(t, []int{3 + 2 + 5 + 7 + 11}, idle.counts)
	require.Equal(t, 1, idle.resets)
}

func TestRunnerRunWithServicesExitPaths(t *testing.T) {
	cause := errors.New("boom")
	other := errors.New("other")
	memberEnd := &consensus.TerminationError{Cause: errors.New("member")}
	grace := serviceFailureGraceNs

	for _, tc := range []struct {
		phase        string
		cancelBefore bool
		cancelOnRead int
		terminating  bool
		memberEnds   bool
		memberAt     int
		services     []*fakeService
		clock        []int64
		wantSame     error
		wantIs       []error
		wantNotIs    []error
		wantText     string
		wantTicks    int
		wantIdle     []int
	}{
		{
			phase:        "cancelled context",
			cancelBefore: true,
			services:     []*fakeService{{id: 3}},
			wantSame:     context.Canceled,
		},
		{
			phase:       "transport beats member and service failure",
			terminating: true,
			memberEnds:  true,
			services:    []*fakeService{{id: 1, failure: cause}},
			wantSame:    ErrTransportTerminating,
			wantTicks:   1,
		},
		{
			phase:      "member end beats service failure",
			memberEnds: true,
			services:   []*fakeService{{id: 1, failure: cause}},
			wantSame:   memberEnd,
			wantTicks:  1,
		},
		{
			phase:      "member end inside the grace",
			memberEnds: true,
			memberAt:   1,
			services:   []*fakeService{{id: 6, failure: cause}},
			clock:      []int64{0, 0, 1, grace - 1},
			wantSame:   memberEnd,
			wantTicks:  2,
			wantIdle:   []int{0},
		},
		{
			phase: "service failure at the deadline",
			services: []*fakeService{{
				id:      4,
				failure: cause,
			}},
			clock: []int64{
				5, 0,
				6, grace - 1,
				7, grace,
			},
			wantIs:    []error{cause},
			wantText:  "cluster: service 4: boom",
			wantTicks: 3,
			wantIdle:  []int{0, 0},
		},
		{
			phase: "lowest id wins, high id first",
			services: []*fakeService{
				{id: 8, failure: other},
				{id: 2, failure: cause},
			},
			clock:     []int64{0, 0, 0, grace},
			wantIs:    []error{cause},
			wantNotIs: []error{other},
			wantText:  "cluster: service 2: boom",
			wantTicks: 2,
			wantIdle:  []int{0},
		},
		{
			phase: "lowest id wins, low id first",
			services: []*fakeService{
				{id: 2, failure: cause},
				{id: 8, failure: other},
			},
			clock:     []int64{0, 0, 0, grace},
			wantIs:    []error{cause},
			wantNotIs: []error{other},
			wantText:  "cluster: service 2: boom",
			wantTicks: 2,
			wantIdle:  []int{0},
		},
		{
			phase: "terminated service until the member ends",
			services: []*fakeService{{
				id:               1,
				reportTerminated: true,
				terminatedAfter:  -1,
			}},
			memberEnds: true,
			memberAt:   2,
			wantSame:   memberEnd,
			wantTicks:  3,
			wantIdle:   []int{0, 0},
		},
		{
			phase:        "context cancel inside the grace",
			cancelOnRead: 2,
			services:     []*fakeService{{id: 1, failure: cause}},
			clock:        []int64{0, 1},
			wantSame:     context.Canceled,
			wantTicks:    1,
			wantIdle:     []int{0},
		},
		{
			phase: "deadline saturates at max int64",
			services: []*fakeService{{
				id:      3,
				failure: cause,
			}},
			clock: []int64{
				0, math.MaxInt64,
				0, math.MaxInt64 - 1,
				0, math.MaxInt64,
			},
			wantIs:    []error{cause},
			wantText:  "cluster: service 3: boom",
			wantTicks: 3,
			wantIdle:  []int{0, 0},
		},
	} {
		t.Run(tc.phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if tc.cancelBefore {
				cancel()
			}

			var reads int
			clock := func() int64 {
				reads++
				if tc.cancelOnRead > 0 && reads == tc.cancelOnRead {
					cancel()
				}

				if reads <= len(tc.clock) {
					return tc.clock[reads-1]
				}
				return 0
			}
			transport := &fakeTransport{terminating: tc.terminating}
			agent := &fakeAgent{}

			if tc.memberEnds {
				agent.endAt = tc.memberAt
				agent.endErr = memberEnd
			}

			services := make([]ServiceAgent, len(tc.services))

			for i, svc := range tc.services {
				services[i] = svc
			}

			idle := &recordingIdle{}
			runner := newTestRunner(transport, nil, agent, idle, clock)
			err := runner.RunWithServices(ctx, services...)

			if tc.wantSame != nil {
				require.Same(t, tc.wantSame, err)
			}

			if tc.wantText != "" {
				require.EqualError(t, err, tc.wantText)
			}

			for _, target := range tc.wantIs {
				require.ErrorIs(t, err, target)
			}

			for _, target := range tc.wantNotIs {
				require.NotErrorIs(t, err, target)
			}

			if tc.wantSame == memberEnd {
				require.ErrorIs(t, err, consensus.ErrTerminated)
			}

			require.Equal(t, tc.wantTicks, transport.ticks)
			require.Equal(t, tc.wantTicks, agent.ticks)

			for _, svc := range tc.services {
				require.Equal(t, tc.wantTicks, svc.ticks, "service %d", svc.id)
			}

			if tc.wantIdle == nil {
				require.Empty(t, idle.counts)
			} else {
				require.Equal(t, tc.wantIdle, idle.counts)
			}

			require.Equal(t, 1, idle.resets)
			require.Len(t, agent.liveness, len(tc.services))
		})
	}
}

func TestRunnerRunWithServicesWiring(t *testing.T) {
	ctx := context.Background()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	zeroClock := func() int64 { return 0 }

	t.Run("argument order before the first tick", func(t *testing.T) {
		s0 := &fakeService{id: 2}
		s1 := &fakeService{id: 0}
		s2 := &fakeService{id: 5}
		agent := &fakeAgent{}
		idle := &recordingIdle{}
		runner := newTestRunner(&fakeTransport{}, nil, agent, idle, zeroClock)

		require.ErrorIs(t, runner.RunWithServices(cancelled, s0, s1, s2), context.Canceled)
		require.Equal(t, []livenessSet{
			{id: 2, source: s0},
			{id: 0, source: s1},
			{id: 5, source: s2},
		}, agent.liveness)
		require.Zero(t, agent.ticks)
		require.Empty(t, idle.counts)
		require.Equal(t, 1, idle.resets)
	})

	t.Run("nil entry", func(t *testing.T) {
		agent := &fakeAgent{}
		idle := &recordingIdle{}
		runner := newTestRunner(&fakeTransport{}, nil, agent, idle, zeroClock)
		s0 := &fakeService{id: 1}

		err := runner.RunWithServices(ctx, nil, s0, &fakeService{id: 1})
		require.EqualError(t, err, "cluster: service at index 0 is nil")
		require.Empty(t, agent.liveness)
		require.Zero(t, agent.ticks)
		require.Empty(t, idle.counts)
		require.Zero(t, idle.resets)

		err = runner.RunWithServices(ctx, s0, nil)
		require.EqualError(t, err, "cluster: service at index 1 is nil")
		require.Empty(t, agent.liveness)
		require.Zero(t, idle.resets)
	})

	t.Run("repeated id", func(t *testing.T) {
		agent := &fakeAgent{}
		idle := &recordingIdle{}
		runner := newTestRunner(&fakeTransport{}, nil, agent, idle, zeroClock)

		err := runner.RunWithServices(ctx,
			&fakeService{id: 4},
			&fakeService{id: 2},
			&fakeService{id: 4},
		)
		require.EqualError(t, err, "cluster: service id 4 repeats")
		require.Empty(t, agent.liveness)
		require.Zero(t, agent.ticks)
		require.Empty(t, idle.counts)
		require.Zero(t, idle.resets)
	})

	t.Run("setter error", func(t *testing.T) {
		inner := errors.New("consensus: service id 9 out of range")
		agent := &fakeAgent{livenessErr: inner, failLivenessID: 9}
		transport := &fakeTransport{}
		idle := &recordingIdle{}
		runner := newTestRunner(transport, nil, agent, idle, zeroClock)

		err := runner.RunWithServices(ctx, &fakeService{id: 9})
		require.EqualError(t, err, "cluster: set service liveness: consensus: service id 9 out of range")
		require.ErrorIs(t, err, inner)
		require.Empty(t, agent.liveness)
		require.Zero(t, transport.ticks)
		require.Empty(t, idle.counts)
		require.Zero(t, idle.resets)
	})

	t.Run("earlier slot stays set", func(t *testing.T) {
		inner := errors.New("consensus: service id 5 out of range")
		agent := &fakeAgent{livenessErr: inner, failLivenessID: 5}
		idle := &recordingIdle{}
		runner := newTestRunner(&fakeTransport{}, nil, agent, idle, zeroClock)
		kept := &fakeService{id: 1}
		refused := &fakeService{id: 5}

		err := runner.RunWithServices(ctx, kept, refused)
		require.EqualError(t, err, "cluster: set service liveness: consensus: service id 5 out of range")
		require.ErrorIs(t, err, inner)
		require.Equal(t, []livenessSet{{id: 1, source: kept}}, agent.liveness)
		require.Zero(t, agent.ticks)
		require.Empty(t, idle.counts)
		require.Zero(t, idle.resets)
	})
}

func TestRunnerRunWithServicesNoServices(t *testing.T) {
	type outcome struct {
		steps  []string
		calls  int
		counts []int
		resets int
		err    error
	}

	run := func(t testing.TB, withServices bool) outcome {
		t.Helper()

		var got outcome
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		clock := func() int64 {
			got.calls++
			if got.calls == 3 {
				cancel()
			}
			return int64(got.calls)
		}
		transport := &fakeTransport{work: []int{1, 0, 0}, steps: &got.steps}
		agent := &fakeAgent{work: []int{0, 2, 0}, steps: &got.steps}
		idle := &recordingIdle{steps: &got.steps}
		runner := newTestRunner(transport, nil, agent, idle, clock)

		if withServices {
			got.err = runner.RunWithServices(ctx)
		} else {
			got.err = runner.Run(ctx)
		}

		require.Empty(t, agent.liveness)
		require.Equal(t, []int64{1, 2, 3}, agent.times)
		got.counts = idle.counts
		got.resets = idle.resets
		return got
	}

	plain := run(t, false)
	require.Equal(t, plain, run(t, true))
	require.Equal(t, 3, plain.calls)
	require.Equal(t, []int{1, 2, 0}, plain.counts)
	require.Equal(t, 1, plain.resets)
	require.ErrorIs(t, plain.err, context.Canceled)
}

// startFailService fails Start with err.
type startFailService struct {
	err error
}

func (s *startFailService) Start(service.Cluster) error { return s.err }

func (s *startFailService) SessionOpen(service.ClientSession, int64) {}

func (s *startFailService) SessionClose(service.ClientSession, int64, clusterwire.CloseReason) {}

func (s *startFailService) SessionMessage(service.ClientSession, int64, []byte) {}

func (s *startFailService) TimerEvent(int64, int64) {}

func TestRunnerRunWithServicesRealAgentFailure(t *testing.T) {
	cause := errors.New("start broke")
	svc := service.NewAgent(service.AgentConfig{
		ServiceID: 0,
		Service:   &startFailService{err: cause},
	})
	var reads []int64
	seq := []int64{
		10, 0,
		11, serviceFailureGraceNs - 1,
		12, serviceFailureGraceNs,
	}
	clock := func() int64 {
		var value int64
		if len(reads) < len(seq) {
			value = seq[len(reads)]
		}
		reads = append(reads, value)
		return value
	}
	agent := &fakeAgent{}
	idle := &recordingIdle{}
	runner := newTestRunner(&fakeTransport{}, nil, agent, idle, clock)

	err := runner.RunWithServices(context.Background(), svc)
	require.Equal(t, seq, reads)
	require.EqualError(t, err, "cluster: service 0: service: failed: service: start: start broke")
	require.ErrorIs(t, err, service.ErrFailed)
	require.ErrorIs(t, err, service.ErrStart)
	require.ErrorIs(t, err, cause)
	var ended *consensus.TerminationError
	require.NotErrorAs(t, err, &ended)
	require.Equal(t, []int{1, 0}, idle.counts)
	require.Equal(t, 1, idle.resets)
	require.Equal(t, []livenessSet{{id: 0, source: svc}}, agent.liveness)
}
