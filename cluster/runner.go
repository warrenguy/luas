package cluster

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/archive"
	"github.com/warrenguy/luas/cluster/consensus"
	"github.com/warrenguy/luas/idlestrategy"
)

// ErrTransportTerminating reports that [Runner.Run] or
// [Runner.RunWithServices] stopped because the transport client entered
// termination.
//
// Causes:
//   - media-driver shutdown
//   - stale keepalive
//   - client or service timeout
//
// The sentinel is not a context error.
//
// A closed client does not produce this sentinel.
var ErrTransportTerminating = errors.New("cluster: transport client is terminating")

// transportInvoker is the transport half of one duty tick.
type transportInvoker interface {
	DoWork() int
	IsTerminating() bool
}

// archiveInvoker is the archive-control half of one duty tick.
type archiveInvoker interface {
	Poll() int
}

// noArchiveInvoker reports zero work when RunnerDeps.Archive is nil.
//
// [NewRunner] installs noArchiveInvoker so [Runner.DoWork] has no nil branch
// and no Runner holds a typed-nil archive.Client.
type noArchiveInvoker struct{}

func (noArchiveInvoker) Poll() int { return 0 }

// agentInvoker is the consensus half of one duty tick.
type agentInvoker interface {
	DoWork(nowNs int64) int
	TerminationError() error
	SetServiceLiveness(serviceID int32, source consensus.ServiceLiveness) error
}

// ServiceAgent is one co-hosted clustered service.
//
// *service.Agent satisfies ServiceAgent.
//
// The service must use [RunnerDeps.Client]. [Runner.RunWithServices] does
// not tick another transport client.
type ServiceAgent interface {
	consensus.ServiceLiveness
	ServiceID() int32
	DoWork(nowNs int64) int
}

// serviceFailureGraceNs is the wait after the first service failure. The
// wait spans ten slow tick intervals.
const serviceFailureGraceNs int64 = 100_000_000

// serviceFailureWatch holds the service-failure deadline for one run.
type serviceFailureWatch struct {
	seen     bool
	deadline int64
}

// RunnerDeps are the collaborators [NewRunner] needs.
//
// Named for [ConsensusModuleDeps] and [RecoveryDeps], which carry injected
// collaborators the same way.
//
// Ownership: [Runner] borrows every field. [Runner] does not close them.
// [Runner] does not outlive them.
type RunnerDeps struct {
	// Client is the transport client whose conductor the duty cycle services.
	// Required. Pass ConsensusModule.Client.
	Client *aeron.Client

	// Agent is the consensus agent the duty cycle ticks after the transport client
	// and the archive control poll. Required. Pass ConsensusModule.Agent.
	Agent *consensus.Agent

	// Archive is the archive control client the duty cycle demuxes. Pass
	// ConsensusModule.ArchiveClient when wiring a ConsensusModule.
	//
	// Nil is allowed. A nil Archive reports zero archive work every tick.
	// OpenConsensusModule supplies a non-nil client. Nil is for tests that
	// drive the agent without an Archive.
	Archive *archive.Client

	// Idle paces [Runner.Run] and [Runner.RunWithServices]. A nil Idle takes
	// idlestrategy.NewDefaultBackoff.
	//
	// A strategy carries per-loop state. Strategies are not safe to share.
	// Every Runner must use its own strategy.
	//
	// Pass ConsensusModule.IdleStrategy when that value is per-module.
	// IdleStrategy is per-module when each member's Config supplies its own
	// value, or when Idle stays nil and Config.Normalize fills it.
	Idle idlestrategy.IdleStrategy

	// Clock reads nanoseconds for Agent.DoWork and each service DoWork.
	// Required. Agent deadlines and election deadlines must share one
	// function. Pass ConsensusModule.NanoClock.
	Clock func() int64
}

// Runner drives one consensus member's duty cycle on the calling goroutine.
//
// Build with [NewRunner]. Drive one tick with [Runner.DoWork]. Drive the
// member loop with [Runner.Run]. Drive the member and its services with
// [Runner.RunWithServices].
//
//	module, err := cluster.OpenConsensusModule(ctx, deps)
//	if err != nil {
//		return err
//	}
//	defer module.Close()
//	runner, err := cluster.NewRunner(cluster.RunnerDeps{
//		Client:  module.Client(),
//		Agent:   module.Agent(),
//		Archive: module.ArchiveClient(),
//		Idle:    module.IdleStrategy(),
//		Clock:   module.NanoClock(),
//	})
//	if err != nil {
//		return err
//	}
//	return runner.Run(ctx)
//
// Without [Runner.RunWithServices], set each liveness source with
// [consensus.Agent.SetServiceLiveness]. Each tick, call [Runner.DoWork]. Then
// call each service DoWork on the same goroutine.
//
// Behaviour: [ConsensusModule.Close] does not stop [Runner.Run] or
// [Runner.RunWithServices]. A closed transport client does not stop either
// loop. [Runner.Run] then calls Idle(0) until the context ends. Each loop
// returns [consensus.TerminationError] after one tick when the member ends
// before Close.
//
// Stop the duty cycle before closing the module. Do not reuse a Runner across a
// close.
//
// Concurrency: call from the goroutine that runs [Runner.DoWork]. Not safe
// from other goroutines. Other goroutines must not call non-atomic agent
// accessors.
//
// Lifetime: Runner remains a usable Go value after [ConsensusModule.Close].
// [Runner.DoWork] then reports zero work from each closed handle: the transport
// client, the archive control client, and the consensus subscription.
//
// Ownership: Runner does not own its collaborators.
//
// Do not copy. A copy shares the idle strategy. Strategies are not safe to
// share.
type Runner struct {
	client  transportInvoker
	archive archiveInvoker
	agent   agentInvoker
	idle    idlestrategy.IdleStrategy
	clock   func() int64
}

// NewRunner binds one duty cycle over a transport client, an optional archive
// control client, and a consensus agent.
//
// Client, Agent, and Clock have no safe default. A nil transport client
// services nothing and reports no error. A nil agent panics on the first tick.
// A defaulted clock would be a second clock the election does not share.
//
// Archive takes a default. A nil Archive installs a no-op invoker that reports
// zero work. OpenConsensusModule supplies a non-nil client. Nil is for tests
// that drive the agent without an Archive.
//
// A nil Idle is filled with idlestrategy.NewDefaultBackoff, matching
// [Config.Normalize].
//
// Rebuilding after [ConsensusModule.Close] fails here rather than later.
// ConsensusModule.Client is nil once closed.
//
// Returns an error when:
//   - Client is nil
//   - Agent is nil
//   - Clock is nil
func NewRunner(deps RunnerDeps) (*Runner, error) {
	if deps.Client == nil {
		return nil, errors.New("cluster: Runner requires a Client")
	}
	if deps.Agent == nil {
		return nil, errors.New("cluster: Runner requires an Agent")
	}
	if deps.Clock == nil {
		return nil, errors.New("cluster: Runner requires a Clock")
	}
	idle := deps.Idle
	if idle == nil {
		idle = idlestrategy.NewDefaultBackoff()
	}
	var archivePoll archiveInvoker = noArchiveInvoker{}
	if deps.Archive != nil {
		archivePoll = deps.Archive
	}
	return &Runner{
		client:  deps.Client,
		archive: archivePoll,
		agent:   deps.Agent,
		idle:    idle,
		clock:   deps.Clock,
	}, nil
}

// DoWork runs one duty tick and returns the total work count.
// Order is transport client, then archive control poll, then the agent at the
// clock's current value.
//
// The transport runs first. Archive control responses arrive on a subscription
// the transport conductor services. The agent acts on those demux effects.
// Reversing the order costs one tick of latency per hop.
//
// DoWork never idles. DoWork never sleeps. The caller paces repeated calls.
//
// Each call performs bounded work: one conductor service tick, one bounded
// archive demux poll (DefaultControlResponseFragmentLimit, ten fragments), and
// one bounded ingress poll. Progress needs repeated calls.
//
// DoWork drives one member. All three parts run in one call.
//
// A multi-member duty cycle that must service every transport before any agent
// must drive the module accessors directly. Do not call DoWork or Run on each
// Runner for that shape.
func (r *Runner) DoWork() int {
	return r.client.DoWork() + r.archive.Poll() + r.agent.DoWork(r.clock())
}

// Run pumps [Runner.DoWork]. Run paces the loop with the idle strategy.
//
// Returns:
//   - ctx.Err when ctx is done
//   - [ErrTransportTerminating] when the transport client enters termination,
//     even when the member terminates in the same tick
//   - the agent's [consensus.TerminationError] when the member terminates.
//     A planned termination has cause [consensus.ErrClusterTerminated]. A
//     service stop matches [consensus.ErrServiceStopped] under errors.Is.
//
// A caller that drives [Runner.DoWork] reads [consensus.Agent.TerminationError]
// itself.
//
// Run does not run a tick after ctx ends.
//
// Run resets the idle strategy on return. Run does not close or drain any
// collaborator. Close the module after Run returns.
func (r *Runner) Run(ctx context.Context) error {
	return r.run(ctx, nil)
}

// RunWithServices pumps the member and each service on the calling goroutine.
//
// Before the first tick, RunWithServices sets each service as the liveness
// source for its ServiceID. A source already set for that id is replaced.
// RunWithServices does not clear those slots on return.
//
// Each tick runs [Runner.DoWork], then each service DoWork in argument order.
// The services share one fresh [RunnerDeps.Clock] read.
//
// Returns:
//   - a start error when an entry is nil, two entries share a ServiceID, or
//     [consensus.Agent.SetServiceLiveness] fails
//   - ctx.Err when ctx is done
//   - [ErrTransportTerminating] when the transport client enters termination.
//     The transport end wins over a member end in that tick.
//   - the agent's [consensus.TerminationError], unchanged, when the member
//     terminates. The member end wins over a service failure in that tick.
//   - "cluster: service %d: %w" when a service failure outlasts a 100ms wait
//     while the member still runs. The lowest failing ServiceID wins. A tick
//     with no failure resets the wait. IsTerminated alone does not stop the
//     loop.
//
// RunWithServices resets the idle strategy on return. Close each service
// after the call returns. Then close the module.
//
// Concurrency: a service callback must not call [Runner.DoWork],
// [Runner.Run], a service DoWork, or Close on its agent or its container.
// Other goroutines may set a control toggle.
func (r *Runner) RunWithServices(ctx context.Context, services ...ServiceAgent) error {
	if err := r.wireServices(services); err != nil {
		return err
	}
	return r.run(ctx, services)
}

func (r *Runner) wireServices(services []ServiceAgent) error {
	for i, svc := range services {
		if svc == nil {
			return fmt.Errorf("cluster: service at index %d is nil", i)
		}
	}

	for i, svc := range services {
		id := svc.ServiceID()

		for j := i + 1; j < len(services); j++ {
			if services[j].ServiceID() == id {
				return fmt.Errorf("cluster: service id %d repeats", id)
			}
		}
	}

	for _, svc := range services {
		err := r.agent.SetServiceLiveness(svc.ServiceID(), svc)
		if err != nil {
			return fmt.Errorf("cluster: set service liveness: %w", err)
		}
	}
	return nil
}

func (r *Runner) run(ctx context.Context, services []ServiceAgent) error {
	defer r.idle.Reset()

	var watch serviceFailureWatch

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		work, err := r.tick(services, &watch)
		if err != nil {
			return err
		}

		r.idle.Idle(work)
	}
}

// tick runs one duty tick. tick then applies the transport, member, and
// service-failure checks.
func (r *Runner) tick(services []ServiceAgent, watch *serviceFailureWatch) (int, error) {
	work := r.DoWork()

	if len(services) == 0 {
		return work, r.stopAfterMember()
	}

	nowNs := r.clock()

	for _, svc := range services {
		work += svc.DoWork(nowNs)
	}

	err := r.stopAfterMember()
	if err != nil {
		return work, err
	}
	return work, watch.stop(services, nowNs)
}

func (r *Runner) stopAfterMember() error {
	if r.client.IsTerminating() {
		return ErrTransportTerminating
	}
	return r.agent.TerminationError()
}

func (watch *serviceFailureWatch) stop(services []ServiceAgent, nowNs int64) error {
	id, failure := lowestServiceFailure(services)
	if failure == nil {
		watch.seen = false
		return nil
	}

	if !watch.seen {
		watch.seen = true
		watch.deadline = serviceFailureDeadline(nowNs)
		return nil
	}

	if nowNs < watch.deadline {
		return nil
	}
	return fmt.Errorf("cluster: service %d: %w", id, failure)
}

func lowestServiceFailure(services []ServiceAgent) (int32, error) {
	var id int32
	var failure error

	for _, svc := range services {
		next := svc.ServiceFailure()
		if next == nil {
			continue
		}

		nextID := svc.ServiceID()
		if failure == nil || nextID < id {
			id, failure = nextID, next
		}
	}
	return id, failure
}

func serviceFailureDeadline(nowNs int64) int64 {
	if nowNs > math.MaxInt64-serviceFailureGraceNs {
		return math.MaxInt64
	}
	return nowNs + serviceFailureGraceNs
}
