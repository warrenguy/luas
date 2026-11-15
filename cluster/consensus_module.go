package cluster

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/archive"
	"github.com/warrenguy/luas/archive/codecs"
	"github.com/warrenguy/luas/cluster/consensus"
	"github.com/warrenguy/luas/cluster/election"
	"github.com/warrenguy/luas/cluster/recordinglog"
	"github.com/warrenguy/luas/idlestrategy"
)

// ConsensusModuleDeps are the collaborators [OpenConsensusModule] needs.
//
// Named for [RecoveryDeps], [ConsensusPublicationsDeps], and [LogStreamsDeps],
// which carry injected collaborators the same way.
type ConsensusModuleDeps struct {
	// Config is the member configuration. OpenConsensusModule normalizes and
	// validates it. OpenConsensusModule does not retain Config.
	Config Config
}

// ConsensusModule is one assembled consensus member.
//
// Open with [OpenConsensusModule]. The module owns every handle it opened.
//
// The module closes every handle it owns. A transport client supplied through
// Config.Aeron stays the caller's. The module does not own that client.
//
// Concurrency: invoker-only. Every method must run on the duty cycle goroutine
// in this order: aeron.Client.DoWork, then archive.Client.Poll, then
// consensus.Agent.DoWork. Exception: [ConsensusModule.CommitPosition] reads the
// mode-1 *atomic.Int64 and is safe from any goroutine.
//
// Lifetime: the module outlives the latest election handle. A settled member
// may install a further election from the open-time election factory. Each
// re-install of *election.Election updates m.election. Close clears that
// factory before it clears the runner. Call Close in caller teardown after the
// duty cycle has stopped.
//
// Do not copy. Most fields are pointers. A value copy shares the same handles.
//
// Both values look like owners responsible for Close.
type ConsensusModule struct {
	topology   *Topology
	client     *aeron.Client
	ownsClient bool

	// archiveClient is the module-owned archive control client.
	//
	// Close closes the archive client before the transport client.
	//
	// The archive control publication and subscription belong to that transport client.
	archiveClient *archive.Client

	recovery  *Recovery
	coldStart bool
	agent     *consensus.Agent
	pubs      *ConsensusPublications
	sub       *ConsensusSubscription

	// clientIngress is the local client ingress subscription.
	//
	// Every member opens the subscription. Only a steady-state leader accepts
	// connects.
	clientIngress *ClientIngressSubscription

	// serviceControlPub is the module-to-service control publication (stream 104).
	// Opened only when Config.ServiceCount > 0, before serviceControl.
	serviceControlPub *ServiceControlPublication

	// serviceControl is the service-to-module control subscription (stream 105).
	// Opened only when Config.ServiceCount > 0.
	serviceControl *ServiceControlSubscription

	// recoveryState is the recovery state counter (type 204). Nil when
	// ServiceCount is 0. The service start handler closes the counter when
	// the wait pops. Close closes the counter too.
	recoveryState *aeron.Counter

	// errorCounter is the module error counter (type 212).
	errorCounter *aeron.Counter

	// errorHandler reports module errors to errorCounter and
	// Config.ErrorHandler. Close stops it first.
	errorHandler *moduleErrorHandler

	// moduleState is the consensus module state counter (type 200).
	moduleState *aeron.Counter

	// controlToggle is the cluster control toggle counter (type 202).
	controlToggle *aeron.Counter

	// nodeControlToggle is the node control toggle counter (type 233).
	nodeControlToggle *aeron.Counter

	streams   *LogStreams
	host      *election.HostAdapter
	election  *election.Election
	idle      idlestrategy.IdleStrategy
	nanoClock func() int64

	logPublisher *consensus.LogPublisher
	logAdapter   *consensus.LogAdapter
	logRecording *logRecording

	// logReplay is the most recently built cluster log replay helper.
	//
	// Retained after the election releases the helper so:
	//   - Close can join the recorded close error
	//   - [ConsensusModule.LogReplayReason] can read the stop reason
	//
	// The module never drops the pointer while a replay is active. Every route
	// that exits a replay state closes the helper first.
	logReplay *logReplay

	// logReplication is the most recently built log replication helper.
	// Close stops it and clears it.
	logReplication *logReplication

	// unresolvedReplications holds replaced helpers whose stop did not resolve
	// before the election built the next helper. Close stops each one.
	unresolvedReplications []*logReplication

	// catchup is the follower catchup destination on the log subscription and
	// leader catchup replay control. Nil until wired after log streams open.
	catchup *catchupControl

	// reseedCtx bounds the reseed recording lookup. Open detaches cancel so a
	// canceled open context does not fail a later reseed.
	reseedCtx context.Context
}

func memberAeronConfig(cfg aeron.Config) aeron.Config {
	cfg.UseConductorInvoker = true
	return cfg
}

// OpenConsensusModule assembles one consensus member. Open binds every
// transport handle and agent dependency before it returns.
//
// ctx bounds the transport connect, the archive control connect, the term
// recording stop, its catalog wait, the recording-extent lookups the recovery
// plan makes, and the module snapshot load. When Config.Aeron is set, ctx
// still bounds the archive control connect.
//
// The log recording control and the log replay helpers keep Open's context
// values and detach cancel (context.WithoutCancel). Cancel after Open returns
// does not fail later start or stop awaits. ControlResponseTimeout bounds
// those awaits.
//
// Open blocks on:
//   - each counter add, bounded by the transport DriverTimeout
//   - the archive control connect, bounded by archive.DefaultConnectTimeout
//   - on a restart, the term recording stop and its catalog wait. The wait
//     ends ControlResponseTimeout after the stop reply. The wait checks that
//     deadline only between catalog lists
//   - on a plan that holds a snapshot, the module snapshot load. The image
//     wait and each fragment gap stop after one leader heartbeat timeout
//
// A process that opens several members in sequence does not service an
// earlier member's transport client during a block. Open every member's
// Archive before opening any member. Keep the total connect cost below each
// member's Client.InterServiceTimeout.
//
// With ServiceCount > 0, Open opens service control and adds the recovery
// state counter. A service container with Recovery set reads that counter in
// its Open. The election does not wait for the service start acks.
//
// Returns an error when:
//   - Config fails Validate
//   - Config.Aeron is set and HasConductorInvoker reports false
//   - a connect fails
//   - a counter add or lookup fails
//   - recovery or the snapshot load fails
//   - a stream fails to open
//
// On error Open closes every handle it opened. Open then releases the cluster
// directory lock and returns nil.
func OpenConsensusModule(
	ctx context.Context,
	deps ConsensusModuleDeps,
) (*ConsensusModule, error) {
	cfg := deps.Config.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	topology, err := NewTopology(cfg)
	if err != nil {
		return nil, err
	}

	module := &ConsensusModule{
		topology: topology,
		idle:     cfg.Idle,
	}

	succeeded := false
	defer func() {
		if !succeeded {
			_ = module.Close()
		}
	}()

	if cfg.Aeron != nil {
		if !cfg.Aeron.HasConductorInvoker() {
			return nil, errors.New("cluster: Aeron must run with UseConductorInvoker true")
		}

		module.client = cfg.Aeron
	} else {
		client, err := aeron.Connect(ctx, memberAeronConfig(cfg.AeronConfig))
		if err != nil {
			return nil, fmt.Errorf("cluster: connect aeron client: %w", err)
		}
		module.client = client
		module.ownsClient = true
	}

	errorCounter, err := addModuleErrorCounter(module.client)
	if err != nil {
		return nil, err
	}
	module.errorCounter = errorCounter

	counters, err := module.client.CountersReader()
	if err != nil {
		return nil, fmt.Errorf("cluster: counters reader: %w", err)
	}

	errorHandler, err := newModuleErrorHandler(counters, errorCounter.CounterID(), cfg.ErrorHandler)
	if err != nil {
		return nil, err
	}
	module.errorHandler = errorHandler
	handleError := errorHandler.handleError

	stateCounter, err := addModuleStateCounter(module.client)
	if err != nil {
		return nil, err
	}
	module.moduleState = stateCounter

	controlToggle, err := addClusterControlToggle(module.client)
	if err != nil {
		return nil, err
	}
	module.controlToggle = controlToggle

	nodeControlToggle, err := addNodeControlToggle(module.client)
	if err != nil {
		return nil, err
	}
	module.nodeControlToggle = nodeControlToggle

	archiveClient, err := archive.Connect(ctx, archiveConnectConfig(cfg, module.client))
	if err != nil {
		return nil, fmt.Errorf("cluster: connect archive client: %w", err)
	}
	module.archiveClient = archiveClient

	nanoClock, epochClock := moduleClocks(cfg)
	module.nanoClock = nanoClock

	lister := archive.NewLister(ctx, module.archiveClient)
	stopper := newTermRecordingStop(
		ctx,
		module.archiveClient,
		module.archiveClient.Config().ControlResponseTimeout.Nanoseconds(),
		nanoClock,
	)

	recovery, err := OpenRecovery(RecoveryDeps{
		Config:               cfg,
		ArchiveLister:        lister,
		TermRecordingStopper: stopper,
	})
	if err != nil {
		return nil, err
	}
	module.recovery = recovery
	module.coldStart = recovery.IsColdStart()
	electionCfg := recovery.ElectionConfig()

	// Seed commit at the plan replay start. ProposeMaxCommit only raises it,
	// so open is the sole absolute write.
	commit := new(atomic.Int64)
	commit.Store(recovery.Plan().SeedLogPosition())

	module.logPublisher = consensus.NewLogPublisher()
	module.logAdapter = consensus.NewLogAdapter(consensus.DefaultLogFragmentLimit)

	module.agent = consensus.NewAgent(moduleAgentConfig(cfg, electionCfg, commit, handleError))

	// Wire the recovered log recording id so term coherency can append. Skip
	// the null sentinel: a cold start has no log term to adopt.
	if id := recovery.Plan().TermRecordingID(); id != recordinglog.NullRecordingID {
		module.agent.SetLogRecordingID(id)
	}

	// Wire archive log recording control before log streams open.
	//
	// Nothing starts a recording at Open. Election join paths call
	// StartLogRecording. The same logRecording is the truncate control.
	//
	// Recovery stops the term recording before it plans. The extent then
	// holds a stop unless the catalog wait expired. Open seeds a concrete
	// recovery-extent stop when the plan holds one. A later successful truncate
	// stores that stop. Prepare cannot re-raise above the stop
	// [LogRecordingControl.StopPositionFor] reports.
	module.logRecording = newLogRecording(
		ctx,
		module.archiveClient,
		topology.LogChannel(),
		topology.LogStreamID(),
		recovery.RecordingLog(),
	)
	seedLogRecordingStopFromPlan(module.logRecording, recovery.Plan())
	module.agent.SetLogRecordingControl(module.logRecording)
	module.agent.SetLogTruncateControl(module.logRecording)

	// Duty-cycle Poll delivers an unsolicited STOP when the recorded image
	// closes. The handler marks a clean stop so IsActive matches the
	// archive session. The handler also sets the STOP mark the agent
	// consumes to enter an election.
	//
	// The snapshot STOP watch runs after the log recording handler, so the
	// log stop mark keeps its order. The election runs last. The election
	// passes the signal to its log replication helper.
	logRecording := module.logRecording
	snapshotControl := newSnapshotRecording(module.archiveClient)
	module.archiveClient.SetRecordingSignalHandler(archive.RecordingSignalHandlerFunc(
		func(event codecs.RecordingSignalEventFields) {
			logRecording.handleRecordingSignal(event)
			snapshotControl.handleRecordingSignal(event)
			module.handleElectionRecordingSignal(event)
		},
	))
	module.agent.SetSnapshotRecordingControl(snapshotControl)
	module.agent.SetSnapshotPublicationAdder(snapshotPublicationAdder{client: module.client})
	module.agent.SetSnapshotRecordingLog(recovery.RecordingLog(), cfg.RecordingLogFileSyncLevel)
	module.agent.SetTerminationRecordingLog(recovery.RecordingLog())
	module.agent.SetSnapshotStream(cfg.SnapshotChannel, cfg.SnapshotStreamID)

	if snapshots := recovery.Plan().Snapshots; len(snapshots) > 0 {
		loader := moduleSnapshotLoad{
			ctx:       ctx,
			control:   module.archiveClient,
			adder:     module.client,
			pump:      module.client.DoWork,
			nanoClock: module.nanoClock,
			channel:   topology.ReplayChannel(),
			streamID:  topology.ReplayStreamID(),
			timeoutNs: electionCfg.LeaderHeartbeatTimeoutNs,
		}
		if err := loader.run(module.agent, snapshots[0]); err != nil {
			return nil, err
		}
	}

	publisher := election.NewAeronPublisher()

	pubs, err := OpenConsensusPublications(ConsensusPublicationsDeps{
		Adder:     module.client,
		Topology:  topology,
		Publisher: publisher,
	})
	if err != nil {
		return nil, err
	}
	module.pubs = pubs

	sub, err := OpenConsensusSubscription(ConsensusSubscriptionDeps{
		Adder:    module.client,
		Topology: topology,
		Agent:    module.agent,
	})
	if err != nil {
		return nil, err
	}
	module.sub = sub

	appendSource := consensus.NewRecordingPosAppendPosition(
		module.agent,
		counters,
		module.archiveClient.ArchiveID(),
	)

	// Seed the exclusive log publication at the recovered append stop so a
	// restart join can extend the established recording. Nil seed means cold
	// start (no log term). Active-extent NullPosition floors to the committed
	// position and still seeds extent geometry.
	streams, err := OpenLogStreams(LogStreamsDeps{
		Adder:                      module.client,
		Topology:                   topology,
		Agent:                      module.agent,
		LogPublisher:               module.logPublisher,
		LogAdapter:                 module.logAdapter,
		AppendSource:               appendSource,
		PublicationInitialPosition: logPublicationSeedFromPlan(recovery.Plan()),
	})
	if err != nil {
		return nil, err
	}
	module.streams = streams

	module.host = election.NewHostAdapter(election.HostAdapterConfig{
		Agent:                    module.agent,
		Publisher:                publisher,
		Members:                  topology.Members(),
		ThisMember:               topology.ThisMember(),
		LeaderHeartbeatTimeoutNs: electionCfg.LeaderHeartbeatTimeoutNs,
		NanoClock:                nanoClock,
		LeaderMemberID:           election.NullMemberID,
		RecordingLogLookup:       recovery.RecordingLog(),
		AppVersion:               electionCfg.AppVersion,
	})
	module.agent.SetPositions(module.host)
	module.agent.SetEgress(module.host)

	// Never nil: a nil randomiser gives every member a zero nomination
	// back-off, and members that all nominate on the same tick split the
	// ballot and never elect.
	electionDeps := election.ElectionDeps{
		Handler:                   module.agent,
		Host:                      module.host,
		Publisher:                 publisher,
		NanoClock:                 nanoClock,
		EpochClock:                epochClock,
		NominationRand:            rand.Float64,
		NodeState:                 recovery.NodeState(),
		Members:                   topology.Members(),
		ThisMember:                topology.ThisMember(),
		RecordingLog:              recovery.RecordingLog(),
		RecordingLogLookup:        recovery.RecordingLog(),
		RecordingLogFileSyncLevel: cfg.RecordingLogFileSyncLevel,
		ErrorHandler:              handleError,
	}
	module.election = election.NewElection(electionCfg, electionDeps)
	module.agent.SetElection(module.election)
	module.agent.SetElectionFactory(trackModuleElection(
		module,
		newElectionFactory(
			electionCfg,
			electionDeps,
			recovery.Plan(),
		),
	))

	// Install after the election factory. Every election builds a replay helper
	// from the same deps, whether open-time or re-entered. The factory is a
	// closure only. No replay starts at Open.
	replayDeps := moduleLogReplayDeps(
		ctx,
		module,
		electionCfg.LeaderHeartbeatTimeoutNs,
		handleError,
	)
	module.agent.SetLogReplayFactory(func(logPosition, appendPosition int64) consensus.LogReplay {
		r := newLogReplay(replayDeps, logPosition, appendPosition)
		module.logReplay = r
		return r
	})

	// Every election builds a replication helper from these deps. Open does
	// not start a replication.
	archiveConfig := module.archiveClient.Config()
	archiveID := module.archiveClient.ArchiveID()
	replicationDeps := logReplicationDeps{
		Ctx:                         ctx,
		Control:                     archiveReplicationControl{client: module.archiveClient},
		Positions:                   cnc.NewRecordingPositionCache(counters, archiveID),
		NextCorrelationID:           module.client.NextCorrelationID,
		Agent:                       module.agent,
		Recording:                   module.logRecording,
		ReplicationChannel:          cfg.ReplicationChannel,
		LeaderArchiveControlChannel: cfg.LeaderArchiveControlChannel,
		ControlRequestStreamID:      archiveConfig.ControlRequestStreamID,
		ProgressCheckIntervalNs:     electionCfg.LeaderHeartbeatIntervalNs,
		ProgressTimeoutNs:           electionCfg.LeaderHeartbeatTimeoutNs,
		OfferTimeoutNs:              archiveConfig.ControlOfferRetryTimeout.Nanoseconds(),
		ResponseTimeoutNs:           archiveConfig.ControlResponseTimeout.Nanoseconds(),
	}
	module.agent.SetLogReplicationFactory(func(
		leaderArchiveEndpoint, responseArchiveEndpoint string,
		leaderRecordingID, stopPosition, nowNs int64,
	) consensus.LogReplication {
		r := newLogReplication(
			replicationDeps,
			leaderArchiveEndpoint,
			responseArchiveEndpoint,
			leaderRecordingID,
			stopPosition,
			nowNs,
		)
		module.trackLogReplication(r)
		return r
	})

	// Catchup control: follower catchup destination on the log subscription,
	// and leader archive replay of the local log recording.
	module.catchup = newCatchupControl(
		ctx,
		module.archiveClient,
		module.client,
		module.agent,
		module.streams,
		topology.LogStreamID(),
		topology.LogChannel(),
	)
	module.agent.SetCatchupControl(module.catchup)

	// A new leader reseeds the log publication so LOCAL recording extends the
	// same catalog id. TERM entries share one recording id.
	module.reseedCtx = moduleControlContext(ctx)
	module.agent.SetLogPublicationReseed(module.reseedLogPublication)

	clientIngress, err := OpenClientIngressSubscription(ClientIngressSubscriptionDeps{
		Adder:    module.client,
		Topology: topology,
		Agent:    module.agent,
	})
	if err != nil {
		return nil, err
	}
	module.clientIngress = clientIngress

	// Session egress: transport client as publication adder.
	// Topology.IngressEndpoints is REDIRECT detail.
	module.agent.SetEgressPublicationAdder(module.client)
	module.agent.SetIngressEndpoints(topology.IngressEndpoints())

	if cfg.ServiceCount > 0 {
		serviceControlPub, err := OpenServiceControlPublication(ServiceControlPublicationDeps{
			Adder:    module.client,
			Topology: topology,
			Agent:    module.agent,
		})
		if err != nil {
			return nil, err
		}
		module.serviceControlPub = serviceControlPub

		serviceControl, err := OpenServiceControlSubscription(ServiceControlSubscriptionDeps{
			Adder:    module.client,
			Topology: topology,
			Agent:    module.agent,
		})
		if err != nil {
			return nil, err
		}
		module.serviceControl = serviceControl

		// Add the counter last, so a failed earlier step never exposes a
		// recovery key.
		plan := recovery.Plan()
		counter, err := addRecoveryStateCounter(module.client, plan)
		if err != nil {
			return nil, err
		}
		module.recoveryState = counter
		if err := module.agent.AwaitServiceStart(plan.SeedLogPosition(), func() {
			_ = module.recoveryState.Close()
		}); err != nil {
			return nil, err
		}
	}

	module.agent.SetModuleStateCounter(moduleStateCounter{
		counterID: module.moduleState.CounterID(),
		reader:    counters,
	})
	module.agent.SetClusterControlToggle(Toggle{
		counterID: module.controlToggle.CounterID(),
		reader:    counters,
	})
	module.agent.SetNodeControlToggle(Toggle{
		counterID: module.nodeControlToggle.CounterID(),
		reader:    counters,
	})

	// Wire last. The steps above return errors and do not report them. Their
	// archive calls drop each unsolicited ERROR.
	wireArchiveFailure(module.errorHandler, module.agent, module.archiveClient)

	succeeded = true
	return module, nil
}

// reseedLogPublication rebuilds the exclusive log publication at position.
// Seeds term geometry from the local log recording descriptor. Returns the new
// publication session id.
func (m *ConsensusModule) reseedLogPublication(position int64) (int32, error) {
	recordingID := m.agent.LocalLogRecordingID()
	if recordingID < 0 {
		return 0, errors.New("cluster: reseed log publication: no local log recording")
	}
	desc, err := m.archiveClient.ListRecording(m.reseedCtx, recordingID)
	if err != nil {
		return 0, fmt.Errorf("cluster: reseed log publication: list recording: %w", err)
	}
	seed := &LogPublicationInitialPosition{
		Position:         position,
		InitialTermID:    desc.InitialTermID,
		TermBufferLength: desc.TermBufferLength,
		MtuLength:        desc.MtuLength,
	}
	if err := m.streams.ReseedPublicationAt(
		m.client,
		m.topology.LogPublicationChannel(),
		m.topology.LogStreamID(),
		seed,
	); err != nil {
		return 0, err
	}
	pub := m.streams.Publication()
	if pub == nil {
		return 0, errors.New("cluster: reseed log publication: publication unbound")
	}
	return pub.SessionID(), nil
}

func moduleClocks(cfg Config) (nanoClock, epochClock func() int64) {
	nanoClock = cfg.AeronConfig.NanoClock
	if nanoClock == nil {
		nanoClock = func() int64 { return time.Now().UnixNano() }
	}
	epochClock = cfg.AeronConfig.EpochClock
	if epochClock == nil {
		epochClock = func() int64 { return time.Now().UnixMilli() }
	}
	return nanoClock, epochClock
}

// trackLogReplication records r as the current log replication helper. The
// helper it replaces joins unresolvedReplications when the archive may still
// run its replication.
//
// Concurrency: invoker-only. The election factory calls it on the duty cycle.
func (m *ConsensusModule) trackLogReplication(r *logReplication) {
	if prior := m.logReplication; prior != nil && prior.hasUnresolvedStop() {
		m.unresolvedReplications = append(m.unresolvedReplications, prior)
	}
	m.logReplication = r
}

// Close tears the member down in reverse open order.
//
// Close sends ServiceTerminationPosition at the commit position. Close does
// not wait for a service ack.
//
// The log recording stop abandons a pending async stop. The blocking drain
// runs only when a start channel is still kept.
//
// Idempotent. Safe on a nil receiver.
//
// A transport client supplied through Config.Aeron stays open.
//
// Returns the joined close errors, or nil. After a lost archive, Close returns
// the errors of its failed archive stop requests.
func (m *ConsensusModule) Close() error {
	if m == nil {
		return nil
	}

	var err error

	// Stop the services first, so the counter and Config.ErrorHandler still
	// see a failed send.
	if m.agent != nil {
		m.agent.StopServicesAtCommit()
	}

	// Stop reports next. A late DoWork error then skips the counter and
	// Config.ErrorHandler.
	if m.errorHandler != nil {
		m.errorHandler.close()
	}

	// Clear each agent binding before closing the handle it references. A
	// late DoWork call must not install an election, accept a connect, or add
	// a publication.
	//
	// Order inside the block:
	//   - MarkClosed first. CLOSED is terminal, so the take cancel cannot
	//     store ACTIVE.
	//   - SetClientIngress(nil) before SetElection(nil). A leader with no
	//     election accepts connects.
	//   - CloseSessionEgress after SetClientIngress(nil), so a new accept
	//     cannot queue an egress publication.
	if m.agent != nil {
		m.agent.MarkClosed()
		m.agent.CloseSnapshotTake()
		m.agent.SetArchiveConnection(nil)
		m.agent.SetModuleStateCounter(nil)
		m.agent.SetClusterControlToggle(nil)
		m.agent.SetNodeControlToggle(nil)
		m.agent.SetSnapshotRecordingControl(nil)
		m.agent.SetSnapshotPublicationAdder(nil)
		m.agent.SetSnapshotRecordingLog(nil, recordinglog.FileSyncNone)
		m.agent.SetTerminationRecordingLog(nil)
		m.agent.SetClientIngress(nil)
		m.agent.SetServiceIngress(nil)
		m.agent.SetServiceProxy(nil)
		m.agent.SetSessionConnectHandler(nil)
		m.agent.CloseSessionEgress()
		m.agent.SetEgressPublicationAdder(nil)
		m.agent.SetElectionFactory(nil)
		m.agent.SetElection(nil)
		m.agent.SetPositions(nil)
		m.agent.SetEgress(nil)
		m.agent.SetLogReplayFactory(nil)
		m.agent.SetLogReplicationFactory(nil)
		m.agent.SetCatchupControl(nil)
		m.agent.SetLogPublicationReseed(nil)
	}
	if m.recoveryState != nil {
		err = errors.Join(err, m.recoveryState.Close())
		m.recoveryState = nil
	}
	if m.catchup != nil {
		err = errors.Join(err, m.catchup.close())
		m.catchup = nil
	}

	// Best-effort stop before the streams and the archive client close.
	// Fail-closed election paths own stop errors. Close still releases the
	// transport handles.
	if m.logRecording != nil {
		if stopErr := m.logRecording.StopLogRecording(); stopErr != nil {
			err = errors.Join(err, stopErr)
		}
		m.logRecording = nil
	}
	if m.agent != nil {
		m.agent.SetLogRecordingControl(nil)
		m.agent.SetLogTruncateControl(nil)
	}

	// StopReplay and StopReplication need a connected archive client, so the
	// election and the replay helpers close first. The replication stop blocks
	// until the archive replies.
	if m.election != nil {
		m.election.Close()
	}
	for _, rep := range m.unresolvedReplications {
		err = errors.Join(err, rep.stopReplication())
	}
	m.unresolvedReplications = nil
	if m.logReplication != nil {
		err = errors.Join(err, m.logReplication.stopReplication())
		m.logReplication = nil
	}
	if m.logReplay != nil {
		// Join the residual close error once, then clear it. Keep the residual
		// pointer for LogReplayReason. A second Close must not re-surface the
		// same error.
		err = errors.Join(err, m.logReplay.closeError())
		m.logReplay.closeErr = nil
	}
	if m.serviceControl != nil {
		err = errors.Join(err, m.serviceControl.Close())
		m.serviceControl = nil
	}
	if m.serviceControlPub != nil {
		err = errors.Join(err, m.serviceControlPub.Close())
		m.serviceControlPub = nil
	}
	if m.clientIngress != nil {
		err = errors.Join(err, m.clientIngress.Close())
		m.clientIngress = nil
	}
	if m.streams != nil {
		err = errors.Join(err, m.streams.Close())
		m.streams = nil
	}
	if m.sub != nil {
		err = errors.Join(err, m.sub.Close())
		m.sub = nil
	}
	if m.pubs != nil {
		err = errors.Join(err, m.pubs.Close())
		m.pubs = nil
	}

	// The transport client owns the archive control publication and
	// subscription, so the archive client closes first.
	if m.archiveClient != nil {
		err = errors.Join(err, m.archiveClient.Close())
		m.archiveClient = nil
	}

	// Close the module counters last before the transport client, in reverse
	// open order. The state counter reads CLOSED for the whole teardown.
	if m.nodeControlToggle != nil {
		err = errors.Join(err, m.nodeControlToggle.Close())
		m.nodeControlToggle = nil
	}
	if m.controlToggle != nil {
		err = errors.Join(err, m.controlToggle.Close())
		m.controlToggle = nil
	}
	if m.moduleState != nil {
		err = errors.Join(err, m.moduleState.Close())
		m.moduleState = nil
	}
	if m.errorCounter != nil {
		err = errors.Join(err, m.errorCounter.Close())
		m.errorCounter = nil
	}
	if m.client != nil && m.ownsClient {
		err = errors.Join(err, m.client.Close())
	}
	m.client = nil
	if m.recovery != nil {
		err = errors.Join(err, m.recovery.Close())
		m.recovery = nil
	}
	return err
}

// Client returns the transport client, or nil after Close.
//
// Pair the client with [ConsensusModule.ArchiveClient] and
// [ConsensusModule.Agent] to drive one duty cycle: transport DoWork, then
// archive Poll, then agent DoWork.
func (m *ConsensusModule) Client() *aeron.Client { return m.client }

// ArchiveClient returns the archive control client, or nil after Close.
//
// Pass the client to RunnerDeps.Archive so the duty cycle demuxes control
// responses and recording signals.
//
// Concurrency: call from the goroutine that runs [consensus.Agent.DoWork]. Not
// safe from other goroutines.
//
// Ownership: the module owns the client. Callers must not close the client or
// replace its error response handler.
func (m *ConsensusModule) ArchiveClient() *archive.Client { return m.archiveClient }

// Agent returns the consensus agent. It stays non-nil after Close, with every
// binding cleared, so a caller can assert teardown.
func (m *ConsensusModule) Agent() *consensus.Agent { return m.agent }

// Topology returns the resolved channel and stream map.
func (m *ConsensusModule) Topology() *Topology { return m.topology }

// ClientIngress returns the client ingress subscription, or nil after Close.
//
// For harness diagnostics. Do not Poll the subscription directly. The agent
// owns polling.
//
// Concurrency: invoker-only.
//
// Ownership: the module owns the subscription. Callers must not close the
// subscription.
func (m *ConsensusModule) ClientIngress() *ClientIngressSubscription {
	if m == nil {
		return nil
	}
	return m.clientIngress
}

// LogStreams returns the cluster log publication and subscription pair, or nil
// after Close.
func (m *ConsensusModule) LogStreams() *LogStreams { return m.streams }

// LogPublisher returns the cluster log publisher. It stays non-nil after Close,
// with its publication unbound.
func (m *ConsensusModule) LogPublisher() *consensus.LogPublisher {
	return m.logPublisher
}

// LogAdapter returns the cluster log adapter. It stays non-nil after Close.
//
// A follower holds a bound Image after it joins the live log; a leader holds
// none.
func (m *ConsensusModule) LogAdapter() *consensus.LogAdapter {
	return m.logAdapter
}

// RecordingLog returns the open recording log, or nil after Close.
func (m *ConsensusModule) RecordingLog() *recordinglog.Log {
	if m.recovery == nil {
		return nil
	}
	return m.recovery.RecordingLog()
}

// IdleStrategy returns the configured idle strategy. It is never nil: Normalize
// defaults it. The module never calls it; the duty-cycle owner does.
func (m *ConsensusModule) IdleStrategy() idlestrategy.IdleStrategy {
	return m.idle
}

// NanoClock returns the clock the election and the host adapter share. A duty
// cycle must pass this clock's value to Agent.DoWork so agent deadlines and
// election deadlines agree.
func (m *ConsensusModule) NanoClock() func() int64 { return m.nanoClock }

// Role returns the current cluster node role.
func (m *ConsensusModule) Role() consensus.Role { return m.agent.Role() }

func (m *ConsensusModule) ModuleState() consensus.ModuleState {
	return m.agent.ModuleState()
}

// IsLeader reports whether this member currently holds leadership.
func (m *ConsensusModule) IsLeader() bool {
	return m.agent.Role() == consensus.RoleLeader
}

// LeadershipTermID returns the current leadership term.
func (m *ConsensusModule) LeadershipTermID() int64 {
	return m.agent.LeadershipTermID()
}

// LeaderMemberID returns the known leader's member id.
//
// On the leader this is -1, not the leader's own id: JoinLogAsLeader clears the
// field because a leader has no append target. Assert -1, never self.
func (m *ConsensusModule) LeaderMemberID() int32 {
	return m.agent.LeaderMemberID()
}

// ThisMemberID returns this member's cluster id.
func (m *ConsensusModule) ThisMemberID() int32 {
	return m.agent.ThisMemberID()
}

// CommitPosition returns the mode-1 local commit value.
//
// Open seeds it at the recovery plan replay start. ProposeMaxCommit only
// raises it.
//
// Concurrency: safe from any goroutine. It reads an *atomic.Int64.
func (m *ConsensusModule) CommitPosition() int64 {
	return m.agent.CommitPosition()
}

// RequestServiceAck publishes RequestServiceAck (template 108) at logPosition
// and queues a coordination wait for the matching acks.
//
// Concurrency: invoker-only.
//
// Returns [consensus.ErrServiceControlUnbound] when the receiver is nil, the
// module opened with ServiceCount 0, or the proxy is not bound. Other errors
// come from [consensus.Agent.RequestServiceAck].
func (m *ConsensusModule) RequestServiceAck(logPosition int64) error {
	if m == nil || m.agent == nil {
		return consensus.ErrServiceControlUnbound
	}
	return m.agent.RequestServiceAck(logPosition)
}

// RequestSnapshot appends one ClusterActionRequest (action SNAPSHOT, flags 0)
// to the cluster log. nowNs is the wire Timestamp. The log publisher assigns
// LogPosition. The method queues one coordination wait at LogPosition.
//
// Concurrency: invoker-only.
//
// Returns errors from [consensus.Agent.RequestSnapshot].
func (m *ConsensusModule) RequestSnapshot(nowNs int64) error {
	return m.agent.RequestSnapshot(nowNs)
}

// NotifiedCommitPosition returns the follower-visible commit high-water.
//
// It advances only while this member is a follower, so a leader's value stays at
// whatever it held when leadership was won. Do not assert that it moves on a
// leader.
func (m *ConsensusModule) NotifiedCommitPosition() int64 {
	return m.agent.NotifiedCommitPosition()
}

// ElectionComplete reports whether the latest installed election completed.
//
// Behaviour:
//   - reads m.election (*election.Election most recently installed at open or
//     settled re-install)
//   - true when that machine is Closed
//   - false when m.election is nil, or the latest machine has not closed
//   - each settled re-install of *election.Election replaces m.election, so
//     this tracks the live machine again after re-entry
//   - Close does not clear m.election; a member closed mid election keeps
//     reporting false until that machine reaches Closed
//   - after Close on a settled member, reports whether the last installed
//     machine is Closed (post-Close dumps)
//
// Same handle as [ConsensusModule.ElectionState]. For install count and
// trigger, use [consensus.Agent.ElectionCount] and
// [consensus.Agent.LastElectionTrigger]. [consensus.Agent.Election] is non-nil
// only while a runner is live on the duty path.
func (m *ConsensusModule) ElectionComplete() bool {
	return m.election != nil && m.election.State() == election.ElectionStateClosed
}

// ElectionState returns the latest installed election state machine's state.
//
// Behaviour:
//   - same handle as [ConsensusModule.ElectionComplete]
//   - each settled re-install of *election.Election replaces m.election and
//     tracks the live *election.Election after re-entry
//   - after Close, reports the state the latest machine last reached
//   - Close does not clear m.election. [election.Election.Close] releases
//     helpers the election holds. Retaining m.election is what makes this
//     accessor answer after transport handles are gone
func (m *ConsensusModule) ElectionState() election.ElectionState {
	if m.election == nil {
		return election.ElectionStateInit
	}
	return m.election.State()
}

// ElectionLastError returns the error the current election machine last
// passed to [election.Election.HandleError].
//
// Behaviour:
//   - nil when no election machine is installed
//   - nil when the machine holds no error
//   - a settled re-install replaces the machine, so a fresh machine reports nil
//
// The error text may hold archive remote text and channel URIs. See
// [election.Election.LastError].
func (m *ConsensusModule) ElectionLastError() error {
	if m.election == nil {
		return nil
	}
	return m.election.LastError()
}

// handleElectionRecordingSignal passes one archive recording signal to the
// current election. Drops the signal when the election is nil.
//
// Concurrency: invoker-only. archive.Client.Poll calls it.
func (m *ConsensusModule) handleElectionRecordingSignal(event codecs.RecordingSignalEventFields) {
	if m.election == nil {
		return
	}
	m.election.OnRecordingSignal(
		event.CorrelationID,
		event.RecordingID,
		event.Position,
		event.Signal,
	)
}

// IsColdStart reports whether recovery found no usable cluster history.
//
// Snapshotted at open, so it keeps its meaning after Close.
func (m *ConsensusModule) IsColdStart() bool { return m.coldStart }

// LogReplayReason returns why the most recent cluster log replay stopped.
//
// Behaviour:
//   - non-empty reason when a hold expired, a replay ended before its bound,
//     or a replay could not start
//   - otherwise the empty string: no replay was built, a replay still runs,
//     or a replay reached its bound
//
// Lifetime: keeps its value after Close. A failure dump may still read the
// reason.
func (m *ConsensusModule) LogReplayReason() string {
	if m == nil || m.logReplay == nil {
		return ""
	}
	return m.logReplay.reason()
}

// LogRecordingStopPosition returns the archive stop position for the retained
// log recording id.
//
// Behaviour:
//   - recordinglog.NullPosition when the module is nil, no recording control is
//     wired, the local recording id is unknown, or the control cannot resolve a
//     stop
//   - after a truncate, the truncate position while the control holds the stop
//   - after a clean stop, the clean stop position for the retained id
//
// Does not report an active recording high-water. Active recordings report no
// stop until the control stops or truncates them.
//
// Concurrency: invoker-only.
func (m *ConsensusModule) LogRecordingStopPosition() int64 {
	if m == nil || m.logRecording == nil || m.agent == nil {
		return recordinglog.NullPosition
	}
	stopPosition, err := m.logRecording.StopPositionFor(m.agent.LocalLogRecordingID())
	if err != nil {
		return recordinglog.NullPosition
	}
	return stopPosition
}
