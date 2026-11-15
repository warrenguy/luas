package service

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/archive"
	"github.com/warrenguy/luas/cluster/codecs"
)

const (
	// DefaultControlChannel is the shared module-service control channel.
	// Matches upstream CONTROL_CHANNEL_DEFAULT.
	DefaultControlChannel = "aeron:ipc?term-length=128k"

	// DefaultServiceStreamID is the module-to-service control stream.
	// Matches upstream SERVICE_STREAM_ID_DEFAULT (104).
	DefaultServiceStreamID int32 = 104

	// DefaultConsensusModuleStreamID is the service-to-module control stream.
	// Matches upstream CONSENSUS_MODULE_STREAM_ID_DEFAULT (105).
	DefaultConsensusModuleStreamID int32 = 105

	// DefaultSnapshotChannel is the snapshot publication channel.
	// Matches upstream SNAPSHOT_CHANNEL_DEFAULT.
	DefaultSnapshotChannel = "aeron:ipc?alias=snapshot"

	// DefaultSnapshotStreamID is the snapshot publication stream.
	// Matches upstream SNAPSHOT_STREAM_ID_DEFAULT (106).
	DefaultSnapshotStreamID int32 = 106

	// MaxServiceCount is the most services one cluster member runs. Matches
	// upstream MAX_SERVICE_COUNT (10). The recovery state counter key holds
	// one snapshot recording id per service.
	MaxServiceCount = codecs.RecoveryStateMaxServiceCount

	// archiveResponseStreamBase is the upstream service archive response
	// stream for service 0 at cluster id 0.
	archiveResponseStreamBase = 100 + archive.DefaultControlResponseStreamID + 1

	ipcChannelPrefix = "aeron:ipc"
)

var (
	errAdderReturnedNoSubscription = errors.New("service: adder returned no subscription")
	errAdderReturnedNoPublication  = errors.New("service: adder returned no publication")
	errArchiveTransportNotClient   = errors.New(
		"service: Archive requires an *aeron.Client transport client",
	)
)

// SnapshotPublicationAdder adds the exclusive snapshot publication.
//
// Behaviour:
//   - OfferAddExclusivePublication starts an async add and returns its
//     correlation id.
//   - PollAddExclusivePublication runs one conductor tick.
//   - CancelPending abandons an in-flight add.
//
// As-built [*aeron.Client] may retry a full to-driver ring with idle sleep
// until DriverTimeout. Callers must treat OfferAddExclusivePublication as
// cold-path only.
//
// Concurrency: invoker-only.
type SnapshotPublicationAdder interface {
	OfferAddExclusivePublication(streamID int32, channel string) (int64, error)
	PollAddExclusivePublication(
		correlationID int64,
	) (*aeron.ExclusivePublication, bool, error)
	CancelPending(correlationID int64)
}

var _ SnapshotPublicationAdder = (*aeron.Client)(nil)

// TerminationReporter reports whether the transport client has entered
// termination.
type TerminationReporter interface {
	IsTerminating() bool
}

var _ TerminationReporter = (*aeron.Client)(nil)

// TransportClient registers control handles, snapshot publications, and
// response publications on a media driver.
//
// *aeron.Client satisfies this interface as-built.
type TransportClient interface {
	SubscriptionAdder
	SnapshotPublicationAdder
	ResponsePublicationAdder
	TerminationReporter
	AddPublication(streamID int32, channel string) (*aeron.Publication, error)
}

var _ TransportClient = (*aeron.Client)(nil)

// Config configures [Open]. Zero-value channel and stream id fields take the
// defaults stated per field. Open passes FragmentLimit, StartAck,
// SnapshotChannel, SnapshotStreamID, and ErrorHandler to [NewAgent].
type Config struct {
	// Aeron is the borrowed transport client. Required. The container does not
	// own Aeron. Open uses Aeron for the control pair. The agent uses Aeron to
	// add the log subscription at join time. The agent adds snapshot
	// publications and response publications through Aeron.
	//
	// Aeron may run in either conductor mode. With the invoker, Open calls
	// DoWork on Aeron while it waits for the archive connect or the recovery
	// state. Call Open on the goroutine that runs DoWork.
	//
	// With Archive set, Aeron must be an *aeron.Client. The archive client
	// shares Aeron.
	//
	// Close the container before Aeron. A closed Aeron is not terminating.
	// The agent then marks only when an ack offer or an add fails.
	Aeron TransportClient

	// Service receives log-event callbacks. Required for [Open]. A nil-service
	// Agent remains a unit-only inject through [NewAgent].
	Service ClusteredService

	// CommittedPosition supplies the committed log prefix this member treats
	// as safe for application consumption. Required.
	CommittedPosition func() int64

	// ServiceID is this container's service id. Must not be negative. Must
	// be below MaxServiceCount. Zero matches upstream SERVICE_ID_DEFAULT.
	ServiceID int32

	// ControlChannel is the shared control channel for the service stream and
	// the consensus module stream. Empty takes DefaultControlChannel.
	ControlChannel string

	// ServiceStreamID is the module-to-service control stream. Zero takes
	// DefaultServiceStreamID. Must differ from ConsensusModuleStreamID and
	// SnapshotStreamID after defaulting.
	ServiceStreamID int32

	// ConsensusModuleStreamID is the service-to-module control stream. Zero
	// takes DefaultConsensusModuleStreamID. Must differ from ServiceStreamID
	// and SnapshotStreamID after defaulting.
	ConsensusModuleStreamID int32

	// FragmentLimit caps fragments per log poll. Open passes FragmentLimit to
	// [NewAgent]. Zero or negative takes DefaultLogFragmentLimit there.
	FragmentLimit int

	// StartAck enables the spontaneous start ack a Java consensus module
	// consumes during recovery. See [StartAckConfig]. Must be nil with
	// Recovery set. Open passes this StartAck to [NewAgent] only when
	// Recovery is nil. Open refuses a config that sets neither.
	StartAck *StartAckConfig

	// Recovery enables recovery in Open. Nil keeps the cold start at log
	// position 0 when StartAck is set. Open refuses a config that sets
	// neither Recovery nor StartAck. Open leaves Recovery unchanged. See
	// [RecoveryConfig].
	Recovery *RecoveryConfig

	// Archive configures the archive client. Nil connects none: each
	// snapshot take then fails and acks -1. Open leaves Archive unchanged.
	//
	// Open applies WithRemoteControl, then these defaults:
	//   - empty ControlRequestChannel and ControlResponseChannel take
	//     archive.DefaultLocalControlChannel
	//   - zero ControlRequestStreamID takes archive.DefaultLocalControlStreamID
	//   - zero ControlResponseStreamID takes 121 + ServiceID
	//
	// Archive must not set Aeron, RecordingSignalHandler, LocalControlChannel,
	// or LocalControlStreamID. Both control channels must be IPC.
	// ControlResponseStreamID must differ from ControlRequestStreamID. Each
	// control stream id must differ from each container stream id after
	// defaulting.
	Archive *archive.Config

	// SnapshotChannel is the snapshot publication channel. Empty takes
	// DefaultSnapshotChannel. Must not set session-id=.
	SnapshotChannel string

	// SnapshotStreamID is the snapshot publication stream. Zero takes
	// DefaultSnapshotStreamID. Must differ from ServiceStreamID and
	// ConsensusModuleStreamID after defaulting.
	SnapshotStreamID int32

	// ErrorHandler receives service-path errors. [AgentConfig.ErrorHandler]
	// states the contract.
	//
	// After consensus.Agent.SetServiceLiveness takes the agent, a fatal error,
	// a termination stop, or [Agent.Close] ends the member. The module does
	// not read the agent before it takes the start ack, or in module state
	// TERMINATING or CLOSED.
	//
	// Without SetServiceLiveness, a fatal error does not end Runner.Run.
	// Agent.DoWork returns 0 after a fatal error or a termination stop. The
	// caller of DoWork reads [Agent.ServiceFailure] and [Agent.IsTerminated].
	ErrorHandler func(error)
}

// StartAckConfig configures the spontaneous start ack a Java consensus module
// consumes during recovery.
//
// Behaviour: the agent queues one start entry before any delivered control
// frame.
//
// Readiness:
//   - successful start work, or a nil Service: the agent sets the start
//     entry ready
//   - Start error: the agent sets the start entry ready with RelevantID
//     NULL_VALUE. The agent retries the start ack each tick until the ack
//     sends, a terminal offer code returns, Timeout passes, or the transport
//     client terminates. The agent then marks the Start error. With no bound
//     control publication, the agent marks the Start error at once.
//
// When the start entry is ready, drain sends it as ack id 0. Every later ack
// id shifts up by one. That is the Java numbering.
//
// Nil disables the start ack in [NewAgent]. Open refuses a config that
// sets neither StartAck nor Recovery. StartAck must not be set against a
// consensus module that pairs acks only with published frames. An unpaired
// first queued entry marks ServiceAckFailure.
type StartAckConfig struct {
	// LogPosition is the recovered log position the start ack carries. Must
	// equal the consensus module's expected ack position. A cold-start Java
	// consensus module expects 0. Must not be negative. Position 0 is a
	// value, not a default.
	LogPosition int64

	// RelevantID carries the transport client id on the member's media driver
	// ([aeron.Client.ClientId]). The Java consensus module stores RelevantID
	// for service liveness tracking. The Java consensus module aborts recovery
	// when any start ack carries the -1 sentinel. The agent sends the
	// caller-set value after a successful start. The agent sends -1 after
	// a Start error.
	RelevantID int64

	// Timeout caps the retry of the start ack after a Start error, on the
	// DoWork clock. The wait begins at the first tick after Start. Zero or
	// negative takes DefaultRecoveryTimeout. With Recovery set, Open takes
	// Recovery.Timeout.
	Timeout time.Duration
}

// Open assembles one service container against a transport client.
//
// Open order:
//  1. default the config
//  2. validate the config. With Archive set, build the archive config
//  3. with Recovery set, await the recovery state
//  4. control subscription on ServiceStreamID
//  5. control publication on ConsensusModuleStreamID
//  6. with Archive set, connect the archive client
//  7. proxy bind
//  8. agent build with control ingress, proxy, and archive client wired. The
//     transport client adds snapshot and response publications
//  9. with a snapshot recording in the recovery state, load the snapshot
//
// Behaviour: Open registers the two adds. With Archive set, Open then
// connects the archive client. The connect waits up to the archive
// ConnectTimeout. Without Recovery, Open does not wait for a consensus
// module.
//
// The snapshot load restores the client sessions, the time unit, and the app
// version. The load then calls [LoadSnapshotHandler.LoadSnapshot] when the
// service implements it.
//
// With Recovery set, the module awaits one start ack once Open reads the
// recovery state. Open sends the start ack with RelevantID NULL_VALUE when a
// later step fails:
//   - the snapshot recording id for ServiceID is negative
//   - the load path has a nil Archive
//   - the control subscription add fails
//   - the control publication add fails
//   - the archive connect fails
//   - the snapshot load fails
//
// Open repeats the offer while the offer code is retriable, up to
// Recovery.Timeout. Open pumps an invoker transport between offers. Open
// then tears down. Open does not send the ack when:
//   - the recovery wait fails
//   - the key does not decode
//   - the cluster id check fails
//   - the log position check fails
//   - the service id check fails
//
// Ownership: the container owns the control subscription, the control
// publication, and the archive client. The container does not own the
// transport client.
//
// Returns an error when:
//   - Aeron, Service, or CommittedPosition is nil
//   - ServiceID is negative or reaches MaxServiceCount
//   - StartAck is set and LogPosition is negative
//   - the two control stream ids collide after defaulting
//   - SnapshotChannel or SnapshotStreamID breaks the rule its godoc states
//   - Recovery and StartAck are both nil
//   - StartAck and Recovery are both set
//   - Recovery breaks a rule the [RecoveryConfig] godoc states
//   - Recovery is set and Aeron lacks CountersReader, DoWork or ClientId
//   - Archive is set and breaks a rule the Archive godoc states
//   - an archive control stream id equals Recovery.ReplayStreamID
//   - the recovery wait fails or exceeds Recovery.Timeout
//   - the recovery state key is invalid
//   - the key names a snapshot recording and Archive is nil
//   - either add fails or returns no handle
//   - Archive is set and Aeron is not an *aeron.Client
//   - the archive connect fails
//   - the snapshot load fails
//
// On error nothing stays open. A failed step closes the handles opened
// before it.
func Open(cfg Config) (*Container, error) {
	return openWithArchiveConnect(cfg, connectContainerArchive)
}

// archiveConnector connects the archive client and returns its replay
// surface.
type archiveConnector func(
	transport TransportClient,
	archiveCfg archive.Config,
) (*archive.Client, snapshotReplayControl, error)

// connectContainerArchive is the [archiveConnector] for [Open].
func connectContainerArchive(
	transport TransportClient,
	archiveCfg archive.Config,
) (*archive.Client, snapshotReplayControl, error) {
	client, err := connectServiceArchive(transport, archiveCfg)
	if err != nil {
		return nil, nil, err
	}
	return client, client, nil
}

// openWithArchiveConnect is the body of [Open]. connect runs step 6 when
// Archive is set.
func openWithArchiveConnect(cfg Config, connect archiveConnector) (*Container, error) {
	cfg = defaultConfig(cfg)
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	var archiveCfg archive.Config
	if cfg.Archive != nil {
		var err error
		archiveCfg, err = serviceArchiveConfig(cfg)
		if err != nil {
			return nil, err
		}
	}

	// validateConfig checked the recovery surface.
	transport, _ := cfg.Aeron.(recoveryTransport)
	var recovery recoveryPath
	if cfg.Recovery != nil {
		var err error
		recovery, err = awaitRecoveryPath(cfg, transport)
		if err != nil {
			return nil, err
		}
		if err := recovery.checkLoad(cfg); err != nil {
			sendFailedStartAck(cfg, transport, recovery.state, nil)
			return nil, err
		}
		cfg.StartAck = &StartAckConfig{
			LogPosition: recovery.state.LogPosition,
			RelevantID:  transport.ClientId(),
			Timeout:     cfg.Recovery.Timeout,
		}
	}

	sub, err := cfg.Aeron.AddSubscription(cfg.ServiceStreamID, cfg.ControlChannel)
	sub, err = addedHandle(sub, err, errAdderReturnedNoSubscription)
	if err != nil {
		sendFailedStartAck(cfg, transport, recovery.state, nil)
		return nil, err
	}

	pub, err := cfg.Aeron.AddPublication(cfg.ConsensusModuleStreamID, cfg.ControlChannel)
	pub, err = addedHandle(pub, err, errAdderReturnedNoPublication)
	if err != nil {
		sendFailedStartAck(cfg, transport, recovery.state, nil)
		_ = sub.Close()
		return nil, err
	}

	var (
		archiveClient *archive.Client
		replay        snapshotReplayControl
	)
	if cfg.Archive != nil {
		archiveClient, replay, err = connect(cfg.Aeron, archiveCfg)
		if err != nil {
			sendFailedStartAck(cfg, transport, recovery.state, pub)
			_ = pub.Close()
			_ = sub.Close()
			return nil, err
		}
	}

	proxy := NewConsensusModuleProxy()
	proxy.SetPublication(pub)
	ingress := NewSubscriptionControlIngress(sub)

	agent := NewAgent(AgentConfig{
		ServiceID:         cfg.ServiceID,
		Service:           cfg.Service,
		CommittedPosition: cfg.CommittedPosition,
		Subscriptions:     cfg.Aeron,
		FragmentLimit:     cfg.FragmentLimit,
		Control:           ingress,
		ModuleProxy:       proxy,
		StartAck:          cfg.StartAck,
		Archive:           archiveClient,

		SnapshotPublications: cfg.Aeron,
		SnapshotChannel:      cfg.SnapshotChannel,
		SnapshotStreamID:     cfg.SnapshotStreamID,
		ResponsePublications: cfg.Aeron,
		ErrorHandler:         cfg.ErrorHandler,
		Transport:            cfg.Aeron,
	})
	container := &Container{
		agent:         agent,
		ingress:       ingress,
		controlSub:    sub,
		controlPub:    pub,
		archiveClient: archiveClient,
	}
	if cfg.Recovery == nil {
		return container, nil
	}

	agent.seedRecovery(recovery.state)
	if !recovery.isLoad() {
		return container, nil
	}
	handler, _ := cfg.Service.(LoadSnapshotHandler)
	load := snapshotLoad{
		control:       replay,
		subscriptions: cfg.Aeron,
		pump:          transportPump(transport),
		channel:       cfg.Recovery.ReplayChannel,
		streamID:      cfg.Recovery.ReplayStreamID,
		timeout:       cfg.Recovery.Timeout,
		handler:       handler,
	}
	if err := load.run(agent, recovery.recordingID, recovery.state.LogPosition); err != nil {
		sendFailedStartAck(cfg, transport, recovery.state, pub)
		_ = container.Close()
		return nil, err
	}
	return container, nil
}

// serviceArchiveConfig builds the archive connect config from a copy of
// cfg.Archive, as [Config.Archive] documents. cfg must hold its defaults.
func serviceArchiveConfig(cfg Config) (archive.Config, error) {
	archiveCfg := *cfg.Archive
	switch {
	case archiveCfg.Aeron != nil:
		return archive.Config{}, errors.New("service: Archive must not set Aeron")
	case archiveCfg.RecordingSignalHandler != nil:
		return archive.Config{}, errors.New("service: Archive must not set RecordingSignalHandler")
	case archiveCfg.LocalControlChannel != "":
		return archive.Config{}, errors.New("service: Archive must not set LocalControlChannel")
	case archiveCfg.LocalControlStreamID != 0:
		return archive.Config{}, errors.New("service: Archive must not set LocalControlStreamID")
	}

	archiveCfg = archiveCfg.WithRemoteControl()
	archiveCfg.ControlRequestChannel = cmp.Or(
		archiveCfg.ControlRequestChannel,
		archive.DefaultLocalControlChannel,
	)
	archiveCfg.ControlResponseChannel = cmp.Or(
		archiveCfg.ControlResponseChannel,
		archive.DefaultLocalControlChannel,
	)
	archiveCfg.ControlRequestStreamID = cmp.Or(
		archiveCfg.ControlRequestStreamID,
		archive.DefaultLocalControlStreamID,
	)
	archiveCfg.ControlResponseStreamID = cmp.Or(
		archiveCfg.ControlResponseStreamID,
		archiveResponseStreamBase+cfg.ServiceID,
	)

	requestStreamID := archiveCfg.ControlRequestStreamID
	responseStreamID := archiveCfg.ControlResponseStreamID
	switch {
	case !strings.HasPrefix(archiveCfg.ControlRequestChannel, ipcChannelPrefix):
		return archive.Config{}, errors.New("service: archive ControlRequestChannel must be IPC")
	case !strings.HasPrefix(archiveCfg.ControlResponseChannel, ipcChannelPrefix):
		return archive.Config{}, errors.New("service: archive ControlResponseChannel must be IPC")
	case responseStreamID == requestStreamID:
		return archive.Config{}, errors.New(
			"service: archive ControlResponseStreamID and ControlRequestStreamID must differ",
		)
	case requestStreamID == cfg.ServiceStreamID:
		return archive.Config{}, errors.New(
			"service: archive ControlRequestStreamID and ServiceStreamID must differ",
		)
	case requestStreamID == cfg.ConsensusModuleStreamID:
		return archive.Config{}, errors.New(
			"service: archive ControlRequestStreamID and ConsensusModuleStreamID must differ",
		)
	case requestStreamID == cfg.SnapshotStreamID:
		return archive.Config{}, errors.New(
			"service: archive ControlRequestStreamID and SnapshotStreamID must differ",
		)
	case cfg.Recovery != nil && requestStreamID == cfg.Recovery.ReplayStreamID:
		return archive.Config{}, errors.New(
			"service: archive ControlRequestStreamID and ReplayStreamID must differ",
		)
	case responseStreamID == cfg.ServiceStreamID:
		return archive.Config{}, errors.New(
			"service: archive ControlResponseStreamID and ServiceStreamID must differ",
		)
	case responseStreamID == cfg.ConsensusModuleStreamID:
		return archive.Config{}, errors.New(
			"service: archive ControlResponseStreamID and ConsensusModuleStreamID must differ",
		)
	case responseStreamID == cfg.SnapshotStreamID:
		return archive.Config{}, errors.New(
			"service: archive ControlResponseStreamID and SnapshotStreamID must differ",
		)
	case cfg.Recovery != nil && responseStreamID == cfg.Recovery.ReplayStreamID:
		return archive.Config{}, errors.New(
			"service: archive ControlResponseStreamID and ReplayStreamID must differ",
		)
	}
	return archiveCfg, nil
}

// connectServiceArchive connects the archive client on transport. The
// returned client does not own transport.
//
// Returns an error when transport is not an *aeron.Client or the connect
// fails.
func connectServiceArchive(
	transport TransportClient,
	archiveCfg archive.Config,
) (*archive.Client, error) {
	client, ok := transport.(*aeron.Client)
	if !ok {
		return nil, errArchiveTransportNotClient
	}
	archiveCfg.Aeron = client
	return archive.Connect(context.Background(), archiveCfg)
}

func addedHandle[T any](handle *T, err, nilErr error) (*T, error) {
	if err != nil {
		return nil, err
	}
	if handle == nil {
		return nil, nilErr
	}
	return handle, nil
}

func defaultConfig(cfg Config) Config {
	cfg.ControlChannel = cmp.Or(cfg.ControlChannel, DefaultControlChannel)
	cfg.ServiceStreamID = cmp.Or(cfg.ServiceStreamID, DefaultServiceStreamID)
	cfg.ConsensusModuleStreamID = cmp.Or(cfg.ConsensusModuleStreamID, DefaultConsensusModuleStreamID)
	cfg.SnapshotChannel = cmp.Or(cfg.SnapshotChannel, DefaultSnapshotChannel)
	cfg.SnapshotStreamID = cmp.Or(cfg.SnapshotStreamID, DefaultSnapshotStreamID)

	if cfg.Recovery != nil {
		cfg.Recovery = defaultRecoveryConfig(*cfg.Recovery)
	}

	return cfg
}

func validateConfig(cfg Config) error {
	switch {
	case isNilOrTypedNil(cfg.Aeron):
		return errors.New("service: transport client is required")
	case isNilOrTypedNil(cfg.Service):
		return errors.New("service: clustered service is required")
	case cfg.CommittedPosition == nil:
		return errors.New("service: committed position is required")
	case cfg.ServiceID < 0:
		return errors.New("service: service id must not be negative")
	case int(cfg.ServiceID) >= MaxServiceCount:
		return fmt.Errorf("service: service id must not exceed %d", MaxServiceCount-1)
	case cfg.StartAck != nil && cfg.StartAck.LogPosition < 0:
		return errors.New("service: start ack log position must not be negative")
	case cfg.ServiceStreamID == cfg.ConsensusModuleStreamID:
		return errors.New("service: ServiceStreamID and ConsensusModuleStreamID must differ")
	case aeron.ChannelParamIndex(cfg.SnapshotChannel, aeron.ChannelSessionIDParam, true) >= 0:
		return errors.New("service: SnapshotChannel must not set session-id=")
	case cfg.SnapshotStreamID == cfg.ServiceStreamID:
		return errors.New("service: SnapshotStreamID and ServiceStreamID must differ")
	case cfg.SnapshotStreamID == cfg.ConsensusModuleStreamID:
		return errors.New("service: SnapshotStreamID and ConsensusModuleStreamID must differ")
	case cfg.Recovery == nil && cfg.StartAck == nil:
		return errors.New("service: Open needs Recovery or StartAck")
	}
	if cfg.Recovery != nil {
		return validateRecoveryConfig(cfg)
	}
	return nil
}

// isNilOrTypedNil reports an untyped nil or a nil pointer stored in an
// interface.
func isNilOrTypedNil(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer {
		return false
	}
	return rv.IsNil()
}

// Container is one assembled service host.
//
// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe from
// other goroutines.
//
// Lifetime: [Open] returns a ready container. [Container.Close] tears the
// container down.
//
// Ownership: owns the control subscription, the control publication, and the
// archive client. Does not own the transport client.
//
// Do not copy. Fields are pointers. A value copy shares the same handles.
type Container struct {
	agent         *Agent
	ingress       *SubscriptionControlIngress
	controlSub    *aeron.Subscription
	controlPub    *aeron.Publication
	archiveClient *archive.Client
}

// Agent returns the duty-cycle agent. Never nil on an open container. Stays
// non-nil after Close. The agent is inert after Close.
func (c *Container) Agent() *Agent {
	return c.agent
}

// ArchiveClient returns the archive client, or nil when Archive is nil or
// after Close.
//
// Ownership: the container owns the client. Callers must not close it. A
// snapshot take owns the recording signal handler and clears it at the end.
func (c *Container) ArchiveClient() *archive.Client {
	return c.archiveClient
}

// Close closes the agent, then the archive client, then clears control
// reassembly, then closes the control subscription, then the control
// publication.
//
// Behaviour: [Agent.Close] calls Terminate when the service started and did
// not stop. [Agent.Close] then ends any snapshot take, disconnects session
// egress, unbinds any log image, and closes any owned log subscription. The
// archive client closes after the agent, so take cleanup reaches an open
// client.
//
// Lifetime: idempotent. After Close the agent is inert. A second Close
// returns nil. A Terminate panic skips every close step. The next Close runs
// those steps and returns the joined errors. Safe on a nil receiver.
//
// Ownership: the container does not own the transport client.
//
// Returns the joined close errors, or nil.
//
// Concurrency: a service callback must not call Close.
func (c *Container) Close() error {
	if c == nil {
		return nil
	}
	if c.agent != nil {
		c.agent.Close()
	}
	var err error
	if c.archiveClient != nil {
		err = c.archiveClient.Close()
		c.archiveClient = nil
	}
	if c.ingress != nil {
		c.ingress.Clear()
		c.ingress = nil
	}
	if c.controlSub != nil {
		err = errors.Join(err, c.controlSub.Close())
		c.controlSub = nil
	}
	if c.controlPub != nil {
		err = errors.Join(err, c.controlPub.Close())
		c.controlPub = nil
	}
	return err
}
