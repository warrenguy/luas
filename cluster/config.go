package cluster

import (
	"cmp"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/cluster/codecs"
	"github.com/warrenguy/luas/cluster/consensus"
	"github.com/warrenguy/luas/cluster/election"
	"github.com/warrenguy/luas/cluster/recordinglog"
	"github.com/warrenguy/luas/idlestrategy"
)

// Config holds member process configuration. Call Normalize before Validate and
// before NewTopology.
type Config struct {
	// MemberID is this member's cluster id. Must appear in ClusterMembers.
	MemberID int32

	// ClusterMembers is the members string in election.ParseMembers format:
	// id,ingress,consensus,log,catchup,archive[,archiveResponse][,egressResponse]
	// with a pipe after each member.
	ClusterMembers string

	// ClusterDir holds recording.log and node-state.dat.
	ClusterDir string

	// AeronDir is the directory of an already-running media driver. This package
	// never launches a driver. Required unless Aeron is set.
	AeronDir string

	// Aeron is an already-connected transport client to use instead of
	// connecting one. Nil means connect an owned client from AeronConfig and
	// close it at teardown. Non-nil means the caller owns the lifecycle.
	//
	// A shared client must run with UseConductorInvoker true. Open rejects a
	// client whose HasConductorInvoker reports false.
	Aeron *aeron.Client

	// AeronConfig is passed to aeron.Connect when Aeron is nil. aeron.Connect
	// applies transport defaults, not Normalize. Normalize only bridges
	// AeronDir into AeronConfig.AeronDir when the latter is empty.
	//
	// Open sets UseConductorInvoker true on a copy of AeronConfig, so the
	// member duty cycle can call DoWork on the owned client. The caller's
	// AeronConfig stays unchanged.
	//
	// Share AeronConfig.NanoClock and AeronConfig.EpochClock with the election
	// deps when a single clock source is wanted.
	//
	// AeronConfig.NanoClock must return nanoseconds since the Unix epoch. The
	// module stamps durable TERM rows and NewLeadershipTerm from it.
	AeronConfig aeron.Config

	// ConsensusChannel is the member-to-member control channel template. Must be
	// a udp channel and must not carry endpoint=; endpoints come from
	// ClusterMembers. Zero value takes DefaultConsensusChannel.
	ConsensusChannel string

	// LogChannel is the cluster log channel template. Same endpoint rule as
	// ConsensusChannel: every log endpoint comes from the members string. The
	// log endpoints select the mode:
	//   - equal on every row: one shared channel
	//   - distinct on every row: a manual multi-destination-cast log
	//   - partly shared: validation rejects the config
	//
	// Zero value takes DefaultLogChannel.
	LogChannel string

	// IngressChannel is the client ingress channel template. Must be a udp
	// channel and must not carry endpoint=. The ingress endpoint comes from
	// ClusterMembers (per-member, port offset 2). Zero value takes
	// DefaultIngressChannel.
	IngressChannel string

	// ReplayChannel is the local channel the Archive uses to replay this member's
	// own cluster log recording. Zero value takes DefaultReplayChannel.
	//
	// The replay is local. Archive delivers this member's recorded history to this
	// member's consensus module. Must not use the shared cluster log channel.
	// Must not set session-id=. The replay path appends session-id= per session.
	ReplayChannel string

	// ServiceControlChannel is the module-service control channel shared by the
	// service stream (104) and the consensus module stream (105). Zero value
	// takes DefaultServiceControlChannel.
	//
	// Keep IPC unless every service process is remote and the transport is
	// trusted.
	ServiceControlChannel string

	// SnapshotChannel is the channel of the module snapshot publication. The
	// local Archive records each module snapshot from it. Zero value takes
	// DefaultSnapshotChannel.
	//
	// Must not set session-id=. Each take appends session-id= for its own
	// publication.
	SnapshotChannel string

	// ReplicationChannel is the channel the local Archive receives a log
	// replication on. Empty uses the Archive's own replication channel. Must
	// be a udp channel when set.
	//
	// Required when a member in ClusterMembers has an archive response
	// endpoint. The replication then rewrites the channel to the response
	// endpoint.
	ReplicationChannel string

	// LeaderArchiveControlChannel is the template of the leader archive
	// control request channel for a log replication. Only its media and
	// interface= reach the request. The endpoint comes from ClusterMembers.
	// Must be a udp channel. Zero value takes
	// DefaultLeaderArchiveControlChannel.
	LeaderArchiveControlChannel string

	// ConsensusStreamID zero takes DefaultConsensusStreamID. Config stream ids
	// must be distinct.
	ConsensusStreamID int32

	// LogStreamID zero takes DefaultLogStreamID. Config stream ids must be
	// distinct.
	LogStreamID int32

	// ReplayStreamID zero takes DefaultReplayStreamID. Config stream ids must
	// be distinct.
	ReplayStreamID int32

	// IngressStreamID zero takes DefaultIngressStreamID. Config stream ids must
	// be distinct.
	IngressStreamID int32

	// ConsensusModuleStreamID zero takes DefaultConsensusModuleStreamID.
	// Service-to-module control (ServiceAck). Not consensus stream 108. Config
	// stream ids must be distinct.
	ConsensusModuleStreamID int32

	// ServiceStreamID zero takes DefaultServiceStreamID. Module-to-service
	// control (JoinLog, RequestServiceAck). Config stream ids must be distinct.
	ServiceStreamID int32

	// SnapshotStreamID zero takes DefaultSnapshotStreamID. Module snapshot
	// recording. Config stream ids must be distinct.
	SnapshotStreamID int32

	// ArchiveControlRequestChannel is the control request URI for this member.
	// Required.
	//
	// The URI reaches archive.Config.ControlRequestChannel with no rewrite.
	// Co-located Archive: IPC URI, typically archive.DefaultLocalControlChannel.
	// Remote Archive: udp URI with endpoint=.
	//
	// Normalize applies no default. Validate refuses an empty value.
	ArchiveControlRequestChannel string

	// ArchiveControlResponseChannel is the subscription URI for archive control
	// responses on this member. Required.
	//
	// Co-located Archive over IPC: same URI as ArchiveControlRequestChannel.
	// Remote Archive: udp URI; endpoint=host:0 lets the driver assign the port.
	// The module receives control responses on stream 120.
	//
	// Normalize applies no default. Validate refuses an empty value.
	ArchiveControlResponseChannel string

	// ServiceCount feeds recordinglog.NewRecoveryPlanConfig. Zero for a member
	// without clustered services. Must not be negative. Must not exceed
	// codecs.RecoveryStateMaxServiceCount.
	ServiceCount int

	// Election seeds the election state machine. Zero timeout fields take
	// election.DefaultElectionConfig values.
	//
	// Normalize overwrites MemberID, AppointedLeaderID, and
	// GracefulClosedLeaderID on this struct from Config.MemberID,
	// Config.AppointedLeaderID, and Config.GracefulClosedLeaderID. Setting those
	// three here does not take effect once Normalize runs. Validate rejects a
	// value that disagrees with the Config fields.
	//
	// The indirection exists because an int32 cannot distinguish "member 0" from
	// "unset". Election reads unset as election.NullMemberID, not zero.
	//
	// Election.LeaderHeartbeatTimeoutNs also caps each wait in an election log
	// replay. A raised value slows leader failure detection. When a service still
	// owes a snapshot ack this long after the replay image closes short of its
	// end of stream, the member ends with [ErrReplayHoldExpired].
	Election election.ElectionConfig

	// AppointedLeaderID restricts nomination to one member. Nil means no
	// appointed leader, which is what an ordinary election needs and what
	// Normalize produces from a zero Config.
	//
	// A non-nil value must name a member present in ClusterMembers. Every member
	// whose id differs then stops before the nominate transition, so a cluster
	// configured this way cannot elect a new leader if the appointed member is
	// down. Set it only for a deliberately fixed appointed leader.
	AppointedLeaderID *int32

	// GracefulClosedLeaderID names a leader that shut down cleanly, whose vote is
	// excluded from the unanimity fast path. Nil means none, which is what
	// Normalize produces from a zero Config.
	//
	// A non-nil value must name a member present in ClusterMembers. Leaving a
	// stale value set silently drops that member from every unanimity check.
	GracefulClosedLeaderID *int32

	// Idle paces the duty cycle. Zero value takes idlestrategy.NewDefaultBackoff.
	Idle idlestrategy.IdleStrategy

	// RecordingLogFileSyncLevel and NodeStateFileSyncLevel select durability for
	// recording.log and node-state.dat. Zero takes recordinglog.FileSyncData;
	// FileSyncNone is not reachable through this type.
	RecordingLogFileSyncLevel recordinglog.FileSyncLevel
	NodeStateFileSyncLevel    recordinglog.FileSyncLevel

	// Authenticator decides whether client sessions open. Nil runs the
	// consensus accept-all default: empty principal, never challenges, never
	// rejects.
	//
	// Ownership: the module does not own the authenticator. No Close duty.
	Authenticator consensus.Authenticator

	// Authoriser decides whether a session principal may run an admin
	// request. Nil refuses every admin request.
	//
	// Ownership: the module does not own the authoriser.
	Authoriser consensus.Authoriser

	// SessionTimeoutNs bounds pre-open session egress, queued egress events,
	// and open-session idle close. Zero takes the 10s upstream default
	// (consensus.DefaultSessionTimeoutNs). Negative fails Validate.
	SessionTimeoutNs int64

	// TerminationTimeoutNs bounds the leader's wait for follower termination
	// acks. The wait starts when the leader enters TERMINATING. It does not
	// bound the service acks. Zero takes the 10s upstream default
	// (consensus.DefaultTerminationTimeoutNs). Negative fails Validate.
	TerminationTimeoutNs int64

	// ErrorHandler receives each error the consensus module reports, including
	// each unsolicited archive ERROR. Nil drops each report.
	//
	// Behaviour:
	//   - The Cluster Errors counter (type 212) counts each report, even a
	//     dropped one.
	//   - After ConsensusModule.Close the module does not count a report.
	//     The module does not call ErrorHandler.
	//   - A fatal cause, an archive STORAGE_SPACE code,
	//     [ErrReplayHoldExpired], or a stopped service ends the member after
	//     its report.
	//
	// Match an election event with errors.Is(err, [election.ErrEvent]). Every
	// other error is a fault.
	//
	// Concurrency: ErrorHandler runs inside a duty cycle tick or a blocking
	// archive.Client method. ErrorHandler must not:
	//   - panic
	//   - block the duty cycle
	//   - call a Runner, ConsensusModule, or consensus.Agent method that ticks,
	//     sends, or changes state, except [consensus.Agent.TerminateOnError]
	//   - call archive.Client.Poll, a blocking archive.Client method, or a New*
	//     request builder
	//
	// ErrorHandler may call the ConsensusModule read accessors, except
	// LogRecordingStopPosition, which can block. For a failed step, the module
	// stores the result before the module calls ErrorHandler.
	//
	// The error text may hold archive remote text and channel URIs. Sanitize
	// the text before an untrusted sink.
	ErrorHandler func(error)
}

// Normalize applies defaults and returns a copy. Idempotent.
func (cfg Config) Normalize() Config {
	cfg.ConsensusChannel = cmp.Or(cfg.ConsensusChannel, DefaultConsensusChannel)
	cfg.LogChannel = cmp.Or(cfg.LogChannel, DefaultLogChannel)
	cfg.IngressChannel = cmp.Or(cfg.IngressChannel, DefaultIngressChannel)
	cfg.ReplayChannel = cmp.Or(cfg.ReplayChannel, DefaultReplayChannel)
	cfg.ServiceControlChannel = cmp.Or(cfg.ServiceControlChannel, DefaultServiceControlChannel)
	cfg.SnapshotChannel = cmp.Or(cfg.SnapshotChannel, DefaultSnapshotChannel)
	cfg.LeaderArchiveControlChannel = cmp.Or(
		cfg.LeaderArchiveControlChannel,
		DefaultLeaderArchiveControlChannel,
	)
	cfg.ConsensusStreamID = cmp.Or(cfg.ConsensusStreamID, DefaultConsensusStreamID)
	cfg.LogStreamID = cmp.Or(cfg.LogStreamID, DefaultLogStreamID)
	cfg.ReplayStreamID = cmp.Or(cfg.ReplayStreamID, DefaultReplayStreamID)
	cfg.IngressStreamID = cmp.Or(cfg.IngressStreamID, DefaultIngressStreamID)
	cfg.ConsensusModuleStreamID = cmp.Or(cfg.ConsensusModuleStreamID, DefaultConsensusModuleStreamID)
	cfg.ServiceStreamID = cmp.Or(cfg.ServiceStreamID, DefaultServiceStreamID)
	cfg.SnapshotStreamID = cmp.Or(cfg.SnapshotStreamID, DefaultSnapshotStreamID)
	cfg.RecordingLogFileSyncLevel = cmp.Or(cfg.RecordingLogFileSyncLevel, recordinglog.FileSyncData)
	cfg.NodeStateFileSyncLevel = cmp.Or(cfg.NodeStateFileSyncLevel, recordinglog.FileSyncData)

	if cfg.Idle == nil {
		cfg.Idle = idlestrategy.NewDefaultBackoff()
	}

	cfg.AeronConfig.AeronDir = cmp.Or(cfg.AeronConfig.AeronDir, cfg.AeronDir)

	defaults := election.DefaultElectionConfig()
	cfg.Election.ElectionTimeoutNs = cmp.Or(cfg.Election.ElectionTimeoutNs, defaults.ElectionTimeoutNs)
	cfg.Election.ElectionStatusIntervalNs = cmp.Or(
		cfg.Election.ElectionStatusIntervalNs,
		defaults.ElectionStatusIntervalNs,
	)
	cfg.Election.LeaderHeartbeatTimeoutNs = cmp.Or(
		cfg.Election.LeaderHeartbeatTimeoutNs,
		defaults.LeaderHeartbeatTimeoutNs,
	)
	cfg.Election.LeaderHeartbeatIntervalNs = cmp.Or(
		cfg.Election.LeaderHeartbeatIntervalNs,
		defaults.LeaderHeartbeatIntervalNs,
	)
	cfg.Election.StartupCanvassTimeoutNs = cmp.Or(
		cfg.Election.StartupCanvassTimeoutNs,
		defaults.StartupCanvassTimeoutNs,
	)

	// Always stamp member id and leader sentinels so a partially populated
	// Election cannot leave zero (member 0) in place of unset.
	cfg.Election.MemberID = cfg.MemberID
	cfg.Election.AppointedLeaderID = optionalMemberID(cfg.AppointedLeaderID)
	cfg.Election.GracefulClosedLeaderID = optionalMemberID(cfg.GracefulClosedLeaderID)

	return cfg
}

// Validate checks a normalized Config. All errors carry the cluster: prefix.
//
// Call Normalize first. Validate is safe on an unnormalized value but returns
// the first failed rule rather than applying defaults. Equal stream id pairs
// fail. A ReplayChannel or SnapshotChannel that sets session-id= fails.
func (cfg Config) Validate() error {
	_, _, err := cfg.validateMembers()
	return err
}

// validateMembers runs Validate and returns the parsed members plus the index of
// MemberID within them, so NewTopology can reuse both without a second
// ParseMembers or a second scan for the local member.
func (cfg Config) validateMembers() ([]election.Member, int, error) {
	if cfg.ClusterDir == "" {
		return nil, 0, errors.New("cluster: ClusterDir is required")
	}
	if cfg.AeronDir == "" && cfg.Aeron == nil {
		return nil, 0, errors.New("cluster: AeronDir is required when Aeron is not set")
	}
	if cfg.ClusterMembers == "" {
		return nil, 0, errors.New("cluster: ClusterMembers is required")
	}

	members, err := election.ParseMembers(cfg.ClusterMembers)
	if err != nil {
		return nil, 0, fmt.Errorf("cluster: invalid ClusterMembers: %w", err)
	}
	if len(members) == 0 {
		return nil, 0, errors.New("cluster: ClusterMembers must contain at least one member")
	}

	memberIndex := make(map[int32]int, len(members))
	for i, m := range members {
		if _, ok := memberIndex[m.ID]; ok {
			return nil, 0, fmt.Errorf("cluster: duplicate cluster member id %d", m.ID)
		}
		memberIndex[m.ID] = i
	}

	thisIndex, err := requireKnownMember(memberIndex, cfg.MemberID, "MemberID")
	if err != nil {
		return nil, 0, err
	}

	for _, m := range members {
		if err := validateMemberEndpoints(m); err != nil {
			return nil, 0, err
		}
	}

	if err := validateLogEndpointSharing(members); err != nil {
		return nil, 0, err
	}

	if err := validateChannelTemplate(cfg.ConsensusChannel, "ConsensusChannel"); err != nil {
		return nil, 0, err
	}
	if err := validateChannelTemplate(cfg.LogChannel, "LogChannel"); err != nil {
		return nil, 0, err
	}
	if err := validateChannelTemplate(cfg.IngressChannel, "IngressChannel"); err != nil {
		return nil, 0, err
	}
	if err := validateStreamIDsDistinct(cfg); err != nil {
		return nil, 0, err
	}
	if aeron.ChannelParamIndex(cfg.ReplayChannel, aeron.ChannelSessionIDParam, true) >= 0 {
		return nil, 0, errors.New("cluster: ReplayChannel must not set session-id=")
	}
	if aeron.ChannelParamIndex(cfg.SnapshotChannel, aeron.ChannelSessionIDParam, true) >= 0 {
		return nil, 0, errors.New("cluster: SnapshotChannel must not set session-id=")
	}
	if err := validateReplicationChannels(cfg, members); err != nil {
		return nil, 0, err
	}
	if cfg.ServiceCount < 0 {
		return nil, 0, errors.New("cluster: ServiceCount must not be negative")
	}
	if cfg.ServiceCount > codecs.RecoveryStateMaxServiceCount {
		return nil, 0, fmt.Errorf(
			"cluster: ServiceCount must not exceed %d", codecs.RecoveryStateMaxServiceCount,
		)
	}
	if cfg.SessionTimeoutNs < 0 {
		return nil, 0, errors.New("cluster: SessionTimeoutNs must not be negative")
	}
	if cfg.TerminationTimeoutNs < 0 {
		return nil, 0, errors.New("cluster: TerminationTimeoutNs must not be negative")
	}

	if err := election.ValidateElectionConfig(cfg.Election); err != nil {
		return nil, 0, fmt.Errorf("cluster: invalid Election config: %w", err)
	}
	if cfg.Election.MemberID != cfg.MemberID {
		return nil, 0, errors.New("cluster: Election.MemberID must equal MemberID")
	}

	if cfg.AppointedLeaderID != nil {
		if _, err := requireKnownMember(
			memberIndex,
			*cfg.AppointedLeaderID,
			"AppointedLeaderID",
		); err != nil {
			return nil, 0, err
		}
	}
	if cfg.GracefulClosedLeaderID != nil {
		if _, err := requireKnownMember(
			memberIndex,
			*cfg.GracefulClosedLeaderID,
			"GracefulClosedLeaderID",
		); err != nil {
			return nil, 0, err
		}
	}

	if cfg.Election.AppointedLeaderID != optionalMemberID(cfg.AppointedLeaderID) {
		return nil, 0, errors.New(
			"cluster: Election.AppointedLeaderID must match AppointedLeaderID",
		)
	}
	if cfg.Election.GracefulClosedLeaderID != optionalMemberID(cfg.GracefulClosedLeaderID) {
		return nil, 0, errors.New(
			"cluster: Election.GracefulClosedLeaderID must match GracefulClosedLeaderID",
		)
	}

	if cfg.Idle == nil {
		return nil, 0, errors.New("cluster: IdleStrategy is required")
	}
	if cfg.ArchiveControlRequestChannel == "" {
		return nil, 0, errors.New("cluster: ArchiveControlRequestChannel is required")
	}
	if cfg.ArchiveControlResponseChannel == "" {
		return nil, 0, errors.New("cluster: ArchiveControlResponseChannel is required")
	}
	if err := validateFileSyncLevel(
		cfg.RecordingLogFileSyncLevel,
		"RecordingLogFileSyncLevel",
	); err != nil {
		return nil, 0, err
	}
	if err := validateFileSyncLevel(
		cfg.NodeStateFileSyncLevel,
		"NodeStateFileSyncLevel",
	); err != nil {
		return nil, 0, err
	}
	return members, thisIndex, nil
}

// validateReplicationChannels checks the two log replication channels.
func validateReplicationChannels(cfg Config, members []election.Member) error {
	if cfg.ReplicationChannel == "" {
		for _, m := range members {
			if m.ArchiveResponseEndpoint != "" {
				return fmt.Errorf(
					"cluster: ReplicationChannel is required: member %d has an archive response endpoint",
					m.ID,
				)
			}
		}
	} else if !isUDPChannel(cfg.ReplicationChannel) {
		return errors.New("cluster: ReplicationChannel must be a udp channel")
	}
	if !isUDPChannel(cfg.LeaderArchiveControlChannel) {
		return errors.New("cluster: LeaderArchiveControlChannel must be a udp channel")
	}
	return nil
}

// optionalMemberID maps a nil Config leader pointer to election.NullMemberID.
// Non-nil keeps the pointed value, including zero (member 0).
func optionalMemberID(id *int32) int32 {
	if id == nil {
		return election.NullMemberID
	}
	return *id
}

// requireKnownMember returns the members-string index of id, and rejects an id
// missing from the ClusterMembers id set.
func requireKnownMember(memberIndex map[int32]int, id int32, field string) (int, error) {
	i, ok := memberIndex[id]
	if !ok {
		return 0, fmt.Errorf(
			"cluster: %s %d is not present in ClusterMembers",
			field,
			id,
		)
	}
	return i, nil
}

const udpChannelPrefix = "aeron:udp"

// endpointDelimiters are the channel URI characters an endpoint token must not
// contain: each one would split the parameter list at substitution.
const endpointDelimiters = "|?="

// isUDPChannel reports whether channel is a udp channel URI with an optional
// parameter list.
func isUDPChannel(channel string) bool {
	if !strings.HasPrefix(channel, udpChannelPrefix) {
		return false
	}
	rest := channel[len(udpChannelPrefix):]
	return rest == "" || strings.HasPrefix(rest, "?")
}

// validateChannelTemplate requires a udp channel template with no baked-in
// endpoint=; endpoints come from ClusterMembers.
func validateChannelTemplate(channel, field string) error {
	if !isUDPChannel(channel) {
		return errors.New("cluster: " + field + " must be a udp channel")
	}
	if aeron.ChannelEndpointParamIndex(channel) >= 0 {
		return errors.New("cluster: " + field + " must not set endpoint=")
	}
	return nil
}

// validateStreamIDsDistinct rejects any two cluster stream ids that collide.
//
// Every pair must differ. One stream id carrying two message kinds delivers
// each subscriber the other stream's frames. Walk order is fixed so the error
// string names the later id first.
func validateStreamIDsDistinct(cfg Config) error {
	// Fixed array. No heap alloc on the Validate path.
	streams := [...]struct {
		name string
		id   int32
	}{
		{name: "LogStreamID", id: cfg.LogStreamID},
		{name: "ConsensusStreamID", id: cfg.ConsensusStreamID},
		{name: "ReplayStreamID", id: cfg.ReplayStreamID},
		{name: "IngressStreamID", id: cfg.IngressStreamID},
		{name: "ConsensusModuleStreamID", id: cfg.ConsensusModuleStreamID},
		{name: "ServiceStreamID", id: cfg.ServiceStreamID},
		{name: "SnapshotStreamID", id: cfg.SnapshotStreamID},
	}

	for i, earlier := range streams {
		for _, later := range streams[i+1:] {
			if earlier.id == later.id {
				return errors.New(
					"cluster: " + later.name + " and " + earlier.name + " must differ",
				)
			}
		}
	}
	return nil
}

// validateFileSyncLevel rejects FileSyncNone and unknown levels. Only
// FileSyncData and FileSyncFull are allowed on this Config.
func validateFileSyncLevel(level recordinglog.FileSyncLevel, field string) error {
	if level != recordinglog.FileSyncData && level != recordinglog.FileSyncFull {
		return errors.New(
			"cluster: " + field + " must be FileSyncData or FileSyncFull",
		)
	}
	return nil
}

type memberEndpoint struct {
	name      string
	value     string
	checkPort bool
}

// validateLogEndpointSharing refuses a members string that shares some log
// endpoints and not others.
func validateLogEndpointSharing(members []election.Member) error {
	equal, distinct := classifyLogEndpoints(members)
	if equal || distinct {
		return nil
	}
	return errors.New(
		"cluster: ClusterMembers log endpoints must be all equal or all distinct",
	)
}

// classifyLogEndpoints reports whether log endpoints are all equal or all
// distinct. One member is equal, not distinct.
func classifyLogEndpoints(members []election.Member) (equal, distinct bool) {
	if len(members) < 2 {
		return true, false
	}

	equal, distinct = true, true

	for i := 1; i < len(members); i++ {
		endpoint := members[i].LogEndpoint
		equal = equal && endpoint == members[0].LogEndpoint

		for j := 0; distinct && j < i; j++ {
			distinct = endpoint != members[j].LogEndpoint
		}
	}

	return equal, distinct
}

func validateMemberEndpoints(m election.Member) error {
	// Fixed array: five required endpoints, no heap alloc on the validate path.
	endpoints := [...]memberEndpoint{
		{name: "ingress", value: m.IngressEndpoint, checkPort: true},
		{name: "consensus", value: m.ConsensusEndpoint, checkPort: true},
		{name: "log", value: m.LogEndpoint, checkPort: true},
		{name: "catchup", value: m.CatchupEndpoint},
		{name: "archive", value: m.ArchiveEndpoint},
	}
	for _, ep := range endpoints {
		if ep.value == "" {
			return fmt.Errorf(
				"cluster: member %d has an empty %s endpoint",
				m.ID,
				ep.name,
			)
		}
		if strings.ContainsAny(ep.value, endpointDelimiters) {
			return fmt.Errorf(
				"cluster: member %d has an invalid %s endpoint: %s",
				m.ID,
				ep.name,
				ep.value,
			)
		}
		if ep.checkPort {
			if err := validateEndpointPort(m.ID, ep.name, ep.value); err != nil {
				return err
			}
		}
	}
	// The archive response endpoint is optional. A delimiter in it would split
	// the replication response channel at substitution.
	responseEndpoint := m.ArchiveResponseEndpoint
	if responseEndpoint != "" && strings.ContainsAny(responseEndpoint, endpointDelimiters) {
		return fmt.Errorf(
			"cluster: member %d has an invalid archive response endpoint: %s",
			m.ID,
			responseEndpoint,
		)
	}
	return nil
}

// validateEndpointPort checks that ingress, consensus, and log endpoints carry
// a numeric port in MinValidPort..MaxValidPort. Split on the last colon so
// bracketed IPv6 works.
func validateEndpointPort(memberID int32, name, endpoint string) error {
	colon := strings.LastIndex(endpoint, ":")
	if colon < 0 {
		return fmt.Errorf(
			"cluster: member %d has a %s endpoint without a port: %s",
			memberID,
			name,
			endpoint,
		)
	}
	port, err := strconv.Atoi(endpoint[colon+1:])
	if err != nil || !IsValidPort(port) {
		return fmt.Errorf(
			"cluster: member %d has an out-of-range %s endpoint port: %s",
			memberID,
			name,
			endpoint,
		)
	}
	return nil
}
