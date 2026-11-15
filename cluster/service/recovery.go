package service

import (
	"cmp"
	"errors"
	"fmt"
	"time"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/cluster/codecs"
	"github.com/warrenguy/luas/idlestrategy"
)

// DefaultRecoveryTimeout is the default [RecoveryConfig.Timeout]. Matches
// the archive DefaultConnectTimeout.
const DefaultRecoveryTimeout = 5 * time.Second

// DefaultReplayChannel is the snapshot replay channel. Matches upstream
// REPLAY_CHANNEL_DEFAULT.
const DefaultReplayChannel = "aeron:ipc"

// DefaultReplayStreamID is the snapshot replay stream. Matches upstream
// REPLAY_STREAM_ID_DEFAULT.
const DefaultReplayStreamID int32 = 103

// Recovery failure causes. Counter wait, path, and load errors wrap one of
// these with the failure detail. A counters reader failure wraps the reader
// error instead.
var (
	errRecoveryTransport = errors.New(
		"service: Recovery requires a transport client with CountersReader, DoWork and ClientId",
	)
	errRecoveryStateMissing = errors.New("service: recovery state counter not found")
	errRecoveryStateInvalid = errors.New("service: recovery state invalid")
	errSnapshotLoad         = errors.New("service: load snapshot")
)

// RecoveryConfig configures the recovery load in [Open].
//
// Behaviour: Open reads the first allocated recovery state counter (type
// 204). The counter key holds the log position, the cluster time, the
// leadership term, and one snapshot recording id per service.
//   - NULL_VALUE term: Open skips the load. The agent starts at the key log
//     position.
//   - any other term: Open loads this service's snapshot recording first.
//
// Open builds the start ack from the key: ack id 0, the key log position,
// and the transport client id as RelevantID. The consensus module must
// consume the start ack. [Open] names the later failures that send
// RelevantID NULL_VALUE. [Open] names the failures that do not send an
// ack. The agent sends RelevantID NULL_VALUE after a Start error.
type RecoveryConfig struct {
	// Timeout caps each recovery wait: the counter, the replay image, each
	// loader gap between fragments, and the failed start ack offer. Open
	// also sets [StartAckConfig.Timeout] to Timeout. Zero takes
	// DefaultRecoveryTimeout.
	// Must not be negative.
	//
	// The module must allocate the recovery state counter within Timeout
	// of Open. Otherwise open the container later, or raise Timeout.
	Timeout time.Duration

	// ReplayChannel is the snapshot replay channel. Empty takes
	// DefaultReplayChannel. Must not set session-id=. Open appends the
	// replay session id.
	ReplayChannel string

	// ReplayStreamID is the snapshot replay stream. Zero takes
	// DefaultReplayStreamID. Must differ from ServiceStreamID and
	// ConsensusModuleStreamID after defaulting.
	ReplayStreamID int32
}

// recoveryTransport is the transport client surface the recovery load
// uses.
type recoveryTransport interface {
	CountersReader() (*cnc.CountersReader, error)
	DoWork() int
	//nolint:staticcheck // Aeron Java API uses ClientId.
	ClientId() int64
}

var _ recoveryTransport = (*aeron.Client)(nil)

// transportPump returns transport.DoWork on an invoker transport, else a func
// that returns 0. A transport without a HasConductorInvoker method counts as
// an invoker transport.
func transportPump(transport recoveryTransport) func() int {
	reporter, hasInvokerMethod := transport.(interface{ HasConductorInvoker() bool })
	if hasInvokerMethod && !reporter.HasConductorInvoker() {
		return func() int { return 0 }
	}
	return transport.DoWork
}

// defaultRecoveryConfig returns a copy of recovery with the defaults set.
func defaultRecoveryConfig(recovery RecoveryConfig) *RecoveryConfig {
	recovery.Timeout = cmp.Or(recovery.Timeout, DefaultRecoveryTimeout)
	recovery.ReplayChannel = cmp.Or(recovery.ReplayChannel, DefaultReplayChannel)
	recovery.ReplayStreamID = cmp.Or(recovery.ReplayStreamID, DefaultReplayStreamID)

	return &recovery
}

// validateRecoveryConfig checks cfg.Recovery. cfg must hold its defaults.
func validateRecoveryConfig(cfg Config) error {
	recovery := cfg.Recovery
	switch {
	case cfg.StartAck != nil:
		return errors.New("service: StartAck and Recovery must not both be set")
	case recovery.Timeout < 0:
		return errors.New("service: Recovery Timeout must not be negative")
	case aeron.ChannelParamIndex(recovery.ReplayChannel, aeron.ChannelSessionIDParam, true) >= 0:
		return errors.New("service: Recovery ReplayChannel must not set session-id=")
	case recovery.ReplayStreamID == cfg.ServiceStreamID:
		return errors.New("service: Recovery ReplayStreamID and ServiceStreamID must differ")
	case recovery.ReplayStreamID == cfg.ConsensusModuleStreamID:
		return errors.New("service: Recovery ReplayStreamID and ConsensusModuleStreamID must differ")
	}
	if _, ok := cfg.Aeron.(recoveryTransport); !ok {
		return errRecoveryTransport
	}
	return nil
}

// awaitRecoveryState returns the key of the first allocated recovery state
// counter. It pumps an invoker transport between reads.
//
// Returns an error when:
//   - the counter does not appear within timeout
//   - the counters reader fails
//   - the key does not decode
//   - the cluster id is not 0. Config does not set a cluster id.
//   - the log position is negative
func awaitRecoveryState(
	transport recoveryTransport,
	timeout time.Duration,
) (codecs.RecoveryStateKeyFields, error) {
	deadline := time.Now().Add(timeout)
	pump := transportPump(transport)
	idle := idlestrategy.NewDefaultBackoff()
	for {
		reader, err := transport.CountersReader()
		if err != nil {
			return codecs.RecoveryStateKeyFields{}, fmt.Errorf(
				"service: read recovery state counter: %w", err,
			)
		}
		if counterID, found := reader.FindCounterOfType(codecs.RecoveryStateCounterTypeID); found {
			// allocated is false when the driver frees the counter between
			// the two reads.
			if key, allocated := reader.CounterKey(counterID); allocated {
				return checkRecoveryState(key[:])
			}
		}
		idle.Idle(pump())
		if !time.Now().Before(deadline) {
			return codecs.RecoveryStateKeyFields{}, fmt.Errorf(
				"%w after %s", errRecoveryStateMissing, timeout,
			)
		}
	}
}

// checkRecoveryState decodes key and applies the key checks of
// [awaitRecoveryState].
func checkRecoveryState(key []byte) (codecs.RecoveryStateKeyFields, error) {
	state, ok := codecs.TryDecodeRecoveryStateKeyAt(key, 0)
	switch {
	case !ok:
		return codecs.RecoveryStateKeyFields{}, fmt.Errorf(
			"%w: key does not decode", errRecoveryStateInvalid,
		)
	case state.ClusterID != 0:
		return codecs.RecoveryStateKeyFields{}, fmt.Errorf(
			"%w: cluster id %d, want 0", errRecoveryStateInvalid, state.ClusterID,
		)
	case state.LogPosition < 0:
		return codecs.RecoveryStateKeyFields{}, fmt.Errorf(
			"%w: log position %d", errRecoveryStateInvalid, state.LogPosition,
		)
	}
	return state, nil
}

// recoveryPath is the resolved recovery state of one service.
type recoveryPath struct {
	state       codecs.RecoveryStateKeyFields
	recordingID int64 // nullValue on the term-only path
}

// isLoad reports a load when the key leadership term differs from NULL_VALUE.
func (p recoveryPath) isLoad() bool {
	return p.state.LeadershipTermID != nullValue
}

// checkLoad returns nil on the term-only path.
//
// Returns an error when:
//   - the snapshot recording id is negative
//   - Archive is nil
func (p recoveryPath) checkLoad(cfg Config) error {
	switch {
	case !p.isLoad():
		return nil
	case p.recordingID < 0:
		return fmt.Errorf(
			"%w: snapshot recording id %d", errRecoveryStateInvalid, p.recordingID,
		)
	case cfg.Archive == nil:
		return fmt.Errorf(
			"%w: recording %d needs Archive", errSnapshotLoad, p.recordingID,
		)
	}
	return nil
}

// resolveRecoveryPath picks the recovery path for serviceID.
//
// Returns:
//   - the term-only path when the key term is NULL_VALUE
//   - the load path with the key recording id for serviceID otherwise.
//     The recording id may be negative. [recoveryPath.checkLoad] refuses
//     a negative id
//   - an error when the key does not hold a recording id for serviceID
func resolveRecoveryPath(
	state codecs.RecoveryStateKeyFields,
	serviceID int32,
) (recoveryPath, error) {
	if state.LeadershipTermID == nullValue {
		return recoveryPath{state: state, recordingID: nullValue}, nil
	}
	recordingID, ok := state.SnapshotRecordingID(serviceID)
	if !ok {
		return recoveryPath{}, fmt.Errorf(
			"%w: service id %d outside service count %d",
			errRecoveryStateInvalid,
			serviceID,
			len(state.SnapshotRecordingIDs),
		)
	}
	return recoveryPath{state: state, recordingID: recordingID}, nil
}

// awaitRecoveryPath waits for the recovery state counter on transport.
// awaitRecoveryPath then resolves the path for cfg.ServiceID. cfg must
// hold its defaults and pass validateConfig.
func awaitRecoveryPath(cfg Config, transport recoveryTransport) (recoveryPath, error) {
	state, err := awaitRecoveryState(transport, cfg.Recovery.Timeout)
	if err != nil {
		return recoveryPath{}, err
	}
	return resolveRecoveryPath(state, cfg.ServiceID)
}

// sendFailedStartAck offers the start ack of a failed recovery. The ack
// id is 0. The log position is the key log position. RelevantID is
// NULL_VALUE. The module that allocated the recovery state counter waits
// on the start ack. sendFailedStartAck returns without an offer when
// cfg.Recovery is nil.
//
// Behaviour: sendFailedStartAck adds the control publication when pub is
// nil. A failed add does not send the ack. Each PublishServiceAck call
// retries under [aeron.OfferAttempts]. sendFailedStartAck repeats a
// retriable offer until cfg.Recovery.Timeout passes. sendFailedStartAck
// pumps an invoker transport between offers. The pump lets a new
// publication connect. sendFailedStartAck does not send the ack on a terminal
// code.
// sendFailedStartAck does not send the ack when the timeout passes.
//
// Ownership: closes the publication added here. The caller owns a non-nil
// pub.
func sendFailedStartAck(
	cfg Config,
	transport recoveryTransport,
	state codecs.RecoveryStateKeyFields,
	pub *aeron.Publication,
) {
	if cfg.Recovery == nil {
		return
	}
	if pub == nil {
		added, err := cfg.Aeron.AddPublication(cfg.ConsensusModuleStreamID, cfg.ControlChannel)
		added, err = addedHandle(added, err, errAdderReturnedNoPublication)
		if err != nil {
			return
		}
		defer func() { _ = added.Close() }()
		pub = added
	}
	proxy := NewConsensusModuleProxy()
	proxy.SetPublication(pub)
	fields := codecs.ServiceAckFields{
		LogPosition: state.LogPosition,
		Timestamp:   state.Timestamp,
		AckID:       0,
		RelevantID:  nullValue,
		ServiceID:   cfg.ServiceID,
	}
	deadline := time.Now().Add(cfg.Recovery.Timeout)
	pump := transportPump(transport)
	idle := idlestrategy.NewDefaultBackoff()
	for {
		result := proxy.PublishServiceAck(fields)
		if result > 0 || !aeron.IsOfferRetriable(result) || !time.Now().Before(deadline) {
			return
		}
		idle.Idle(pump())
	}
}
