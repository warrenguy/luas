//go:build testhooks

package service

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/archive"
	"github.com/warrenguy/luas/atomicbuffer"
	"github.com/warrenguy/luas/cluster/codecs"
	"github.com/warrenguy/luas/cluster/codecs/clusterwire"
)

const (
	recoveryFakeCounterCount            = 4
	recoveryFakeClientID          int64 = 4_242
	recoveryKeyServiceCountOffset       = 3*atomicbuffer.SizeInt64 + atomicbuffer.SizeInt32
)

// recoveryFakeTransport implements recoveryTransport over fakeTransport and
// fabricated counters buffers.
type recoveryFakeTransport struct {
	*fakeTransport

	meta      []byte
	values    []byte
	reader    *cnc.CountersReader
	readerErr error

	pumps    int
	pumpHook func(pumps int)
	reads    int
	readHook func(reads int)

	replay snapshotLoadAdder
}

var _ recoveryTransport = (*recoveryFakeTransport)(nil)

func newRecoveryFakeTransport(tb testing.TB) *recoveryFakeTransport {
	tb.Helper()
	meta := make([]byte, recoveryFakeCounterCount*cnc.CounterMetadataLength)
	values := make([]byte, recoveryFakeCounterCount*cnc.CounterValueLength)
	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(tb, err)
	return &recoveryFakeTransport{
		fakeTransport: newFakeTransport(tb),
		meta:          meta,
		values:        values,
		reader:        reader,
	}
}

func (f *recoveryFakeTransport) CountersReader() (*cnc.CountersReader, error) {
	f.reads++
	if f.readHook != nil {
		f.readHook(f.reads)
	}

	if f.readerErr != nil {
		return nil, f.readerErr
	}
	return f.reader, nil
}

func (f *recoveryFakeTransport) DoWork() int {
	f.pumps++
	if f.pumpHook != nil {
		f.pumpHook(f.pumps)
	}
	return 0
}

//nolint:staticcheck // Aeron Java API uses ClientId.
func (*recoveryFakeTransport) ClientId() int64 {
	return recoveryFakeClientID
}

// AddSubscription returns a fresh hook subscription for the replay stream.
// Other streams take the shared fakeTransport subscription.
func (f *recoveryFakeTransport) AddSubscription(
	streamID int32,
	channel string,
) (*aeron.Subscription, error) {
	if streamID != DefaultReplayStreamID {
		return f.fakeTransport.AddSubscription(streamID, channel)
	}
	return f.replay.AddSubscription(streamID, channel)
}

func encodeRecoveryKey(tb testing.TB, fields codecs.RecoveryStateKeyFields) []byte {
	tb.Helper()
	key := make([]byte, cnc.CounterMetadataMaxKeyLength)
	end, ok := codecs.EncodeRecoveryStateKey(key, 0, fields)
	require.True(tb, ok)
	return key[:end]
}

func (f *recoveryFakeTransport) writeRecoveryCounter(
	tb testing.TB,
	counterID int32,
	state int32,
	key []byte,
) {
	tb.Helper()
	require.NoError(tb, cnc.WriteCounterRecordWithKey(
		f.meta,
		f.values,
		counterID,
		state,
		codecs.RecoveryStateCounterTypeID,
		key,
		"recovery state",
		0,
		0,
	))
}

// writeRecoveryState writes an allocated recovery state counter at id 0.
func (f *recoveryFakeTransport) writeRecoveryState(
	tb testing.TB,
	fields codecs.RecoveryStateKeyFields,
) {
	tb.Helper()
	f.writeRecoveryCounter(tb, 0, cnc.CounterRecordAllocated, encodeRecoveryKey(tb, fields))
}

// recoveryKeyPastMaxServiceCount encodes a key whose recording-id list is
// empty. The service count is codecs.RecoveryStateMaxServiceCount plus one.
func recoveryKeyPastMaxServiceCount(tb testing.TB) []byte {
	tb.Helper()
	key := encodeRecoveryKey(tb, codecs.RecoveryStateKeyFields{LeadershipTermID: 1})
	atomicbuffer.Wrap(key).PutInt32(
		recoveryKeyServiceCountOffset, int32(codecs.RecoveryStateMaxServiceCount+1),
	)
	return key
}

func recoveryOpenConfig(client *recoveryFakeTransport, svc ClusteredService) Config {
	cfg := openConfig(client)
	cfg.Service = svc
	cfg.StartAck = nil
	cfg.Recovery = &RecoveryConfig{}
	return cfg
}

// loadRecordingService adds LoadSnapshotHandler to recordingService.
// LoadSnapshot stores the image position and one polled fragment.
type loadRecordingService struct {
	recordingService
	loadErr       error
	loadPositions []int64
	loadBytes     [][]byte
}

var _ LoadSnapshotHandler = (*loadRecordingService)(nil)

func (s *loadRecordingService) LoadSnapshot(image *aeron.Image) error {
	s.order = append(s.order, "load")
	s.loadPositions = append(s.loadPositions, image.Position())
	image.Poll(func(buffer atomicbuffer.AtomicBuffer, offset, length int, _ *aeron.Header) {
		s.loadBytes = append(s.loadBytes, append([]byte(nil), buffer.Bytes()[offset:offset+length]...))
	}, 1)
	return s.loadErr
}

func (s *loadRecordingService) Start(cluster Cluster) error {
	s.order = append(s.order, "start")
	return s.recordingService.Start(cluster)
}

func TestAwaitRecoveryState(t *testing.T) {
	t.Parallel()

	errReader := errors.New("counters unavailable")
	state := codecs.RecoveryStateKeyFields{
		LeadershipTermID:     5,
		LogPosition:          640,
		Timestamp:            9,
		SnapshotRecordingIDs: []int64{42, 43},
	}
	tests := []struct {
		name      string
		setup     func(t *testing.T, client *recoveryFakeTransport)
		timeout   time.Duration
		want      codecs.RecoveryStateKeyFields
		wantErr   error
		wantText  string
		wantPumps bool
	}{
		{
			name: "counter at the first read",
			setup: func(t *testing.T, client *recoveryFakeTransport) {
				client.writeRecoveryState(t, state)
			},
			want: state,
		},
		{
			name: "counter after three pumps",
			setup: func(t *testing.T, client *recoveryFakeTransport) {
				client.pumpHook = func(pumps int) {
					if pumps == 3 {
						client.writeRecoveryState(t, state)
					}
				}
			},
			want:      state,
			wantPumps: true,
		},
		{
			name: "reclaimed record before an allocated one",
			setup: func(t *testing.T, client *recoveryFakeTransport) {
				stale := state
				stale.LogPosition = 64
				client.writeRecoveryCounter(
					t, 0, cnc.CounterRecordReclaimed, encodeRecoveryKey(t, stale),
				)
				client.writeRecoveryCounter(
					t, 1, cnc.CounterRecordAllocated, encodeRecoveryKey(t, state),
				)
			},
			want: state,
		},
		{
			name: "counters reader fails",
			setup: func(_ *testing.T, client *recoveryFakeTransport) {
				client.readerErr = errReader
			},
			wantErr:  errReader,
			wantText: "service: read recovery state counter: counters unavailable",
		},
		{
			name:      "counter absent past the timeout",
			setup:     func(*testing.T, *recoveryFakeTransport) {},
			timeout:   10 * time.Millisecond,
			wantErr:   errRecoveryStateMissing,
			wantText:  "service: recovery state counter not found after 10ms",
			wantPumps: true,
		},
		{
			name: "service count past the max",
			setup: func(t *testing.T, client *recoveryFakeTransport) {
				client.writeRecoveryCounter(
					t, 0, cnc.CounterRecordAllocated, recoveryKeyPastMaxServiceCount(t),
				)
			},
			wantErr: errRecoveryStateInvalid,
		},
		{
			name: "cluster id 1",
			setup: func(t *testing.T, client *recoveryFakeTransport) {
				bad := state
				bad.ClusterID = 1
				client.writeRecoveryState(t, bad)
			},
			wantErr:  errRecoveryStateInvalid,
			wantText: "service: recovery state invalid: cluster id 1, want 0",
		},
		{
			name: "negative log position",
			setup: func(t *testing.T, client *recoveryFakeTransport) {
				bad := state
				bad.LogPosition = -1
				client.writeRecoveryState(t, bad)
			},
			wantErr: errRecoveryStateInvalid,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := newRecoveryFakeTransport(t)
			tt.setup(t, client)
			timeout := tt.timeout
			if timeout == 0 {
				timeout = DefaultRecoveryTimeout
			}

			got, err := awaitRecoveryState(client, timeout)
			if tt.wantPumps {
				require.Positive(t, client.pumps, "pumps")
			} else {
				require.Zero(t, client.pumps, "pumps")
			}
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				if tt.wantText != "" {
					require.EqualError(t, err, tt.wantText)
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestResolveRecoveryPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		state     codecs.RecoveryStateKeyFields
		serviceID int32
		want      int64
		wantText  string
	}{
		{
			name:  "term -1 takes the term-only path",
			state: codecs.RecoveryStateKeyFields{LeadershipTermID: nullValue, LogPosition: 64},
			want:  nullValue,
		},
		{
			name: "valid id takes the load path",
			state: codecs.RecoveryStateKeyFields{
				LeadershipTermID:     2,
				SnapshotRecordingIDs: []int64{40, 41},
			},
			serviceID: 1,
			want:      41,
		},
		{
			name: "service id outside the count",
			state: codecs.RecoveryStateKeyFields{
				LeadershipTermID:     2,
				SnapshotRecordingIDs: []int64{40, 41},
			},
			serviceID: 3,
			wantText:  "service: recovery state invalid: service id 3 outside service count 2",
		},
		{
			name: "negative recording id takes the load path",
			state: codecs.RecoveryStateKeyFields{
				LeadershipTermID:     2,
				SnapshotRecordingIDs: []int64{-1},
			},
			want: -1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := resolveRecoveryPath(tt.state, tt.serviceID)
			if tt.wantText != "" {
				require.ErrorIs(t, err, errRecoveryStateInvalid)
				require.EqualError(t, err, tt.wantText)
				return
			}
			require.NoError(t, err)
			require.Equal(t, recoveryPath{state: tt.state, recordingID: tt.want}, got)
		})
	}
}

func TestAgent_seedRecovery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		state    codecs.RecoveryStateKeyFields
		wantTerm int64
	}{
		{
			name: "snapshot term",
			state: codecs.RecoveryStateKeyFields{
				LeadershipTermID: 5,
				LogPosition:      640,
				Timestamp:        9,
			},
			wantTerm: 5,
		},
		{
			name: "term -1",
			state: codecs.RecoveryStateKeyFields{
				LeadershipTermID: nullValue,
				LogPosition:      640,
				Timestamp:        9,
			},
			wantTerm: nullValue,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			agent := NewAgent(AgentConfig{})
			agent.seedRecovery(tt.state)
			require.Equal(t, int64(640), agent.LogPosition())
			require.Equal(t, int64(9), agent.ClusterTime())
			require.Equal(t, tt.wantTerm, agent.egressLeadershipTermID)
			require.Equal(t, int32(-1), agent.MemberID())
			require.Equal(t, codecs.RoleFollower, agent.Role())
		})
	}
}

// recoveryLoadFixture drives one snapshot load through openWithArchiveConnect.
// Its pump hook attaches the replay image once the replay subscription exists.
type recoveryLoadFixture struct {
	client *recoveryFakeTransport
	replay *fakeSnapshotReplay
	image  *aeron.Image
}

const (
	recoveryLoadRecordingID int64 = 42
	recoveryLoadLogPosition int64 = 640
	recoveryLoadSessionID   int32 = 77
	recoveryLoadReplayID    int64 = 3<<32 | int64(recoveryLoadSessionID)
)

func newRecoveryLoadFixture(t *testing.T, frames [][]byte) *recoveryLoadFixture {
	t.Helper()
	f := &recoveryLoadFixture{
		client: newRecoveryFakeTransport(t),
		replay: &fakeSnapshotReplay{sessionID: recoveryLoadReplayID},
	}
	f.client.writeRecoveryState(t, codecs.RecoveryStateKeyFields{
		LeadershipTermID:     5,
		LogPosition:          recoveryLoadLogPosition,
		Timestamp:            9,
		SnapshotRecordingIDs: []int64{recoveryLoadRecordingID},
	})
	f.client.pumpHook = func(int) {
		if f.image != nil || len(f.client.replay.subs) == 0 {
			return
		}
		sub := f.client.replay.subs[0]
		f.image = aeron.NewTestImageForSubscriptionHooksWithSessionID(
			t, sub, recoveryLoadSessionID, 9_401, 0,
		)
		writeSnapshotFrames(t, f.image, frames)
		require.True(t, aeron.SubscriptionAddImageForHooks(sub, f.image))
	}
	return f
}

func (f *recoveryLoadFixture) open(svc ClusteredService) (*Container, error) {
	cfg := recoveryOpenConfig(f.client, svc)
	cfg.Archive = &archive.Config{}
	return openWithArchiveConnect(cfg, func(
		TransportClient,
		archive.Config,
	) (*archive.Client, snapshotReplayControl, error) {
		return nil, f.replay, nil
	})
}

// requireReplayStopped asserts one replay start, one stop, and one closed
// replay subscription on the session-scoped channel.
func (f *recoveryLoadFixture) requireReplayStopped(t *testing.T) {
	t.Helper()
	require.Equal(t, []fakeStartReplayCall{{
		recordingID:    recoveryLoadRecordingID,
		position:       0,
		length:         nullValue,
		replayStreamID: DefaultReplayStreamID,
		replayChannel:  DefaultReplayChannel,
	}}, f.replay.startCalls)
	require.Equal(t, []int64{recoveryLoadReplayID}, f.replay.stopCalls)
	require.Equal(t, []transportAdd{{
		streamID: DefaultReplayStreamID,
		channel:  "aeron:ipc?session-id=77",
	}}, f.client.replay.calls)
	require.Len(t, f.client.replay.subs, 1)
	require.True(t, f.client.replay.subs[0].IsClosed(), "replay subscription closed")
}

func TestOpen_recovery(t *testing.T) {
	t.Parallel()

	t.Run("refusals", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name      string
			setup     func(t *testing.T, client *recoveryFakeTransport)
			serviceID int32
			archive   *archive.Config
			wantErr   error
		}{
			{
				name:    "counter absent",
				setup:   func(*testing.T, *recoveryFakeTransport) {},
				wantErr: errRecoveryStateMissing,
			},
			{
				name: "key decode",
				setup: func(t *testing.T, client *recoveryFakeTransport) {
					client.writeRecoveryCounter(
						t, 0, cnc.CounterRecordAllocated, recoveryKeyPastMaxServiceCount(t),
					)
				},
				wantErr: errRecoveryStateInvalid,
			},
			{
				name: "cluster id",
				setup: func(t *testing.T, client *recoveryFakeTransport) {
					client.writeRecoveryState(t, codecs.RecoveryStateKeyFields{
						LeadershipTermID: nullValue,
						ClusterID:        1,
					})
				},
				wantErr: errRecoveryStateInvalid,
			},
			{
				name: "negative log position",
				setup: func(t *testing.T, client *recoveryFakeTransport) {
					client.writeRecoveryState(t, codecs.RecoveryStateKeyFields{
						LeadershipTermID: nullValue,
						LogPosition:      -64,
					})
				},
				wantErr: errRecoveryStateInvalid,
			},
			{
				name: "service id outside the count",
				setup: func(t *testing.T, client *recoveryFakeTransport) {
					client.writeRecoveryState(t, codecs.RecoveryStateKeyFields{
						LeadershipTermID:     5,
						SnapshotRecordingIDs: []int64{42},
					})
				},
				serviceID: 3,
				archive:   &archive.Config{},
				wantErr:   errRecoveryStateInvalid,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				client := newRecoveryFakeTransport(t)
				tt.setup(t, client)
				cfg := recoveryOpenConfig(client, &recordingService{})
				cfg.ServiceID = tt.serviceID
				cfg.Archive = tt.archive
				cfg.Recovery.Timeout = 10 * time.Millisecond

				c, err := Open(cfg)
				require.ErrorIs(t, err, tt.wantErr)
				require.Nil(t, c)
				require.Empty(t, client.subCalls)
				require.Empty(t, client.pubCalls)
				require.Empty(t, client.replay.calls)
			})
		}
	})

	t.Run("failed start ack", func(t *testing.T) {
		t.Parallel()
		errConnect := errors.New("archive connect boom")
		errSubscription := errors.New("subscription add boom")
		loadState := codecs.RecoveryStateKeyFields{
			LeadershipTermID:     5,
			LogPosition:          640,
			Timestamp:            9,
			SnapshotRecordingIDs: []int64{42},
		}
		termOnlyState := codecs.RecoveryStateKeyFields{
			LeadershipTermID: nullValue,
			LogPosition:      640,
			Timestamp:        9,
		}
		// notConnected rows offer on a stream that never connects. The offer
		// retries until Recovery.Timeout. Open pumps between offers. The
		// row does not send an ack.
		tests := []struct {
			name         string
			state        codecs.RecoveryStateKeyFields
			archive      *archive.Config
			subErr       error
			connectErr   error
			notConnected bool
			wantErr      error
			wantSubCalls int
		}{
			{
				name: "negative recording id",
				state: codecs.RecoveryStateKeyFields{
					LeadershipTermID:     5,
					LogPosition:          640,
					Timestamp:            9,
					SnapshotRecordingIDs: []int64{-5},
				},
				archive: &archive.Config{},
				wantErr: errRecoveryStateInvalid,
			},
			{
				name:    "load path without Archive",
				state:   loadState,
				wantErr: errSnapshotLoad,
			},
			{
				name:         "control subscription add",
				state:        termOnlyState,
				subErr:       errSubscription,
				wantErr:      errSubscription,
				wantSubCalls: 1,
			},
			{
				name:         "control subscription add on a stream that never connects",
				state:        termOnlyState,
				subErr:       errSubscription,
				notConnected: true,
				wantErr:      errSubscription,
				wantSubCalls: 1,
			},
			{
				name:         "archive connect",
				state:        loadState,
				archive:      &archive.Config{},
				connectErr:   errConnect,
				wantErr:      errConnect,
				wantSubCalls: 1,
			},
			{
				name:         "archive connect on a stream that never connects",
				state:        loadState,
				archive:      &archive.Config{},
				connectErr:   errConnect,
				notConnected: true,
				wantErr:      errConnect,
				wantSubCalls: 1,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				client := newRecoveryFakeTransport(t)
				client.writeRecoveryState(t, tt.state)
				client.subErr = tt.subErr
				if tt.notConnected {
					client.pub = aeron.NewTestPublicationForHooksWithLimit(t, 0, false)
				}
				cfg := recoveryOpenConfig(client, &recordingService{})
				cfg.Archive = tt.archive
				cfg.Recovery.Timeout = 10 * time.Millisecond

				c, err := openWithArchiveConnect(cfg, func(
					TransportClient,
					archive.Config,
				) (*archive.Client, snapshotReplayControl, error) {
					return nil, nil, tt.connectErr
				})
				require.ErrorIs(t, err, tt.wantErr)
				require.Nil(t, c)
				require.Len(t, client.subCalls, tt.wantSubCalls)
				if tt.wantSubCalls > 0 && tt.subErr == nil {
					require.True(t, client.sub.IsClosed(), "control subscription closed")
				}
				require.Equal(t, []transportAdd{{
					streamID: DefaultConsensusModuleStreamID,
					channel:  DefaultControlChannel,
				}}, client.pubCalls)
				require.True(t, client.pub.IsClosed(), "control publication closed")
				require.Empty(t, client.replay.calls)
				if tt.notConnected {
					require.Positive(t, client.pumps, "pumps between offers")
					require.Empty(t, decodeServiceAcksFromPub(t, client.pub))
					return
				}
				require.Zero(t, client.pumps, "pumps before the first offer")
				require.Equal(t, []codecs.ServiceAckFields{{
					LogPosition: 640,
					Timestamp:   9,
					AckID:       0,
					RelevantID:  nullValue,
					ServiceID:   0,
				}}, decodeServiceAcksFromPub(t, client.pub))
			})
		}
	})

	t.Run("control publication add", func(t *testing.T) {
		t.Parallel()
		// The failed start ack adds the control publication again. A second
		// add failure does not send the ack.
		tests := []struct {
			name       string
			pubErrAdds int
			wantAck    bool
		}{
			{name: "transient error sends the ack", pubErrAdds: 1, wantAck: true},
			{name: "persistent error sends nothing"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				errPublication := errors.New("publication add boom")
				client := newRecoveryFakeTransport(t)
				client.writeRecoveryState(t, codecs.RecoveryStateKeyFields{
					LeadershipTermID: nullValue,
					LogPosition:      640,
					Timestamp:        9,
				})
				client.pubErr = errPublication
				client.pubErrAdds = tt.pubErrAdds

				c, err := Open(recoveryOpenConfig(client, &recordingService{}))
				require.ErrorIs(t, err, errPublication)
				require.Nil(t, c)
				require.Len(t, client.pubCalls, 2)
				require.True(t, client.sub.IsClosed(), "control subscription closed")
				if !tt.wantAck {
					require.Nil(t, client.pub)
					return
				}
				require.True(t, client.pub.IsClosed(), "control publication closed")
				require.Equal(t, []codecs.ServiceAckFields{{
					LogPosition: 640,
					Timestamp:   9,
					AckID:       0,
					RelevantID:  nullValue,
					ServiceID:   0,
				}}, decodeServiceAcksFromPub(t, client.pub))
			})
		}
	})

	t.Run("term-only", func(t *testing.T) {
		t.Parallel()
		client := newRecoveryFakeTransport(t)
		client.writeRecoveryState(t, codecs.RecoveryStateKeyFields{
			LeadershipTermID: nullValue,
			LogPosition:      640,
			Timestamp:        9,
		})
		svc := &recordingService{}
		cfg := recoveryOpenConfig(client, svc)
		cfg.ServiceID = 2
		c := openContainer(t, cfg)
		agent := c.Agent()
		require.Equal(t, RecoveryConfig{}, *cfg.Recovery, "caller config stays unchanged")

		require.Equal(t, int64(640), agent.LogPosition())
		require.Equal(t, int64(9), agent.ClusterTime())
		require.Equal(t, nullValue, agent.egressLeadershipTermID)
		require.Equal(t, []coordinationAck{{
			logPosition: 640,
			kind:        ackKindStart,
			relevantID:  recoveryFakeClientID,
		}}, agent.coordinationAcks)
		require.Empty(t, client.replay.calls)

		agent.DoWork(0)
		require.NoError(t, agent.ServiceFailure())
		require.Equal(t, 1, svc.startCount)
		require.Equal(t, []codecs.ServiceAckFields{{
			LogPosition: 640,
			Timestamp:   9,
			AckID:       0,
			RelevantID:  recoveryFakeClientID,
			ServiceID:   2,
		}}, decodeServiceAcksFromPub(t, client.pub))
	})

	t.Run("term-only Start error", func(t *testing.T) {
		t.Parallel()
		startErr := errors.New("start boom")
		client := newRecoveryFakeTransport(t)
		client.writeRecoveryState(t, codecs.RecoveryStateKeyFields{
			LeadershipTermID: nullValue,
			LogPosition:      640,
			Timestamp:        9,
		})
		cfg := recoveryOpenConfig(client, &recordingService{startErr: startErr})
		cfg.Recovery.Timeout = 7 * time.Second
		agent := openContainer(t, cfg).Agent()
		require.Equal(t, cfg.Recovery.Timeout.Nanoseconds(), agent.startAckTimeoutNs)

		require.Equal(t, 2, agent.DoWork(0), "Start + start drain")
		require.ErrorIs(t, agent.ServiceFailure(), startErr)
		require.Equal(t, []codecs.ServiceAckFields{{
			LogPosition: 640,
			Timestamp:   9,
			AckID:       0,
			RelevantID:  nullValue,
			ServiceID:   0,
		}}, decodeServiceAcksFromPub(t, client.pub))
	})

	t.Run("load failure", func(t *testing.T) {
		t.Parallel()
		f := newRecoveryLoadFixture(t, [][]byte{
			encodeNLT(t, codecs.NewLeadershipTermEventFields{LeadershipTermID: 5}),
		})
		svc := &loadRecordingService{}

		c, err := f.open(svc)
		require.ErrorIs(t, err, errSnapshotLoad)
		require.ErrorContains(t, err, "template 24 before the begin marker")
		require.Nil(t, c)
		require.True(t, f.client.sub.IsClosed(), "control subscription closed")
		require.True(t, f.client.pub.IsClosed(), "control publication closed")
		require.Equal(t, []codecs.ServiceAckFields{{
			LogPosition: recoveryLoadLogPosition,
			Timestamp:   9,
			AckID:       0,
			RelevantID:  nullValue,
			ServiceID:   0,
		}}, decodeServiceAcksFromPub(t, f.client.pub))
		f.requireReplayStopped(t)
		require.Empty(t, svc.loadPositions, "LoadSnapshot calls")
		require.Equal(t, 0, svc.startCount)
	})

	t.Run("load", func(t *testing.T) {
		t.Parallel()
		begin := serviceMarkerFields(recoveryLoadLogPosition, clusterwire.SnapshotMark_BEGIN)
		begin.TimeUnit = clusterwire.ClusterTimeUnit_MICROS
		begin.AppVersion = 4
		end := begin
		end.Mark = clusterwire.SnapshotMark_END
		frames := [][]byte{
			encodeSnapshotMarkerFrame(t, begin),
			encodeClientSessionFrame(t, codecs.ClientSessionFields{
				ClusterSessionID: 8,
				ResponseStreamID: 18,
				ResponseChannel:  []byte("aeron:udp?endpoint=c:8"),
				EncodedPrincipal: []byte("p8"),
			}),
			encodeSnapshotMarkerFrame(t, end),
			[]byte("user-state"),
		}
		f := newRecoveryLoadFixture(t, frames)
		svc := &loadRecordingService{}

		c, err := f.open(svc)
		require.NoError(t, err)
		require.NotNil(t, c)
		t.Cleanup(func() { _ = c.Close() })
		f.requireReplayStopped(t)
		require.False(t, f.client.sub.IsClosed(), "control subscription open")

		agent := c.Agent()
		require.Equal(t, recoveryLoadLogPosition, agent.LogPosition())
		require.Equal(t, int64(9), agent.ClusterTime())
		require.Equal(t, int64(5), agent.egressLeadershipTermID)
		require.Equal(t, clusterwire.ClusterTimeUnit_MICROS, agent.timeUnit)
		require.Equal(t, int32(4), agent.appVersion)
		session, ok := agent.ClientSession(8)
		require.True(t, ok)
		require.Equal(t, int32(18), session.ResponseStreamID())
		require.Equal(t, "aeron:udp?endpoint=c:8", session.ResponseChannel())
		require.Equal(t, []byte("p8"), session.EncodedPrincipal())
		require.Empty(t, svc.opens, "SessionOpen calls")
		require.Empty(t, agent.responseAdds, "response adds")
		restored := agent.sessions[8]
		require.Equal(t, nullValue, restored.addCorrelationID, "add correlation id")
		require.Nil(t, restored.publication, "response publication")
		require.Equal(t, [][]byte{[]byte("user-state")}, svc.loadBytes)

		agent.DoWork(0)
		require.NoError(t, agent.ServiceFailure())
		require.Equal(t, []string{"load", "start"}, svc.order)
		require.Equal(t, []codecs.ServiceAckFields{{
			LogPosition: recoveryLoadLogPosition,
			Timestamp:   9,
			AckID:       0,
			RelevantID:  recoveryFakeClientID,
			ServiceID:   0,
		}}, decodeServiceAcksFromPub(t, f.client.pub))
	})
}
