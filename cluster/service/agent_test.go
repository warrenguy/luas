//go:build testhooks

package service

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/archive"
	"github.com/warrenguy/luas/cluster/codecs"
	"github.com/warrenguy/luas/cluster/codecs/clusterwire"
	"github.com/warrenguy/luas/internal/testutil"
)

func TestAgent_Start_once(t *testing.T) {
	svc := &recordingService{}
	agent := NewAgent(AgentConfig{Service: svc})

	require.Equal(t, 1, agent.DoWork(0))
	require.Equal(t, 1, svc.startCount)
	require.NoError(t, agent.ServiceFailure())

	// Later ticks do not re-call Start.
	require.Equal(t, 0, agent.DoWork(0))
	require.Equal(t, 1, svc.startCount)
}

func TestAgent_Start_errorMarksSticky(t *testing.T) {
	startErr := errors.New("start boom")
	svc := &recordingService{startErr: startErr}
	sub, _ := newLogImage(t, testLogSessionID, testLogStreamID, 0)
	agent := NewAgent(AgentConfig{
		Service:       svc,
		Subscriptions: &stubAdder{sub: sub},
	})
	// Join is ready to bind. Start failure must return before joinWork.
	agent.handleJoinLog(defaultJoinEvent(0))

	require.Equal(t, 1, agent.DoWork(0))
	require.ErrorIs(t, agent.ServiceFailure(), startErr)
	require.Equal(t, 1, svc.startCount)
	require.True(t, agent.HasPendingJoin())
	require.False(t, agent.IsLogBound())

	// serviceFailure early return. Without that check, joinWork would bind.
	require.Equal(t, 0, agent.DoWork(0))
	require.False(t, agent.IsLogBound())
	require.True(t, agent.HasPendingJoin())
	require.Equal(t, 0, agent.ReadyCoordinationAcks())
	require.Equal(t, 1, svc.startCount)
}

func TestAgent_CallbackOrderGolden(t *testing.T) {
	svc := &recordingService{}
	agent, image, committed := bindAgent(t, svc, 0)

	frames := [][]byte{
		encodeSessionOpen(t, codecs.SessionOpenEventFields{
			LeadershipTermID: 1,
			ClusterSessionID: 1,
			Timestamp:        1,
			ResponseChannel:  []byte("c"),
		}),
		encodeSessionMessage(
			t,
			codecs.SessionMessageHeaderFields{
				LeadershipTermID: 1,
				ClusterSessionID: 1,
				Timestamp:        2,
			},
			[]byte("m1"),
		),
		encodeSessionMessage(
			t,
			codecs.SessionMessageHeaderFields{
				LeadershipTermID: 1,
				ClusterSessionID: 1,
				Timestamp:        3,
			},
			[]byte("m2"),
		),
		encodeTimerEvent(t, codecs.TimerEventFields{
			LeadershipTermID: 1,
			CorrelationID:    99,
			Timestamp:        4,
		}),
		encodeSessionClose(t, codecs.SessionCloseEventFields{
			LeadershipTermID: 1,
			ClusterSessionID: 1,
			Timestamp:        5,
			CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
		}),
	}

	offset := 0
	for _, payload := range frames {
		offset += writeAgentFrame(t, image, offset, payload)
	}
	end := int64(offset)
	*committed = end

	// Drain until all frames delivered (fragment limit may chunk).
	for agent.LogPosition() < end && agent.ServiceFailure() == nil {
		agent.DoWork(0)
	}
	require.NoError(t, agent.ServiceFailure())
	require.Equal(t, []string{"open", "message", "message", "timer", "close"}, svc.order)
	require.Equal(t, end, agent.LogPosition())
	require.Equal(t, int64(5), agent.ClusterTime())
	require.Len(t, svc.messages, 2)
}

func TestAgent_PositionAndTime_boundLimited(t *testing.T) {
	svc := &recordingService{}
	agent, image, committed := bindAgent(t, svc, 0)

	p1 := encodeTimerEvent(t, codecs.TimerEventFields{
		LeadershipTermID: 1,
		CorrelationID:    1,
		Timestamp:        100,
	})
	p2 := encodeTimerEvent(t, codecs.TimerEventFields{
		LeadershipTermID: 1,
		CorrelationID:    2,
		Timestamp:        200,
	})
	span1 := writeAgentFrame(t, image, 0, p1)
	span2 := writeAgentFrame(t, image, span1, p2)

	// Bound between frames.
	*committed = int64(span1)
	require.Equal(t, 1, agent.DoWork(0))
	require.Equal(t, int64(span1), agent.LogPosition())
	require.Equal(t, int64(100), agent.ClusterTime())
	require.Len(t, svc.timers, 1)

	*committed = int64(span1 + span2)
	require.Equal(t, 1, agent.DoWork(0))
	require.Equal(t, int64(span1+span2), agent.LogPosition())
	require.Equal(t, int64(200), agent.ClusterTime())
	require.Len(t, svc.timers, 2)
}

func TestAgent_NLT_optionalHandler(t *testing.T) {
	t.Run("implements", func(t *testing.T) {
		svc := &nltRecordingService{}
		agent, image, committed := bindAgent(t, svc, 0)
		fields := codecs.NewLeadershipTermEventFields{
			LeadershipTermID:    7,
			LogPosition:         128,
			Timestamp:           999,
			TermBaseLogPosition: 64,
			LeaderMemberID:      1,
			LogSessionID:        3,
			TimeUnit:            clusterwire.ClusterTimeUnit_NANOS,
			AppVersion:          42,
		}
		span := writeAgentFrame(t, image, 0, encodeNLT(t, fields))
		*committed = int64(span)
		require.Equal(t, 1, agent.DoWork(0))
		require.Len(t, svc.nlt, 1)
		require.Equal(
			t,
			nltRec{
				leadershipTermID:    fields.LeadershipTermID,
				logPosition:         fields.LogPosition,
				timestamp:           fields.Timestamp,
				termBaseLogPosition: fields.TermBaseLogPosition,
				leaderMemberID:      fields.LeaderMemberID,
				logSessionID:        fields.LogSessionID,
				timeUnit:            fields.TimeUnit,
				appVersion:          fields.AppVersion,
			},
			svc.nlt[0],
		)
		require.Equal(t, int64(span), agent.LogPosition())
		require.Equal(t, int64(999), agent.ClusterTime())
		require.Equal(t, fields.LeadershipTermID, agent.egressLeadershipTermID)
	})

	t.Run("doesNotImplement", func(t *testing.T) {
		svc := &recordingService{}
		agent, image, committed := bindAgent(t, svc, 0)
		fields := codecs.NewLeadershipTermEventFields{
			LeadershipTermID: 1,
			LogPosition:      1,
			Timestamp:        50,
		}
		span := writeAgentFrame(t, image, 0, encodeNLT(t, fields))
		*committed = int64(span)
		require.Equal(t, 1, agent.DoWork(0))
		require.Equal(t, int64(span), agent.LogPosition())
		require.Equal(t, int64(50), agent.ClusterTime())
		require.Empty(t, svc.order)
		require.Equal(t, fields.LeadershipTermID, agent.egressLeadershipTermID)
	})
}

func TestAgent_NilService(t *testing.T) {
	agent, image, committed := bindAgent(t, nil, 0)

	openPayload := encodeSessionOpen(t, codecs.SessionOpenEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: 3,
		Timestamp:        1,
		ResponseChannel:  []byte("c"),
	})
	span := writeAgentFrame(t, image, 0, openPayload)
	*committed = int64(span)

	require.NotPanics(t, func() {
		require.Equal(t, 1, agent.DoWork(0))
	})
	require.True(t, agent.HasSession(3))
	require.Equal(t, int64(span), agent.LogPosition())
}

func TestAgent_ScheduleTimer(t *testing.T) {
	fields := sampleScheduleTimerFields()
	proxy, pub := boundModuleProxy(t)
	agent := NewAgent(AgentConfig{ModuleProxy: proxy})
	require.True(t, agent.ScheduleTimer(fields.CorrelationID, fields.Deadline))
	require.NoError(t, agent.ServiceFailure())
	requireDecodedFrame(
		t,
		pub,
		codecs.ScheduleTimerEncodedLen(),
		codecs.TryDecodeScheduleTimerAt,
		fields,
	)
}

func TestAgent_CancelTimer(t *testing.T) {
	fields := sampleCancelTimerFields()
	proxy, pub := boundModuleProxy(t)
	agent := NewAgent(AgentConfig{ModuleProxy: proxy})
	require.True(t, agent.CancelTimer(fields.CorrelationID))
	require.NoError(t, agent.ServiceFailure())
	requireDecodedFrame(t, pub, codecs.CancelTimerEncodedLen(), codecs.TryDecodeCancelTimerAt, fields)
}

func TestAgent_TimerPublish_failurePaths(t *testing.T) {
	schedule := sampleScheduleTimerFields()
	cancel := sampleCancelTimerFields()
	ops := []struct {
		name    string
		publish func(*Agent) bool
	}{
		{
			name: "schedule",
			publish: func(a *Agent) bool {
				return a.ScheduleTimer(schedule.CorrelationID, schedule.Deadline)
			},
		},
		{
			name: "cancel",
			publish: func(a *Agent) bool {
				return a.CancelTimer(cancel.CorrelationID)
			},
		},
	}
	for _, tc := range moduleProxyFailureCases(ErrTimerPublish) {
		t.Run(tc.name, func(t *testing.T) {
			for _, op := range ops {
				t.Run(op.name, func(t *testing.T) {
					var got []error
					agent := NewAgent(AgentConfig{
						ModuleProxy:  tc.setup(t),
						ErrorHandler: collectErrors(&got),
					})
					require.False(t, op.publish(agent))
					require.NoError(t, agent.ServiceFailure())

					if tc.reported == nil {
						require.Empty(t, got)
						return
					}

					requireOneNonFatalReport(t, got, tc.reported)
					require.False(t, op.publish(agent))
					require.Len(t, got, 2)
					require.NoError(t, agent.ServiceFailure())
				})
			}
		})
	}
}

// TestAgent_Close_rejectsModuleRequests asserts a closed Agent rejects
// each module request. A rejected request does not close the session,
// publish, or mark a failure.
func TestAgent_Close_rejectsModuleRequests(t *testing.T) {
	const openID int64 = 7
	log, _, pub := bindCloseAgent(t, nil, codecs.RoleLeader)
	agent := log.agent
	log.open(openID, 1)
	agent.Close()
	require.True(t, agent.IsClosed())
	sent := pub.Position()

	require.False(t, agent.ScheduleTimer(1, 1))
	require.False(t, agent.CancelTimer(1))
	require.False(t, agent.CloseClientSession(openID))
	session, ok := agent.ClientSession(openID)
	require.True(t, ok)
	require.False(t, session.Close())
	require.False(t, session.IsClosing())
	require.Equal(t, sent, pub.Position())
	require.NoError(t, agent.ServiceFailure())
}

func TestAgent_NilCommittedPosition_skipsPoll(t *testing.T) {
	svc := &recordingService{}
	sub, image := newLogImage(t, testLogSessionID, testLogStreamID, 0)

	agent := NewAgent(AgentConfig{
		Service:       svc,
		Subscriptions: &stubAdder{sub: sub},
		// CommittedPosition nil
	})
	agent.handleJoinLog(defaultJoinEvent(0))
	agent.DoWork(0)
	require.True(t, agent.IsLogBound())

	payload := encodeTimerEvent(t, codecs.TimerEventFields{
		LeadershipTermID: 1,
		CorrelationID:    1,
		Timestamp:        1,
	})
	writeAgentFrame(t, image, 0, payload)

	require.Equal(t, 0, agent.DoWork(0))
	require.Empty(t, svc.timers)
	require.Equal(t, int64(0), agent.LogPosition())
}

func TestAgent_Close_idempotent(t *testing.T) {
	adder := newFakeResponseAdder()
	log, svc := bindSessionAgent(t, adder, codecs.RoleLeader)
	agent := log.agent

	// Session 1 holds a publication. Session 2 has an add in flight.
	log.open(1, 1)
	pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
	adder.next(1, responsePoll{publication: pub, done: true})
	log.open(2, 1)
	require.Equal(t, 1, agent.PendingResponsePublicationAdds())

	payload := encodeTimerEvent(t, codecs.TimerEventFields{
		LeadershipTermID: 1,
		CorrelationID:    1,
		Timestamp:        1,
	})
	log.write(t, payload)

	require.False(t, agent.IsClosed())
	agent.Close()
	require.True(t, agent.IsClosed())
	require.False(t, agent.IsLogBound())
	require.True(t, agent.logSubscription == nil || agent.logSubscription.IsClosed())
	require.True(t, pub.IsClosed())
	require.Equal(t, []int64{2}, adder.cancels)
	require.Empty(t, adder.inFlight)
	require.Equal(t, 0, agent.PendingResponsePublicationAdds())
	require.Equal(t, 2, agent.SessionCount())
	for session := range agent.ClientSessions() {
		require.Equal(t, aeron.NotConnected, session.Offer([]byte("late")))
	}

	// Idempotent.
	agent.Close()
	require.True(t, agent.IsClosed())
	require.Equal(t, []int64{2}, adder.cancels)

	// Inert after close.
	require.Equal(t, 0, agent.DoWork(0))
	require.Empty(t, svc.timers)
}

func TestAgent_NewAgent_sentinels(t *testing.T) {
	agent := NewAgent(AgentConfig{})
	require.Equal(t, int32(0), agent.ServiceID())
	require.Equal(t, int32(-1), agent.MemberID())
	require.Equal(t, int64(0), agent.LogPosition())
	require.Equal(t, codecs.RoleFollower, agent.Role())
	require.False(t, agent.IsLogBound())
	require.False(t, agent.HasPendingJoin())
	require.NoError(t, agent.ServiceFailure())
	require.Equal(t, nullValue, agent.egressLeadershipTermID)
	require.Equal(t, 0, agent.ResponsePublicationCount())

	other := NewAgent(AgentConfig{ServiceID: 3})
	require.Equal(t, int32(3), other.ServiceID())
}

func TestAgent_DoWork_pollsArchive(t *testing.T) {
	svc := &recordingService{}
	agent := NewAgent(AgentConfig{Service: svc})
	archiveControl := &countingArchiveControl{pollWork: 2}
	agent.archive = archiveControl

	// Tick 1: Start plus the archive Poll count.
	require.Equal(t, 3, agent.DoWork(0))
	require.Equal(t, 1, archiveControl.polls)

	// Tick 2: the archive Poll count alone.
	require.Equal(t, 2, agent.DoWork(0))
	require.Equal(t, 2, archiveControl.polls)

	// After Close: no Poll.
	agent.Close()
	require.Equal(t, 0, agent.DoWork(0))
	require.Equal(t, 2, archiveControl.polls)
}

func TestAgent_NewAgent_noArchive(t *testing.T) {
	agent := NewAgent(AgentConfig{})

	// A typed-nil client in the interface field would panic in Poll.
	require.Equal(t, snapshotArchiveControl(nil), agent.archive)
	require.Equal(t, 1, agent.DoWork(0))
	require.NoError(t, agent.ServiceFailure())
}

func TestAgent_DispatchFailure_sticky(t *testing.T) {
	svc := &recordingService{}
	agent, image, committed := bindAgent(t, svc, 0)

	// Wrong schema aborts and marks failure.
	payload := encodeMessageHeader(t, 0, 1, 112, 0)
	span := writeAgentFrame(t, image, 0, payload)
	*committed = int64(span)

	require.Equal(t, 0, agent.DoWork(0))
	require.ErrorIs(t, agent.ServiceFailure(), ErrLogDispatchSchema)
	require.Equal(t, 0, agent.DoWork(0))
}

// lifecycleRecordingService adds RoleChangeHandler, BackgroundWorkHandler, and
// TerminateHandler to snapshotRecordingService. Each call except Terminate
// appends its name to order. probes maps a call name ("start", "role",
// "background", "terminate", "message") to a func that runs inside that call.
type lifecycleRecordingService struct {
	snapshotRecordingService

	agent *Agent

	roles          []roleChangeRecord
	backgroundNows []int64

	backgroundResult int

	probes map[string]func(*Agent)
}

// roleChangeRecord is one RoleChange call and the agent state inside it.
type roleChangeRecord struct {
	role         codecs.Role
	seenRole     codecs.Role
	logBound     bool
	pendingJoin  bool
	pendingAdds  int
	publications int
}

func (s *lifecycleRecordingService) Start(cluster Cluster) error {
	s.agent, _ = cluster.(*Agent)
	s.order = append(s.order, "start")
	s.runProbe("start")
	return s.snapshotRecordingService.Start(cluster)
}

func (s *lifecycleRecordingService) SessionMessage(
	session ClientSession,
	timestamp int64,
	payload []byte,
) {
	s.snapshotRecordingService.SessionMessage(session, timestamp, payload)
	s.runProbe("message")
}

func (s *lifecycleRecordingService) RoleChange(role codecs.Role) {
	agent := s.agent
	s.roles = append(s.roles, roleChangeRecord{
		role:         role,
		seenRole:     agent.Role(),
		logBound:     agent.IsLogBound(),
		pendingJoin:  agent.HasPendingJoin(),
		pendingAdds:  agent.PendingResponsePublicationAdds(),
		publications: agent.ResponsePublicationCount(),
	})
	s.order = append(s.order, "role")
	s.runProbe("role")
}

func (s *lifecycleRecordingService) DoBackgroundWork(nowNs int64) int {
	s.backgroundNows = append(s.backgroundNows, nowNs)
	s.order = append(s.order, "background")
	s.runProbe("background")
	return s.backgroundResult
}

func (s *lifecycleRecordingService) Terminate(Cluster) error {
	s.runProbe("terminate")
	return nil
}

func (s *lifecycleRecordingService) runProbe(call string) {
	if probe := s.probes[call]; probe != nil {
		probe(s.agent)
	}
}

func (s *lifecycleRecordingService) events() []string {
	return slices.DeleteFunc(slices.Clone(s.order), func(event string) bool {
		return event == "background"
	})
}

// lifecycleZeroAllocService adds a no-op RoleChangeHandler and a
// BackgroundWorkHandler to recordingService. Unlike lifecycleRecordingService,
// its handlers do not append to a slice.
type lifecycleZeroAllocService struct {
	recordingService

	cluster Cluster
	refuse  bool
}

func (s *lifecycleZeroAllocService) Start(cluster Cluster) error {
	s.cluster = cluster
	return s.recordingService.Start(cluster)
}

func (*lifecycleZeroAllocService) RoleChange(codecs.Role) {}

func (s *lifecycleZeroAllocService) DoBackgroundWork(int64) int {
	if s.refuse {
		s.cluster.ScheduleTimer(1, 2)
	}
	return 0
}

// requestProbe calls ScheduleTimer, CancelTimer, CloseClientSession, and
// ClientSession.Offer once per run, in that order.
type requestProbe struct {
	sessionID int64
	runs      []requestProbeResult
}

// requestProbeResult holds one requestProbe run. offer stays 0 when sessionID
// is not open.
type requestProbeResult struct {
	scheduled bool
	cancelled bool
	closed    bool
	offer     int64
}

func (p *requestProbe) run(agent *Agent) {
	result := requestProbeResult{
		scheduled: agent.ScheduleTimer(1, 2),
		cancelled: agent.CancelTimer(1),
		closed:    agent.CloseClientSession(p.sessionID),
	}
	if session, ok := agent.ClientSession(p.sessionID); ok {
		result.offer = session.Offer([]byte("probe"))
	}
	p.runs = append(p.runs, result)
}

func refusalTexts(call string) []string {
	return []string{
		"service: refused inside lifecycle call: ScheduleTimer in " + call,
		"service: refused inside lifecycle call: CancelTimer in " + call,
		"service: refused inside lifecycle call: CloseClientSession in " + call,
		"service: refused inside lifecycle call: ClientSession.Offer in " + call,
	}
}

func requireRefusals(tb testing.TB, got []error, texts []string) {
	tb.Helper()
	require.Len(tb, got, len(texts))
	for i, err := range got {
		require.ErrorIs(tb, err, ErrLifecycleRefused, "report %d", i)
		require.NotErrorIs(tb, err, ErrFailed, "report %d", i)
		require.EqualError(tb, err, texts[i], "report %d", i)
	}
}

// bindJoinAgentWithProxy is bindJoinAgent plus a connected module proxy.
func bindJoinAgentWithProxy(
	tb testing.TB,
	svc ClusteredService,
	adder ResponsePublicationAdder,
	role codecs.Role,
) (*joinLog, *aeron.Publication) {
	tb.Helper()
	log := bindJoinAgent(tb, svc, adder, role)
	proxy, pub := boundModuleProxy(tb)
	log.agent.moduleProxy = proxy
	return log, pub
}

// TestAgent_RoleChange asserts each RoleChange call, the role it carries,
// and the agent state inside it.
func TestAgent_RoleChange(t *testing.T) {
	t.Run("join and unbind", func(t *testing.T) {
		adder := newFakeResponseAdder()
		svc := &lifecycleRecordingService{}
		log := bindJoinAgent(t, svc, adder, codecs.RoleFollower)
		agent := log.agent

		// Phase "follower bind": the first bind keeps FOLLOWER.
		require.Empty(t, svc.roles)
		require.Equal(t, codecs.RoleFollower, agent.Role())

		// Phase "promote": the bind calls RoleChange after the join egress rule
		// starts both adds.
		log.openEgress(1, 2)
		log.join(codecs.RoleLeader, false)
		agent.DoWork(0)
		require.False(t, agent.IsLogBound())
		require.Empty(t, svc.roles)
		log.rebind()
		require.Equal(t, []roleChangeRecord{{
			role:        codecs.RoleLeader,
			seenRole:    codecs.RoleLeader,
			logBound:    true,
			pendingAdds: 2,
		}}, svc.roles)
		log.deliver(encodeTrivialSessionMessage(t))
		require.Equal(t, []string{"start", "open", "open", "role", "message"}, svc.events())

		// Phase "leader rejoin": the unbind calls RoleChange after it closes both
		// publications.
		for id := int64(1); id <= 2; id++ {
			adder.next(id, responsePoll{
				publication: aeron.NewTestPublicationForHooksWithConnected(t, true),
				done:        true,
			})
		}
		agent.DoWork(0)
		require.Equal(t, 2, agent.ResponsePublicationCount())
		log.join(codecs.RoleLeader, false)
		agent.DoWork(0)
		require.False(t, agent.IsLogBound())
		require.Len(t, svc.roles, 2)
		require.Equal(t, roleChangeRecord{
			role:        codecs.RoleFollower,
			seenRole:    codecs.RoleFollower,
			pendingJoin: true,
		}, svc.roles[1])
		log.rebind()
		require.Equal(t, roleChangeRecord{
			role:        codecs.RoleLeader,
			seenRole:    codecs.RoleLeader,
			logBound:    true,
			pendingAdds: 2,
		}, svc.roles[2])
		want := []string{"start", "open", "open", "role", "message", "role", "role"}
		require.Equal(t, want, svc.events())

		// Phase "image close".
		aeron.ImageInternalCloseForHooks(log.image)
		agent.DoWork(0)
		require.False(t, agent.IsLogBound())
		require.Len(t, svc.roles, 4)
		require.Equal(t, roleChangeRecord{
			role:     codecs.RoleFollower,
			seenRole: codecs.RoleFollower,
		}, svc.roles[3])
		want = append(want, "role")
		require.Equal(t, want, svc.events())

		// Phase "demote".
		log.join(codecs.RoleLeader, true)
		log.rebind()
		require.Len(t, svc.roles, 5)
		require.Equal(t, codecs.RoleLeader, svc.roles[4].role)
		log.join(codecs.RoleFollower, false)
		agent.DoWork(0)
		require.False(t, agent.IsLogBound())
		require.Len(t, svc.roles, 6)
		require.Equal(t, roleChangeRecord{
			role:        codecs.RoleFollower,
			seenRole:    codecs.RoleFollower,
			pendingJoin: true,
		}, svc.roles[5])
		log.rebind()
		require.Len(t, svc.roles, 6)
		require.Equal(t, codecs.RoleFollower, agent.Role())
		want = append(want, "role", "role")
		require.Equal(t, want, svc.events())

		// Phase "close": Close on a bound leader keeps LEADER.
		log.join(codecs.RoleLeader, true)
		log.rebind()
		require.Len(t, svc.roles, 7)
		agent.Close()
		require.Len(t, svc.roles, 7)
		require.Equal(t, codecs.RoleLeader, agent.Role())
		require.NoError(t, agent.ServiceFailure())
		want = append(want, "role")
		require.Equal(t, want, svc.events())
	})

	t.Run("add report role", func(t *testing.T) {
		adder := newFakeResponseAdder()
		svc := &lifecycleRecordingService{}
		log := bindJoinAgent(t, svc, adder, codecs.RoleFollower)
		agent := log.agent
		var got []error
		var seen []codecs.Role
		agent.errorHandler = func(err error) {
			got = append(got, err)
			seen = append(seen, agent.Role())
		}
		log.openEgress(1)
		adder.offerErrs = map[int32]error{responseAddOf(1).streamID: errors.New("offer add boom")}

		log.join(codecs.RoleLeader, false)
		log.rebind()
		require.Len(t, got, 1)
		require.ErrorIs(t, got[0], ErrResponsePublicationAdd)
		require.Equal(t, []codecs.Role{codecs.RoleFollower}, seen)
		require.Equal(t, []roleChangeRecord{{
			role:     codecs.RoleLeader,
			seenRole: codecs.RoleLeader,
			logBound: true,
		}}, svc.roles)
		require.Equal(t, []string{"start", "open", "role"}, svc.events())
	})

	t.Run("seed recovery", func(t *testing.T) {
		const position int64 = 128
		svc := &lifecycleRecordingService{}
		sub, _ := newLogImage(t, testLogSessionID, testLogStreamID, position)
		agent := NewAgent(AgentConfig{
			Service:           svc,
			CommittedPosition: func() int64 { return position },
			Subscriptions:     &stubAdder{sub: sub},
		})
		agent.seedRecovery(codecs.RecoveryStateKeyFields{
			LeadershipTermID: 3,
			LogPosition:      position,
			Timestamp:        9,
		})
		event := defaultJoinEvent(position)
		event.role = codecs.RoleFollower
		agent.handleJoinLog(event)
		agent.DoWork(0)
		require.NoError(t, agent.ServiceFailure())
		require.True(t, agent.IsLogBound())
		require.Empty(t, svc.roles)
		require.Equal(t, codecs.RoleFollower, agent.Role())
	})

	t.Run("take", func(t *testing.T) {
		svc := &lifecycleRecordingService{}
		log, pub := bindJoinAgentWithProxy(t, svc, nil, codecs.RoleLeader)
		agent := log.agent
		fake := InstallSnapshotTakeFakeForTest(t, agent)
		const awaitStartTicks = 6
		fake.startSignals = make([]snapshotTakeFakeStep, awaitStartTicks)
		log.termEvent(t, clusterwire.ClusterTimeUnit_NANOS, 0)
		end := log.snapshot(t, 1)
		require.Len(t, svc.roles, 1)

		// The first tick stores the take. The image then closes. A leader join
		// arrives at the take position.
		agent.DoWork(0)
		require.NoError(t, agent.ServiceFailure())
		require.NotNil(t, agent.take)
		require.Equal(t, end, agent.LogPosition())
		aeron.ImageInternalCloseForHooks(log.image)
		log.join(codecs.RoleLeader, false)

		// Phase "await START": the take delays the unbind and the bind.
		for tick := range awaitStartTicks {
			agent.DoWork(0)
			require.NoError(t, agent.ServiceFailure(), "tick %d", tick)
			require.NotNil(t, agent.take, "tick %d", tick)
			require.Equal(t, takeAwaitingStart, agent.take.state, "tick %d", tick)
			require.True(t, agent.IsLogBound(), "tick %d", tick)
			require.True(t, agent.HasPendingJoin(), "tick %d", tick)
			require.Len(t, svc.roles, 1, "tick %d", tick)
		}

		// Phase "resolve": the take resolves, then the agent unbinds and binds.
		for tick := 0; tick < 8 && len(decodeServiceAcksFromPub(t, pub)) < 3; tick++ {
			agent.DoWork(0)
			require.NoError(t, agent.ServiceFailure(), "tick %d", tick)
		}
		require.Equal(t, []string{"start", "role", "snapshot", "role", "role"}, svc.events())
		wantRoles := []codecs.Role{codecs.RoleLeader, codecs.RoleFollower, codecs.RoleLeader}
		require.Equal(t, wantRoles, []codecs.Role{
			svc.roles[0].role,
			svc.roles[1].role,
			svc.roles[2].role,
		})
		acks := decodeServiceAcksFromPub(t, pub)
		require.Len(t, acks, 3)
		require.Equal(t, end, acks[1].LogPosition)
		require.Equal(t, fake.RecordingID(), acks[1].RelevantID)
		require.Equal(t, end, acks[2].LogPosition)
		require.Equal(t, nullValue, acks[2].RelevantID)
		require.True(t, agent.IsLogBound())
		require.False(t, agent.HasPendingJoin())
		require.NoError(t, agent.ServiceFailure())
	})
}

// TestAgent_BackgroundWork asserts one DoBackgroundWork call per DoWork call
// after a successful Start. An early DoWork return skips DoBackgroundWork.
func TestAgent_BackgroundWork(t *testing.T) {
	t.Run("first tick", func(t *testing.T) {
		svc := &lifecycleRecordingService{backgroundResult: 3}
		agent := NewAgent(AgentConfig{Service: svc})
		require.Equal(t, 4, agent.DoWork(5))
		require.Equal(t, []string{"start", "background"}, svc.order)
		require.Equal(t, []int64{5}, svc.backgroundNows)
	})

	t.Run("each tick", func(t *testing.T) {
		svc := &lifecycleRecordingService{}
		agent := NewAgent(AgentConfig{Service: svc})
		for _, nowNs := range []int64{7, 8, 9} {
			agent.DoWork(nowNs)
		}
		require.Equal(t, []int64{7, 8, 9}, svc.backgroundNows)
	})

	t.Run("negative count", func(t *testing.T) {
		svc := &lifecycleRecordingService{backgroundResult: -2}
		agent := NewAgent(AgentConfig{Service: svc})
		require.Equal(t, 1, agent.DoWork(0))
		require.Equal(t, 0, agent.DoWork(0))
		require.Len(t, svc.backgroundNows, 2)
	})

	t.Run("join wait", func(t *testing.T) {
		svc := &lifecycleRecordingService{}
		sub := aeron.NewSubscriptionForHooks(9001, testLogStreamID, testLogChannel)
		agent := NewAgent(AgentConfig{
			Service:       svc,
			Subscriptions: &stubAdder{sub: sub},
		})
		agent.handleJoinLog(defaultJoinEvent(0))
		for tick := range 3 {
			agent.DoWork(0)
			require.True(t, agent.HasPendingJoin(), "tick %d", tick)
			require.Len(t, svc.backgroundNows, tick+1)
		}
	})

	t.Run("ack wait", func(t *testing.T) {
		svc := &lifecycleRecordingService{}
		proxy, _ := boundModuleProxy(t)
		agent := NewAgent(AgentConfig{Service: svc, ModuleProxy: proxy})
		agent.handleRequestServiceAck(128)
		for tick := range 3 {
			agent.DoWork(0)
			require.NoError(t, agent.ServiceFailure(), "tick %d", tick)
			require.Equal(t, 1, agent.PendingCoordinationAcks(), "tick %d", tick)
			require.Equal(t, 0, agent.ReadyCoordinationAcks(), "tick %d", tick)
			require.Len(t, svc.backgroundNows, tick+1)
		}
	})

	t.Run("take wait", func(t *testing.T) {
		svc := &lifecycleRecordingService{}
		proxy, _ := boundModuleProxy(t)
		agent, image, committed := bindImage(t, AgentConfig{
			ServiceID:   1,
			Service:     svc,
			ModuleProxy: proxy,
		}, 0)
		fake := InstallSnapshotTakeFakeForTest(t, agent)
		fake.stopDelay = 3
		writeTermEventAndSnapshot(t, image, committed, 1)

		agent.DoWork(0)
		require.NotNil(t, agent.take)
		ticks := 0
		for agent.take != nil {
			require.Less(t, ticks, 10, "take did not resolve")
			calls := len(svc.backgroundNows)
			agent.DoWork(0)
			require.NoError(t, agent.ServiceFailure(), "tick %d", ticks)
			require.Len(t, svc.backgroundNows, calls+1, "tick %d", ticks)
			ticks++
		}
		require.GreaterOrEqual(t, ticks, 4)
		require.Equal(t, []string{"start", "snapshot"}, svc.events())
	})

	t.Run("start error", func(t *testing.T) {
		startErr := errors.New("start boom")
		svc := &lifecycleRecordingService{}
		svc.startErr = startErr
		agent := NewAgent(AgentConfig{
			Service:     svc,
			ModuleProxy: boundModuleProxyWithLimit(t, 0, false),
			StartAck: &StartAckConfig{
				LogPosition: 0,
				RelevantID:  1,
				Timeout:     10 * time.Nanosecond,
			},
		})
		agent.DoWork(0)
		agent.DoWork(1)
		require.NoError(t, agent.ServiceFailure())
		agent.DoWork(11)
		failure := agent.ServiceFailure()
		require.ErrorIs(t, failure, ErrFailed)
		require.ErrorIs(t, failure, ErrStart)
		require.ErrorIs(t, failure, startErr)
		require.Equal(t, 0, agent.DoWork(12))
		require.Empty(t, svc.backgroundNows)
	})

	t.Run("failure", func(t *testing.T) {
		svc := &lifecycleRecordingService{}
		agent, image, committed := bindAgent(t, svc, 0)
		calls := len(svc.backgroundNows)
		payload := encodeMessageHeader(t, 0, 1, 112, 0)
		*committed = int64(writeAgentFrame(t, image, 0, payload))
		require.Equal(t, 0, agent.DoWork(0))
		require.ErrorIs(t, agent.ServiceFailure(), ErrLogDispatchSchema)
		require.Equal(t, 0, agent.DoWork(0))
		require.Len(t, svc.backgroundNows, calls)
	})

	t.Run("closed", func(t *testing.T) {
		svc := &lifecycleRecordingService{}
		agent := NewAgent(AgentConfig{Service: svc})
		agent.DoWork(0)
		agent.Close()
		require.Equal(t, 0, agent.DoWork(0))
		require.Len(t, svc.backgroundNows, 1)
	})
}

type refusalRun struct {
	agent *Agent
	svc   *lifecycleRecordingService

	proxyPub *aeron.Publication

	// responsePub is the response publication the session offers to after
	// the call, or nil when the member is a follower.
	responsePub *aeron.Publication

	trigger func()

	// settle resolves a pending response add, or is nil.
	settle func()
}

const refusalSessionID int64 = 7

func setupStartRefusal(t *testing.T, handler func(error)) refusalRun {
	t.Helper()
	svc := &lifecycleRecordingService{}
	proxy, pub := boundModuleProxy(t)
	agent := NewAgent(AgentConfig{Service: svc, ModuleProxy: proxy, ErrorHandler: handler})
	require.NoError(t, agent.restoreClientSession(
		sampleClientSession(refusalSessionID, testResponseChannel),
	))
	return refusalRun{
		agent:    agent,
		svc:      svc,
		proxyPub: pub,
		trigger:  func() { agent.DoWork(0) },
	}
}

func setupEgressRefusal(
	t *testing.T,
	handler func(error),
	role codecs.Role,
) (refusalRun, *joinLog) {
	t.Helper()
	adder := newFakeResponseAdder()
	svc := &lifecycleRecordingService{}
	log, pub := bindJoinAgentWithProxy(t, svc, adder, role)
	agent := log.agent
	agent.errorHandler = handler
	log.openEgress(refusalSessionID)
	responsePub := aeron.NewTestPublicationForHooksWithConnected(t, true)
	return refusalRun{
		agent:       agent,
		svc:         svc,
		proxyPub:    pub,
		responsePub: responsePub,
		trigger:     func() { agent.DoWork(0) },
		settle: func() {
			adder.next(1, responsePoll{publication: responsePub, done: true})
			agent.DoWork(0)
			require.Equal(t, 1, agent.ResponsePublicationCount())
		},
	}, log
}

func setupRoleChangeRefusal(t *testing.T, handler func(error)) refusalRun {
	t.Helper()
	run, log := setupEgressRefusal(t, handler, codecs.RoleFollower)
	log.join(codecs.RoleLeader, false)
	run.trigger = log.rebind
	return run
}

func setupBackgroundRefusal(t *testing.T, handler func(error)) refusalRun {
	t.Helper()
	run, _ := setupEgressRefusal(t, handler, codecs.RoleLeader)
	run.settle()
	run.settle = nil
	return run
}

// setupTerminateRefusal stops a bound leader at its log position with the ack
// flag false, so the stop does not move the module publication.
func setupTerminateRefusal(t *testing.T, handler func(error)) refusalRun {
	t.Helper()
	run := setupBackgroundRefusal(t, handler)
	agent := run.agent
	run.trigger = func() {
		agent.handleServiceTerminationPosition(agent.LogPosition(), false)
		agent.DoWork(0)
		require.True(t, agent.IsTerminated())
	}
	return run
}

func publicationPosition(publication *aeron.Publication) int64 {
	if publication == nil {
		return 0
	}
	return publication.Position()
}

// TestAgent_LifecycleCalls_refused asserts the module request and session
// offer refusal inside Start, RoleChange, DoBackgroundWork, and Terminate.
func TestAgent_LifecycleCalls_refused(t *testing.T) {
	refusedResult := requestProbeResult{offer: LifecycleRefused}
	setups := []struct {
		// name is the lifecycle call name a refusal report carries.
		name string

		// call is the probes key of the lifecycle call.
		call  string
		setup func(*testing.T, func(error)) refusalRun
	}{
		{name: "Start", call: "start", setup: setupStartRefusal},
		{name: "RoleChange", call: "role", setup: setupRoleChangeRefusal},
		{name: "DoBackgroundWork", call: "background", setup: setupBackgroundRefusal},
		{name: "Terminate", call: "terminate", setup: setupTerminateRefusal},
	}
	for _, tc := range setups {
		t.Run(tc.name, func(t *testing.T) {
			var got []error
			run := tc.setup(t, collectErrors(&got))
			agent := run.agent
			probe := &requestProbe{sessionID: refusalSessionID}
			run.svc.probes = map[string]func(*Agent){tc.call: probe.run}
			proxySent := run.proxyPub.Position()
			responseSent := publicationPosition(run.responsePub)

			// Phase "refuse": the agent refuses every request inside the call.
			run.trigger()
			require.Equal(t, []requestProbeResult{refusedResult}, probe.runs)
			require.Equal(t, proxySent, run.proxyPub.Position())
			require.Equal(t, responseSent, publicationPosition(run.responsePub))
			session, ok := agent.ClientSession(refusalSessionID)
			require.True(t, ok)
			require.False(t, session.IsClosing())
			require.NoError(t, agent.ServiceFailure())
			requireRefusals(t, got, refusalTexts(tc.name))

			// Phase "after": a SessionMessage runs the same probe. The agent
			// accepts each request. The leader rows send egress. The follower
			// row mocks it.
			run.svc.probes = map[string]func(*Agent){"message": probe.run}
			if run.settle != nil {
				run.settle()
			}
			agent.handleSessionMessage(agent.LogPosition(), refusalSessionID, 1, []byte("m"))
			require.Len(t, probe.runs, 2)
			after := probe.runs[1]
			require.True(t, after.scheduled)
			require.True(t, after.cancelled)
			require.True(t, after.closed)
			if run.responsePub == nil {
				require.Equal(t, MockedOffer, after.offer)
			} else {
				require.Greater(t, after.offer, responseSent)
				require.Equal(t, run.responsePub.Position(), after.offer)
			}
			require.Greater(t, run.proxyPub.Position(), proxySent)
			require.Len(t, got, 4)
			require.NoError(t, agent.ServiceFailure())
		})
	}

	t.Run("terminate at close", func(t *testing.T) {
		var got []error
		run := setupBackgroundRefusal(t, collectErrors(&got))
		agent := run.agent
		probe := &requestProbe{sessionID: refusalSessionID}
		proxySent := run.proxyPub.Position()
		responseSent := run.responsePub.Position()

		// Close closes the response publication after Terminate, so the probe
		// reads both positions inside the call.
		var proxyInside, responseInside int64
		run.svc.probes = map[string]func(*Agent){"terminate": func(a *Agent) {
			probe.run(a)
			proxyInside = run.proxyPub.Position()
			responseInside = run.responsePub.Position()
		}}

		agent.Close()
		require.Equal(t, []requestProbeResult{refusedResult}, probe.runs)
		require.Equal(t, proxySent, proxyInside)
		require.Equal(t, responseSent, responseInside)
		require.NoError(t, agent.ServiceFailure())
		requireRefusals(t, got, refusalTexts("Terminate"))
	})

	t.Run("unknown id", func(t *testing.T) {
		var got []error
		run := setupStartRefusal(t, collectErrors(&got))
		probe := &requestProbe{sessionID: 99}
		run.svc.probes = map[string]func(*Agent){"start": probe.run}
		run.trigger()
		require.Equal(t, []requestProbeResult{{}}, probe.runs)
		requireRefusals(t, got, refusalTexts("Start")[:3])
		require.NotErrorIs(t, got[2], ErrCloseUnknownSession)
	})

	t.Run("closing", func(t *testing.T) {
		var got []error
		run := setupBackgroundRefusal(t, collectErrors(&got))
		agent := run.agent
		require.True(t, agent.CloseClientSession(refusalSessionID))
		session, ok := agent.ClientSession(refusalSessionID)
		require.True(t, ok)
		require.True(t, session.IsClosing())
		proxySent := run.proxyPub.Position()

		var closed []bool
		run.svc.probes = map[string]func(*Agent){"background": func(a *Agent) {
			closed = append(closed, a.CloseClientSession(refusalSessionID), session.Close())
		}}
		run.trigger()
		require.Equal(t, []bool{false, false}, closed)
		require.Equal(t, proxySent, run.proxyPub.Position())
		text := refusalTexts("DoBackgroundWork")[2]
		requireRefusals(t, got, []string{text, text})
		require.True(t, session.IsClosing())
	})

	t.Run("repeat", func(t *testing.T) {
		var got []error
		run := setupBackgroundRefusal(t, collectErrors(&got))
		probe := &requestProbe{sessionID: refusalSessionID}
		run.svc.probes = map[string]func(*Agent){"background": probe.run}
		run.trigger()
		run.trigger()
		require.Equal(t, []requestProbeResult{refusedResult, refusedResult}, probe.runs)
		requireRefusals(t, got, slices.Repeat(refusalTexts("DoBackgroundWork"), 2))
		for i := range 4 {
			require.Same(t, got[i], got[i+4], "report %d", i)
		}
	})

	t.Run("nil handler", func(t *testing.T) {
		run := setupBackgroundRefusal(t, nil)
		probe := &requestProbe{sessionID: refusalSessionID}
		run.svc.probes = map[string]func(*Agent){"background": probe.run}
		proxySent := run.proxyPub.Position()
		responseSent := run.responsePub.Position()
		run.trigger()
		require.Equal(t, []requestProbeResult{refusedResult}, probe.runs)
		require.Equal(t, proxySent, run.proxyPub.Position())
		require.Equal(t, responseSent, run.responsePub.Position())
		require.NoError(t, run.agent.ServiceFailure())
	})

	t.Run("start error", func(t *testing.T) {
		startErr := errors.New("start boom")
		var got []error
		run := setupStartRefusal(t, collectErrors(&got))
		run.svc.startErr = startErr
		probe := &requestProbe{sessionID: refusalSessionID}
		run.svc.probes = map[string]func(*Agent){"start": probe.run}
		run.trigger()
		require.Equal(t, []requestProbeResult{refusedResult}, probe.runs)
		require.Len(t, got, 5)
		requireRefusals(t, got[:4], refusalTexts("Start"))
		requireFailureReported(t, run.agent, got[4:], ErrStart, startErr)
	})

	t.Run("role change panic", func(t *testing.T) {
		svc := &lifecycleRecordingService{
			probes: map[string]func(*Agent){"role": func(*Agent) { panic("role boom") }},
		}
		log, pub := bindJoinAgentWithProxy(t, svc, nil, codecs.RoleFollower)
		agent := log.agent
		log.join(codecs.RoleLeader, true)
		agent.DoWork(0)
		require.PanicsWithValue(t, "role boom", func() { agent.DoWork(0) })
		require.Equal(t, codecs.RoleLeader, agent.Role())
		require.True(t, agent.IsLogBound())
		require.False(t, agent.HasPendingJoin())
		require.Len(t, svc.roles, 1)

		requireMessageSchedules(t, log, svc, pub)
		require.Len(t, svc.roles, 1)
		require.NoError(t, agent.ServiceFailure())
	})

	t.Run("background work panic", func(t *testing.T) {
		svc := &lifecycleRecordingService{}
		log, pub := bindJoinAgentWithProxy(t, svc, nil, codecs.RoleLeader)
		agent := log.agent
		svc.probes = map[string]func(*Agent){"background": func(*Agent) { panic("background boom") }}
		require.PanicsWithValue(t, "background boom", func() { agent.DoWork(0) })

		requireMessageSchedules(t, log, svc, pub)
		require.Equal(t, codecs.RoleLeader, agent.Role())
		require.NoError(t, agent.ServiceFailure())
	})
}

func requireMessageSchedules(
	t *testing.T,
	log *joinLog,
	svc *lifecycleRecordingService,
	pub *aeron.Publication,
) {
	t.Helper()
	var sent bool
	svc.probes = map[string]func(*Agent){"message": func(a *Agent) { sent = a.ScheduleTimer(1, 2) }}
	sentBefore := pub.Position()
	log.deliver(encodeTrivialSessionMessage(t))
	require.True(t, sent)
	require.Greater(t, pub.Position(), sentBefore)
}

// TestAgent_DoWork_zeroAllocs asserts an idle DoWork does not allocate, with
// and without the lifecycle extensions. A refusal inside DoBackgroundWork
// does not allocate either.
func TestAgent_DoWork_zeroAllocs(t *testing.T) {
	services := []struct {
		name  string
		build func() ClusteredService
	}{
		{name: "recordingService", build: func() ClusteredService { return &recordingService{} }},
		{
			name:  "lifecycleZeroAllocService",
			build: func() ClusteredService { return &lifecycleZeroAllocService{} },
		},
	}
	for _, tc := range services {
		t.Run(tc.name, func(t *testing.T) {
			unbound := NewAgent(AgentConfig{Service: tc.build()})
			unbound.DoWork(0)
			testutil.AssertZeroAllocs(t, "unbound DoWork", 100, func() {
				unbound.DoWork(0)
			})

			bound, _, _ := bindAgent(t, tc.build(), 0)
			bound.DoWork(0)
			testutil.AssertZeroAllocs(t, "bound DoWork", 100, func() {
				bound.DoWork(0)
			})
			require.NoError(t, bound.ServiceFailure())
		})
	}

	t.Run("refusal", func(t *testing.T) {
		reports := 0
		svc := &lifecycleZeroAllocService{refuse: true}
		agent := NewAgent(AgentConfig{
			Service:      svc,
			ErrorHandler: func(error) { reports++ },
		})
		agent.DoWork(0)
		require.Equal(t, 1, reports)
		calls := 0
		testutil.AssertZeroAllocs(t, "refusing DoWork", 100, func() {
			agent.DoWork(0)
			calls++
		})
		require.Equal(t, 1+calls, reports)
		require.NoError(t, agent.ServiceFailure())
	})
}

func TestIsFatal(t *testing.T) {
	inner := errors.New("inner")
	storage := &archive.Exception{ErrorCode: archive.ErrorStorageSpace}
	other := &archive.Exception{ErrorCode: archive.ErrorGeneric}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "ErrStart", err: ErrStart, want: true},
		{name: "ErrLogSubscriptionAdd", err: ErrLogSubscriptionAdd},
		{name: "ErrLogNotContiguous", err: ErrLogNotContiguous, want: true},
		{name: "ErrSessionClash", err: ErrSessionClash},
		{name: "ErrTimerPublish", err: ErrTimerPublish},
		{name: "ErrCloseSessionPublish", err: ErrCloseSessionPublish},
		{name: "ErrTakeSnapshot", err: ErrTakeSnapshot},
		{name: "ErrActionPositionMismatch", err: ErrActionPositionMismatch, want: true},
		{name: "ErrUnknownClusterAction", err: ErrUnknownClusterAction},
		{name: "ErrResponsePublicationAdd", err: ErrResponsePublicationAdd},
		{name: "ErrTransportTerminating", err: ErrTransportTerminating, want: true},
		{name: "ErrUnknownSessionClose", err: ErrUnknownSessionClose},
		{name: "ErrCloseUnknownSession", err: ErrCloseUnknownSession},
		{name: "ErrUnknownTimeUnit", err: ErrUnknownTimeUnit},
		{name: "ErrTerminate", err: ErrTerminate},
		{name: "ErrTerminationPositionPassed", err: ErrTerminationPositionPassed, want: true},
		{name: "ErrTerminationAckPublish", err: ErrTerminationAckPublish},
		{name: "ErrLifecycleRefused", err: ErrLifecycleRefused},
		{name: "ErrAckRequestPassed", err: ErrAckRequestPassed},
		{name: "ErrAckPublish", err: ErrAckPublish, want: true},
		{name: "ErrControlMessageSchema", err: ErrControlMessageSchema},
		{name: "ErrControlMessageDecode", err: ErrControlMessageDecode},
		{name: "ErrLogDispatchHeader", err: ErrLogDispatchHeader, want: true},
		{name: "ErrLogDispatchSchema", err: ErrLogDispatchSchema, want: true},
		{name: "ErrLogDispatchDecode", err: ErrLogDispatchDecode, want: true},
		{name: "ErrSnapshotNoArchive", err: ErrSnapshotNoArchive},
		{name: "ErrSnapshotTimeUnitUnknown", err: ErrSnapshotTimeUnitUnknown},
		{name: "ErrSnapshotPublicationAdd", err: ErrSnapshotPublicationAdd},
		{name: "ErrSnapshotRecordingStart", err: ErrSnapshotRecordingStart},
		{name: "ErrSnapshotWrite", err: ErrSnapshotWrite},
		{name: "ErrSnapshotRecordingStop", err: ErrSnapshotRecordingStop},
		{name: "ErrClientClosed", err: aeron.ErrClientClosed, want: true},
		{
			name: "wrapped ErrClientClosed",
			err:  fmt.Errorf("wrap: %w", aeron.ErrClientClosed),
			want: true,
		},
		{name: "ErrConductorTerminating", err: aeron.ErrConductorTerminating, want: true},
		{
			name: "wrapped ErrConductorTerminating",
			err:  fmt.Errorf("wrap: %w", aeron.ErrConductorTerminating),
			want: true,
		},
		{name: "storage space", err: fmt.Errorf("wrap: %w", storage), want: true},
		{name: "other archive code", err: fmt.Errorf("wrap: %w", other)},
		{name: "joined storage space last", err: errors.Join(other, storage), want: true},
		{name: "joined storage space first", err: errors.Join(storage, other), want: true},
		{
			name: "take wraps joined storage space",
			err:  fmt.Errorf("%w: %w", ErrTakeSnapshot, errors.Join(other, storage)),
			want: true,
		},
		{name: "joined other archive code", err: errors.Join(other, inner)},
		{
			name: "terminal ack wraps a non-fatal inner",
			err:  fmt.Errorf("%w: %d: %w", ErrAckPublish, aeron.Closed, inner),
			want: true,
		},
		{
			name: "timer offer with a nil inner",
			err:  fmt.Errorf("%w: %d", ErrTimerPublish, aeron.Closed),
		},
		{
			name: "transport wraps a pending start",
			err: fmt.Errorf(
				"%w: %w",
				ErrTransportTerminating,
				fmt.Errorf("%w: %w", ErrStart, inner),
			),
			want: true,
		},
		{name: "plain", err: inner},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isFatal(tt.err))
		})
	}
}

type backgroundCountingService struct {
	recordingService
	backgroundCalls int
}

func (s *backgroundCountingService) DoBackgroundWork(int64) int {
	s.backgroundCalls++
	return 0
}

type terminatingSubscriptionAdder struct {
	reporter *terminatingReporter
	err      error
}

func (a *terminatingSubscriptionAdder) AddSubscription(int32, string) (*aeron.Subscription, error) {
	a.reporter.terminating = true
	return nil, a.err
}

type terminatingResponseAdder struct {
	*fakeResponseAdder
	reporter *terminatingReporter
}

func (a *terminatingResponseAdder) PollAddPublication(
	correlationID int64,
) (*aeron.Publication, bool, error) {
	a.reporter.terminating = true
	return a.fakeResponseAdder.PollAddPublication(correlationID)
}

type terminatingSnapshotAdder struct {
	SnapshotPublicationAdder
	reporter *terminatingReporter
}

func (a *terminatingSnapshotAdder) OfferAddExclusivePublication(
	streamID int32,
	channel string,
) (int64, error) {
	a.reporter.terminating = true
	return a.SnapshotPublicationAdder.OfferAddExclusivePublication(streamID, channel)
}

func TestAgent_transportTerminating(t *testing.T) {
	t.Run("before first tick", func(t *testing.T) {
		svc := &recordingService{}
		archiveControl := &countingArchiveControl{pollWork: 3}
		var got []error
		agent, image, committed := bindImage(t, AgentConfig{
			Service:      svc,
			ErrorHandler: collectErrors(&got),
			Transport:    &terminatingReporter{terminating: true},
		}, 0)
		agent.archive = archiveControl
		log := &takeLog{image: image, committed: committed}
		log.timer(t, 1)

		require.Equal(t, 0, agent.DoWork(0))
		require.Equal(t, 0, svc.startCount)
		require.Equal(t, 0, archiveControl.polls)
		require.Nil(t, agent.take)
		require.Equal(t, int64(0), agent.LogPosition())
		require.Equal(t, int64(0), agent.adapter.Position())
		requireFailureReported(t, agent, got, ErrTransportTerminating)
	})

	t.Run("failed start waits then wraps", func(t *testing.T) {
		startErr := errors.New("start boom")
		svc := &recordingService{startErr: startErr}
		reporter := &terminatingReporter{}
		var got []error
		agent, _, _ := bindImage(t, AgentConfig{
			Service:      svc,
			ModuleProxy:  boundModuleProxyWithLimit(t, 0, false),
			ErrorHandler: collectErrors(&got),
			Transport:    reporter,
			StartAck:     &StartAckConfig{RelevantID: 1},
		}, 0)

		agent.DoWork(0)
		require.Equal(t, 1, svc.startCount)
		require.NoError(t, agent.ServiceFailure())
		require.Empty(t, got)

		reporter.terminating = true
		require.Equal(t, 0, agent.DoWork(0))
		requireFailureReported(t, agent, got, ErrTransportTerminating, ErrStart, startErr)
	})

	t.Run("storage space take waits then wraps", func(t *testing.T) {
		storage := &archive.Exception{ErrorCode: archive.ErrorStorageSpace}
		hookErr := fmt.Errorf("snap: %w", storage)
		var got []error
		take := bindDisconnectedTake(t, hookErr, collectErrors(&got))
		reporter := &terminatingReporter{}
		take.agent.transport = reporter
		writeTermEventAndSnapshot(t, take.image, take.committed, 321)

		take.agent.DoWork(0)
		take.agent.DoWork(0)
		require.NoError(t, take.agent.ServiceFailure())
		require.Empty(t, got)
		require.Equal(t, 1, take.agent.PendingCoordinationAcks())

		reporter.terminating = true
		require.Equal(t, 0, take.agent.DoWork(0))
		requireFailureReported(t, take.agent, got, ErrTransportTerminating, storage)
	})

	t.Run("join add error wraps", func(t *testing.T) {
		svc := &backgroundCountingService{}
		reporter := &terminatingReporter{}
		addErr := &aeron.DriverTimeoutError{Cause: errors.New("timeout")}
		var got []error
		agent := NewAgent(AgentConfig{
			Service:       svc,
			Subscriptions: &terminatingSubscriptionAdder{reporter: reporter, err: addErr},
			ErrorHandler:  collectErrors(&got),
			Transport:     reporter,
		})
		agent.handleJoinLog(defaultJoinEvent(0))

		agent.DoWork(0)
		requireFailureReported(t, agent, got, ErrTransportTerminating, ErrLogSubscriptionAdd, addErr)
		require.Zero(t, svc.backgroundCalls)
	})

	t.Run("join add error keeps a pending ack cause", func(t *testing.T) {
		reporter := &terminatingReporter{}
		addErr := &aeron.DriverTimeoutError{Cause: errors.New("timeout")}
		pending := fmt.Errorf("%w: pending", ErrAckPublish)
		var got []error
		agent := NewAgent(AgentConfig{
			Service:       &recordingService{},
			Subscriptions: &terminatingSubscriptionAdder{reporter: reporter, err: addErr},
			ErrorHandler:  collectErrors(&got),
			Transport:     reporter,
		})
		agent.coordinationAcks = append(agent.coordinationAcks, coordinationAck{failure: pending})
		agent.handleJoinLog(defaultJoinEvent(0))

		agent.DoWork(0)
		requireFailureReported(
			t, agent, got, ErrTransportTerminating, pending, ErrLogSubscriptionAdd, addErr,
		)
	})

	t.Run("response add poll error wraps", func(t *testing.T) {
		reporter := &terminatingReporter{}
		pollErr := &aeron.DriverTimeoutError{Cause: errors.New("timeout")}
		adder := &terminatingResponseAdder{fakeResponseAdder: newFakeResponseAdder(), reporter: reporter}
		var got []error
		log := bindHandledLeader(t, adder, collectErrors(&got))
		log.agent.transport = reporter
		log.open(7, 50)
		adder.next(1, responsePoll{err: pollErr})

		log.agent.DoWork(0)
		requireFailureReported(
			t, log.agent, got, ErrTransportTerminating, ErrResponsePublicationAdd, pollErr,
		)
	})

	t.Run("snapshot add error wraps once", func(t *testing.T) {
		reporter := &terminatingReporter{}
		addErr := &aeron.DriverTimeoutError{Cause: errors.New("timeout")}
		h := newTakeHarness(t, &snapshotRecordingService{})
		h.agent.transport = reporter
		h.fake.addOffers = []error{addErr}
		h.agent.snapshotPublications = &terminatingSnapshotAdder{
			SnapshotPublicationAdder: h.fake,
			reporter:                 reporter,
		}
		h.log.termEvent(t, clusterwire.ClusterTimeUnit_NANOS, 0)
		h.log.snapshot(t, 9)

		h.agent.DoWork(0)
		h.agent.DoWork(0)
		requireFailureReported(
			t, h.agent, h.errs, ErrTransportTerminating, ErrSnapshotPublicationAdd, addErr,
		)
		failureText := h.agent.ServiceFailure().Error()
		require.Equal(t, 1, strings.Count(failureText, ErrTransportTerminating.Error()))
	})

	nilTransports := []struct {
		name      string
		transport TerminationReporter
	}{
		{name: "nil transport"},
		{name: "typed nil transport", transport: (*terminatingReporter)(nil)},
	}

	for _, tc := range nilTransports {
		t.Run(tc.name, func(t *testing.T) {
			svc := &recordingService{}
			agent := NewAgent(AgentConfig{Service: svc, Transport: tc.transport})
			require.Equal(t, 1, agent.DoWork(0))
			require.Equal(t, 1, svc.startCount)
			require.NoError(t, agent.ServiceFailure())
		})
	}
}
