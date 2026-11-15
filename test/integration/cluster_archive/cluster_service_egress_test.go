//go:build integration && archive_live

package cluster_archive_test

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/atomicbuffer"
	"github.com/warrenguy/luas/cluster"
	"github.com/warrenguy/luas/cluster/client"
	"github.com/warrenguy/luas/cluster/codecs"
	"github.com/warrenguy/luas/cluster/service"
	"github.com/warrenguy/luas/test/integration/harness"
)

// egressEcho is one client echo with the term from its SessionMessageHeader.
// leadershipTermID is -1 when the header did not decode.
type egressEcho struct {
	sessionID        int64
	leadershipTermID int64
	payload          []byte
}

// egressRecorder keeps each client echo and close detail. Invoker-only:
// PollEgress runs on the test goroutine.
type egressRecorder struct {
	echoes        []egressEcho
	closedDetails []string
}

// handler returns client egress handlers that append to r. MessageFunc
// decodes the SessionMessageHeader that precedes the payload offset.
func (r *egressRecorder) handler() *client.EgressHandlers {
	return &client.EgressHandlers{
		MessageFunc: func(
			sessionID, _ int64,
			buffer atomicbuffer.AtomicBuffer,
			offset, length int,
			_ *aeron.Header,
		) {
			raw := buffer.Bytes()
			term := int64(-1)
			headerOffset := offset - client.SessionHeaderLength()
			if header, ok := codecs.TryDecodeSessionMessageHeaderAt(raw, headerOffset); ok {
				term = header.LeadershipTermID
			}
			r.echoes = append(r.echoes, egressEcho{
				sessionID:        sessionID,
				leadershipTermID: term,
				payload:          bytes.Clone(raw[offset : offset+length]),
			})
		},
		SessionEventFunc: closedDetailHandler(&r.closedDetails),
	}
}

// formatEgressState prints the client session state, each echo, and the
// closed details for a timeout dump.
func formatEgressState(egress *egressRecorder, session *client.AeronCluster) string {
	var b strings.Builder
	fmt.Fprintf(
		&b,
		"clientState=%s clientTerm=%d closed=%q\n",
		session.SessionState(),
		session.LeadershipTermID(),
		egress.closedDetails,
	)
	for i, echo := range egress.echoes {
		fmt.Fprintf(
			&b,
			"echo[%d] session=%d term=%d payload=%q\n",
			i,
			echo.sessionID,
			echo.leadershipTermID,
			echo.payload,
		)
	}
	return b.String()
}

func requireSessionOpenWithLeaderEgress(
	t *testing.T,
	sc serviceCluster,
	pump *transferPump,
	leader *cluster.ConsensusModule,
	sessionID int64,
) {
	t.Helper()

	leaderIndex := int(leader.ThisMemberID())
	requireServiceAwaitTick(
		t, sc,
		"session open committed",
		durableWorkBound,
		pump.sessionPump(func() bool {
			if leader.Agent().OpenSessionCount() != 1 {
				return false
			}
			for i, mem := range sc.members {
				if !mem.rec.holdsOpen(sessionID) || mem.agent.PendingResponsePublicationAdds() != 0 {
					return false
				}
				want := 0
				if i == leaderIndex {
					want = 1
				}
				if mem.agent.ResponsePublicationCount() != want {
					return false
				}
			}
			return true
		}),
	)
}

// requireFirstEcho offers payload and waits until every service holds it
// after the session open. The client must hold that one echo. Returns the
// echo.
func requireFirstEcho(
	t *testing.T,
	sc serviceCluster,
	pump *transferPump,
	egress *egressRecorder,
	session *client.AeronCluster,
	label string,
	payload []byte,
	leadershipTermID int64,
) egressEcho {
	t.Helper()

	sessionID := session.ClusterSessionID()
	egressDetail := func() string { return formatEgressState(egress, session) }
	offerTransferSessionMessage(t, sc.set, pump, payload, sc.dump)
	requireServiceAwaitTick(
		t, sc,
		label,
		durableWorkBound,
		pump.sessionPump(func() bool {
			for _, mem := range sc.members {
				if !mem.rec.holdsMessagesAfterOpen(sessionID, payload) {
					return false
				}
			}
			return len(egress.echoes) != 0
		}),
		egressDetail,
	)
	echo := egressEcho{sessionID: sessionID, leadershipTermID: leadershipTermID, payload: payload}
	require.Equal(t, []egressEcho{echo}, egress.echoes,
		"%s:\n%s\n%s", label, egressDetail(), sc.dump())
	return echo
}

// stopLeaderAndAwaitEgress stops the leader at leaderIndex once every
// service reaches commit. It waits for re-election, session recovery, and
// egress reconnect. Returns the new leader, the surviving follower index,
// and the service log floors at the stop.
func stopLeaderAndAwaitEgress(
	t *testing.T,
	sc serviceCluster,
	pump *transferPump,
	session *client.AeronCluster,
	leaderIndex int,
	preTerm int64,
) (
	newLeader *cluster.ConsensusModule,
	followerIndex int,
	logFloors []int64,
) {
	t.Helper()

	set := sc.set
	requireServicesAtCommit(t, sc, pump)
	preCommit := set.Module(leaderIndex).CommitPosition()
	logFloors = snapshotServiceLogPositions(sc)

	require.NoError(t, set.StopMember(leaderIndex))
	require.Nil(t, set.Module(leaderIndex))
	closeServiceTransferContainer(t, sc, leaderIndex)
	newLeader = requireTransferSessionRecovery(
		t, sc, pump, session, leaderIndex, preTerm, preCommit, logFloors,
	)
	newIndex := int(newLeader.ThisMemberID())
	followerIndex = firstNonLeaderIndex(set, newLeader.ThisMemberID())
	require.GreaterOrEqual(t, followerIndex, 0, "no surviving follower:\n%s", sc.dump())

	requireServiceAwaitTick(
		t, sc,
		"egress reconnect",
		durableWorkBound,
		pump.sessionPump(func() bool {
			failIfSurvivorStalled(t, sc, leaderIndex, logFloors)
			return serviceEgressReconnected(sc, newIndex, followerIndex)
		}),
	)
	return newLeader, followerIndex, logFloors
}

// serviceEgressReconnected reports whether egress settled after a leader
// change:
//   - the service at newIndex is a bound leader with no pending join and
//     holds one response publication
//   - the service at followerIndex is a bound follower and holds none
//   - neither service has a response add in flight
func serviceEgressReconnected(sc serviceCluster, newIndex, followerIndex int) bool {
	leader := sc.members[newIndex].agent
	if !leader.IsLogBound() || leader.HasPendingJoin() || leader.Role() != codecs.RoleLeader {
		return false
	}
	if leader.PendingResponsePublicationAdds() != 0 || leader.ResponsePublicationCount() != 1 {
		return false
	}
	follower := sc.members[followerIndex].agent
	if !follower.IsLogBound() || follower.Role() != codecs.RoleFollower {
		return false
	}
	return follower.PendingResponsePublicationAdds() == 0 &&
		follower.ResponsePublicationCount() == 0
}

// TestClusterServiceEgress_repliesAcrossLeaderStop asserts a client receives
// echoes and a close across a leader stop.
func TestClusterServiceEgress_repliesAcrossLeaderStop(t *testing.T) {
	t.Parallel()

	// Phase: open.
	sc := startServiceCluster(
		t,
		harness.ClusterServiceEgressPortBase,
		harness.ClusterServiceEgressLogEndpoint,
		amendServiceTransferMember,
	)
	set := sc.set
	for _, mem := range sc.members {
		mem.rec.echo = true
	}
	requireServicesJoined(t, sc)
	requireNoServiceFailures(t, sc, "")
	leader := requireSingleLeader(t, set, "")
	leaderIndex := int(leader.ThisMemberID())
	for i, mem := range sc.members {
		wantRole := codecs.RoleFollower
		if i == leaderIndex {
			wantRole = codecs.RoleLeader
		}
		require.Equal(t, wantRole, mem.agent.Role(), "service role index %d:\n%s", i, sc.dump())
		requireRoleSequence(t, mem, fmt.Sprintf("member %d after election", i))
	}

	// Phase: connect.
	egress := &egressRecorder{}
	pump, session := openTransferSession(
		t, sc, leader,
		harness.ClientEgressPort(harness.ClusterServiceEgressPortBase),
		egress.handler(),
	)
	sessionID := session.ClusterSessionID()
	egressDetail := func() string { return formatEgressState(egress, session) }

	// Phase: session open committed. Only the leader holds a publication.
	requireSessionOpenWithLeaderEgress(t, sc, pump, leader, sessionID)

	// Phase: echo before stop.
	first := []byte(serviceMessagePayload)
	preTerm := leader.LeadershipTermID()
	firstEcho := requireFirstEcho(t, sc, pump, egress, session, "echo before stop", first, preTerm)
	require.Equal(t, preTerm, session.LeadershipTermID(), sc.dump())
	for i, mem := range sc.members {
		if i == leaderIndex {
			require.Len(t, mem.rec.echoResults, 1,
				"leader echo results index %d:\n%s", i, sc.dump())
			require.Positive(t, mem.rec.echoResults[0],
				"leader echo result index %d:\n%s", i, sc.dump())
			continue
		}
		require.Equal(t, []int64{service.MockedOffer}, mem.rec.echoResults,
			"follower echo results index %d:\n%s", i, sc.dump())
	}

	// Phase: stop the leader, recover the session, and reconnect egress.
	backgroundBeforeStop := make([]int, len(sc.members))
	for i, mem := range sc.members {
		backgroundBeforeStop[i] = mem.rec.backgroundCalls
	}
	newLeader, followerIndex, logFloors := stopLeaderAndAwaitEgress(
		t, sc, pump, session, leaderIndex, preTerm,
	)
	newIndex := int(newLeader.ThisMemberID())
	survivors := []int{newIndex, followerIndex}
	for _, i := range survivors {
		mem := sc.members[i]
		requireRoleSequence(t, mem, fmt.Sprintf("member %d after leader stop", i))
		require.Greater(t, mem.rec.backgroundCalls, backgroundBeforeStop[i],
			"background calls index %d:\n%s", i, sc.dump())
	}
	newLeaderRoles := sc.members[newIndex].rec.roles
	require.Equal(t, codecs.RoleLeader, newLeaderRoles[len(newLeaderRoles)-1],
		"new leader roles %v:\n%s", newLeaderRoles, sc.dump())

	// Phase: echo after leader change.
	second := []byte(serviceSecondPayload)
	offerTransferSessionMessage(t, set, pump, second, sc.dump)
	requireServiceAwaitTick(
		t, sc,
		"echo after leader change",
		durableWorkBound,
		pump.sessionPump(func() bool {
			failIfSurvivorStalled(t, sc, leaderIndex, logFloors)
			failIfRecorderSequenceCorrupt(t, sc, leaderIndex, sessionID, egressDetail, first, second)
			return survivorServicesHoldSequence(sc, leaderIndex, sessionID, first, second) &&
				len(egress.echoes) >= 2
		}),
		egressDetail,
	)
	newTerm := newLeader.LeadershipTermID()
	require.Greater(t, newTerm, preTerm, sc.dump())
	require.Equal(t, newTerm, session.LeadershipTermID(), sc.dump())
	secondEcho := egressEcho{sessionID: sessionID, leadershipTermID: newTerm, payload: second}
	require.Equal(t, []egressEcho{firstEcho, secondEcho}, egress.echoes,
		"echoes after leader change:\n%s\n%s", egressDetail(), sc.dump())
	newResults := sc.members[newIndex].rec.echoResults
	require.Len(t, newResults, 2, "new leader echo results:\n%s", sc.dump())
	require.Equal(t, service.MockedOffer, newResults[0],
		"new leader echo result before stop:\n%s", sc.dump())
	require.Positive(t, newResults[1], "new leader echo result after stop:\n%s", sc.dump())
	followerResults := sc.members[followerIndex].rec.echoResults
	require.Equal(t, []int64{service.MockedOffer, service.MockedOffer}, followerResults,
		"follower echo results:\n%s", sc.dump())

	// Phase: service close across leader change. failIfRecorderSequenceCorrupt
	// fails on a close, so this wait omits it.
	closePayload := []byte(serviceClosePayload)
	for _, i := range survivors {
		sc.members[i].rec.ClosePayload = closePayload
	}
	offerTransferSessionMessage(t, set, pump, closePayload, sc.dump)
	wantClosed := []string{"SERVICE_ACTION"}
	requireServiceAwaitTick(
		t, sc,
		"service close across leader change",
		durableWorkBound,
		pump.sessionPump(func() bool {
			failIfSurvivorStalled(t, sc, leaderIndex, logFloors)
			return servicesClosedSession(sc, leaderIndex, sessionID) &&
				slices.Equal(egress.closedDetails, wantClosed) && session.IsClosed()
		}),
		egressDetail,
	)
	for _, i := range survivors {
		mem := sc.members[i]
		require.Equal(t, []bool{true}, mem.rec.CloseResults,
			"close results index %d:\n%s", i, sc.dump())
		require.Len(t, mem.rec.echoResults, 2,
			"echo results after close index %d:\n%s", i, sc.dump())
		require.Zero(t, mem.agent.ResponsePublicationCount(),
			"publications after close index %d:\n%s", i, sc.dump())
		require.Zero(t, mem.agent.SessionCount(),
			"service sessions after close index %d:\n%s", i, sc.dump())
		requireNoServiceFailure(t, sc, i, " after close")
	}
}

// TestClusterServiceEgress_restoredSessionsReplyAcrossLeaderStop asserts
// sessions that followers restored from a snapshot reply after the leader
// stops.
func TestClusterServiceEgress_restoredSessionsReplyAcrossLeaderStop(t *testing.T) {
	t.Parallel()

	sc := startServiceCluster(
		t,
		harness.ClusterServiceRestoredEgressPortBase,
		harness.ClusterServiceRestoredEgressLogEndpoint,
		amendServiceTransferMember,
	)
	set := sc.set
	for _, mem := range sc.members {
		mem.rec.echo = true
	}
	requireServicesJoined(t, sc)
	leader := requireSingleLeader(t, set, "")
	leaderIndex := int(leader.ThisMemberID())

	egress := &egressRecorder{}
	pump, session := openTransferSession(
		t, sc, leader,
		harness.ClientEgressPort(harness.ClusterServiceRestoredEgressPortBase),
		egress.handler(),
	)
	sessionID := session.ClusterSessionID()
	egressDetail := func() string { return formatEgressState(egress, session) }

	requireSessionOpenWithLeaderEgress(t, sc, pump, leader, sessionID)

	first := []byte(serviceMessagePayload)
	preTerm := leader.LeadershipTermID()
	firstEcho := requireFirstEcho(t, sc, pump, egress, session, "echo before snapshot", first, preTerm)

	_, actionPosition := requestSnapshotAction(t, sc, leader, pump, "snapshot")
	requireServiceAwaitTick(
		t, sc,
		"snapshot on every member",
		durableWorkBound,
		pump.sessionPump(func() bool {
			requireNoServiceFailures(t, sc, " during snapshot")
			return snapshotTakenOnEveryMember(t, set, actionPosition, sc.dump)
		}),
	)

	requireServiceAwaitTick(
		t, sc,
		"members at snapshot",
		durableWorkBound,
		pump.sessionPump(func() bool {
			return membersReachCommit(set, actionPosition) &&
				survivorsAppendAtLeast(set, nil, actionPosition)
		}),
	)

	followers := nonLeaderIndices(set, leader.ThisMemberID())
	require.Len(t, followers, 2, "followers:\n%s", sc.dump())
	for _, followerIndex := range followers {
		restartServiceMember(t, sc, followerIndex, nil)
		sc.members[followerIndex].rec.echo = true

		agent := sc.members[followerIndex].agent
		requireRestoredSession(
			t, agent, sc.members[followerIndex].rec, actionPosition, sessionID, sc.dump,
		)

		requireServiceAwaitTick(
			t, sc,
			"restarted follower settled",
			durableWorkBound,
			pump.sessionPump(func() bool {
				requireNoServiceFailure(t, sc, followerIndex, " during restart")
				module := set.Module(followerIndex)
				if !module.ElectionComplete() || module.Role() != codecs.RoleFollower {
					return false
				}
				if module.Agent().PendingServiceAckWaits() != 0 {
					return false
				}
				if !agent.IsLogBound() || agent.HasPendingJoin() {
					return false
				}
				return agent.LogPosition() >= leader.CommitPosition()
			}),
		)
	}

	// The original leader still leads at the pre-snapshot term. ElectionCount
	// 2 on a follower stalls the later re-election wait.
	still := requireSingleLeader(t, set, "before stop")
	require.Equal(t, leader.ThisMemberID(), still.ThisMemberID(), sc.dump())
	require.Equal(t, preTerm, still.LeadershipTermID(), sc.dump())
	for _, followerIndex := range followers {
		module := set.Module(followerIndex)
		require.Equal(
			t,
			int64(1),
			module.Agent().ElectionCount(),
			"member %d ElectionCount before stop:\n%s",
			module.ThisMemberID(),
			sc.dump(),
		)
	}

	newLeader, otherIndex, logFloors := stopLeaderAndAwaitEgress(
		t, sc, pump, session, leaderIndex, preTerm,
	)
	newIndex := int(newLeader.ThisMemberID())

	second := []byte(serviceSecondPayload)
	offerTransferSessionMessage(t, set, pump, second, sc.dump)
	requireServiceAwaitTick(
		t, sc,
		"echo after leader stop",
		durableWorkBound,
		pump.sessionPump(func() bool {
			failIfSurvivorStalled(t, sc, leaderIndex, logFloors)
			for _, index := range []int{newIndex, otherIndex} {
				rec := sc.members[index].rec
				corrupt := len(rec.opens) != 0 || len(rec.Closes) != 0 ||
					len(rec.messages) > 1 ||
					len(rec.messages) == 1 && !rec.messagesAre(sessionID, [][]byte{second})
				if corrupt {
					require.FailNow(
						t,
						"restored recorder sequence corrupted",
						"member=%d opens=%v closes=%v messages=%v:\n%s\n%s",
						set.Module(index).ThisMemberID(),
						rec.opens,
						rec.Closes,
						rec.messages,
						egressDetail(),
						sc.dump(),
					)
				}
				if !rec.holdsMessages(sessionID, second) {
					return false
				}
			}
			return len(egress.echoes) >= 2
		}),
		egressDetail,
	)

	newTerm := newLeader.LeadershipTermID()
	require.Greater(t, newTerm, preTerm, sc.dump())
	require.Equal(t, newTerm, session.LeadershipTermID(), sc.dump())
	secondEcho := egressEcho{sessionID: sessionID, leadershipTermID: newTerm, payload: second}
	require.Equal(t, []egressEcho{firstEcho, secondEcho}, egress.echoes,
		"echoes after leader stop:\n%s\n%s", egressDetail(), sc.dump())

	newResults := sc.members[newIndex].rec.echoResults
	require.Len(t, newResults, 1, "new leader echo results:\n%s", sc.dump())
	require.Positive(t, newResults[0], "new leader echo result:\n%s", sc.dump())

	otherResults := sc.members[otherIndex].rec.echoResults
	require.Equal(t, []int64{service.MockedOffer}, otherResults,
		"follower echo results:\n%s", sc.dump())
}

// TestClusterServiceEgress_backgroundTransportSession echoes one payload
// over a client with a background conductor. The session goroutine does not
// call DoWork.
func TestClusterServiceEgress_backgroundTransportSession(t *testing.T) {
	t.Parallel()

	sc := startWrappedServiceCluster(
		t,
		1,
		harness.ClusterBackgroundEgressPortBase,
		harness.ClusterBackgroundEgressLogEndpoint,
		amendServiceMember,
		nil,
	)
	for _, mem := range sc.members {
		mem.rec.echo = true
	}

	requireServicesJoined(t, sc)
	leader := requireSingleLeader(t, sc.set, "")
	ingress := leader.Topology().IngressEndpoints()

	md := harness.Start(t)
	transport := harness.ConnectBackgroundClient(t, md, aeron.Config{})
	require.False(t, transport.HasConductorInvoker())

	payload := []byte("background-echo")
	echoed := make(chan []byte, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sessionDone := make(chan error, 1)
	go func() {
		sessionDone <- runBackgroundEchoSession(t, ctx, ingress, transport, payload, echoed)
	}()

	var got []byte
	ok := sc.set.TickUntil(durableWorkBound, func() bool {
		select {
		case got = <-echoed:
			return true
		default:
			return false
		}
	})
	cancel()
	require.True(t, ok, "echo not received:\n%s", sc.dump())
	require.NoError(t, <-sessionDone)
	require.Equal(t, payload, got)
	require.Len(t, sc.members[0].rec.echoResults, 1, sc.dump())
	require.Positive(t, sc.members[0].rec.echoResults[0], sc.dump())
}

func runBackgroundEchoSession(
	t testing.TB,
	ctx context.Context,
	ingressEndpoints string,
	transport *aeron.Client,
	payload []byte,
	echoed chan<- []byte,
) error {
	t.Helper()

	saw := false
	session, err := client.Connect(ctx, client.Config{
		IngressChannel:      cluster.DefaultIngressChannel,
		IngressEndpoints:    ingressEndpoints,
		EgressChannel:       "aeron:udp?endpoint=127.0.0.1:0",
		Aeron:               transport,
		MessageTimeout:      durableWorkBound,
		IngressRetryTimeout: durableWorkBound,
		CredentialsSupplier: fixedCredentialsSupplier{
			credentials: []byte(sessionSmokeCredentials),
		},
		EgressHandler: &client.EgressHandlers{
			MessageBytesFunc: func(_, _ int64, got []byte, _ *aeron.Header) {
				if saw || !bytes.Equal(got, payload) {
					return
				}

				saw = true
				echoed <- bytes.Clone(got)
			},
		},
	})
	if err != nil {
		return err
	}
	defer func() { _ = session.Close() }()

	offered := false
	lastKeepalive := time.Now()
	for !saw {
		if err := ctx.Err(); err != nil {
			return err
		}

		if !offered {
			position := session.Offer(payload)
			if position > 0 {
				offered = true
			} else if !aeron.IsOfferRetriable(position) {
				return fmt.Errorf("cluster offer %d", position)
			}
		}

		session.PollEgress()

		if time.Since(lastKeepalive) >= time.Second {
			_ = session.SendKeepAlive()
			lastKeepalive = time.Now()
		}

		time.Sleep(time.Millisecond)
	}
	return nil
}
