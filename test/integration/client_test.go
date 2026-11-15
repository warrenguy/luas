//go:build integration

package integration_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/aeron/conductor"
	"github.com/warrenguy/luas/test/integration/harness"
)

const clientStayAliveDuration = 1 * time.Second

type clientRegHandle interface {
	RegistrationID() int64
}

func assertAddRejectedAfterClose(
	t *testing.T,
	md *harness.MediaDriver,
	streamID int32,
	add func(*aeron.Client, int32) (any, error),
) {
	t.Helper()

	// Client must stay closed; ConnectClient cleanup would re-close.
	client, err := aeron.Connect(t.Context(), aeron.Config{AeronDir: md.Dir})
	require.NoError(t, err)
	require.NoError(t, client.Close())
	require.Zero(t, client.DoWork())

	got, err := add(client, streamID)
	require.Nil(t, got)
	require.ErrorIs(t, err, aeron.ErrClientClosed)
	md.AssertProcessAlive(t)
}

func assertLookupHitThenMissAfterClose(
	t *testing.T,
	md *harness.MediaDriver,
	streamID int32,
	setup func(*aeron.Client, int32) (clientRegHandle, func() error),
	lookup func(*aeron.Client, int64) (any, bool),
	requireMiss func(testing.TB, *aeron.Client, int64),
) {
	t.Helper()

	client := harness.ConnectClient(t, md, aeron.Config{})

	handle, closeFn := setup(client, streamID)
	regID := handle.RegistrationID()
	got, ok := lookup(client, regID)
	require.True(t, ok)
	require.Same(t, handle, got)

	require.NoError(t, closeFn())
	requireMiss(t, client, regID)
	md.AssertProcessAlive(t)
}

// Client Add* IPC integration tests prove blocking AddPublication/AddSubscription
// and regID lookup on live aeronmd. Does not send Offer payloads or poll
// fragments; unit tests cover synthetic-driver error paths.
//
// Stream IDs 7101-7107 are fixed per test for readable isolation.
// Stream IDs 7201-7206 are reserved for exclusive publication client tests.

// TestClientAddPublicationIPC proves AddPublication blocks until ready, inserts
// the handle into the client map, and Publication(regID) returns the same pointer.
func TestClientAddPublicationIPC(t *testing.T) {
	md := harness.SharedParallel(t)

	client := harness.ConnectClient(t, md, aeron.Config{})

	const streamID int32 = 7101
	pub, err := client.AddPublication(streamID, harness.IPCChannel)
	require.NoError(t, err)
	require.NotNil(t, pub)
	require.Positive(t, pub.RegistrationID())

	regID := pub.RegistrationID()
	got, ok := client.Publication(regID)
	require.True(t, ok)
	require.Same(t, pub, got)

	require.NoError(t, pub.Close())
	md.AssertProcessAlive(t)
}

// TestClientAddPublicationIsConnectedEventually proves IsConnected becomes true once
// a matching IPC subscriber is registered and driver events are pumped via DoWork.
func TestClientAddPublicationIsConnectedEventually(t *testing.T) {
	md := harness.SharedParallel(t)

	client := harness.ConnectClient(t, md, aeron.Config{})

	const streamID int32 = 7102
	pub, err := client.AddPublication(streamID, harness.IPCChannel)
	require.NoError(t, err)
	require.NotNil(t, pub)
	require.False(t, pub.IsConnected())

	// Live IPC: pub is_connected stays false until a matching subscriber attaches.
	sub, err := client.AddSubscription(streamID, harness.IPCChannel)
	require.NoError(t, err)
	require.NotNil(t, sub)
	harness.CleanupClose(t, "close subscription", sub.Close)

	harness.WaitForIPCLinkConnected(t, client, pub, sub)

	require.NoError(t, pub.Close())
	md.AssertProcessAlive(t)
}

// TestClientSharedPublicationCloseFirstKeepsSecond closes one of two adds that
// share a driver publication, then offers on the other.
func TestClientSharedPublicationCloseFirstKeepsSecond(t *testing.T) {
	md := harness.SharedParallel(t)
	client, hadClientError := harness.ConnectIPCClient(t, md)

	const streamID = harness.SharedPublicationCloseStreamID
	first, sub := harness.ConnectIPCPubSubClients(t, client, client, streamID)
	second, err := client.AddPublication(streamID, harness.IPCChannel)
	require.NoError(t, err)
	harness.CleanupClose(t, "close second publication", second.Close)

	require.NotEqual(t, first.RegistrationID(), second.RegistrationID())
	require.Equal(t, first.RegistrationID(), first.OriginalRegistrationID())
	require.Equal(t, first.OriginalRegistrationID(), second.OriginalRegistrationID())

	require.NoError(t, first.Close())
	require.True(t, first.IsClosed())
	require.False(t, second.IsClosed())
	got, ok := client.Publication(second.RegistrationID())
	require.True(t, ok)
	require.Same(t, second, got)

	payload := []byte("shared publication")
	require.Positive(t, harness.OfferWithRetry(t, second, payload))
	require.Equal(t, [][]byte{payload}, harness.PollDelivered(t, client, sub, 1))

	require.NoError(t, second.Close())
	harness.RequirePublicationLookupMiss(t, client, second.RegistrationID())
	finishPubSubIPCTest(t, md, hadClientError)
}

// TestClientAddExclusivePublicationIPC proves AddExclusivePublication blocks until
// ready, inserts the handle into the client map, and ExclusivePublication(regID)
// returns the same pointer.
func TestClientAddExclusivePublicationIPC(t *testing.T) {
	md := harness.SharedParallel(t)

	client := harness.ConnectClient(t, md, aeron.Config{})

	const streamID int32 = 7201
	pub, err := client.AddExclusivePublication(streamID, harness.IPCChannel)
	require.NoError(t, err)
	require.NotNil(t, pub)
	require.Positive(t, pub.RegistrationID())

	regID := pub.RegistrationID()
	got, ok := client.ExclusivePublication(regID)
	require.True(t, ok)
	require.Same(t, pub, got)

	require.NoError(t, pub.Close())
	md.AssertProcessAlive(t)
}

// TestClientAddExclusivePublicationIsConnectedEventually proves IsConnected becomes true
// once a matching IPC subscriber is registered and driver events are pumped via DoWork.
func TestClientAddExclusivePublicationIsConnectedEventually(t *testing.T) {
	md := harness.SharedParallel(t)

	client := harness.ConnectClient(t, md, aeron.Config{})

	const streamID int32 = 7203
	pub, err := client.AddExclusivePublication(streamID, harness.IPCChannel)
	require.NoError(t, err)
	require.NotNil(t, pub)
	require.False(t, pub.IsConnected())

	// Live IPC: pub is_connected stays false until a matching subscriber attaches.
	sub, err := client.AddSubscription(streamID, harness.IPCChannel)
	require.NoError(t, err)
	require.NotNil(t, sub)
	harness.CleanupClose(t, "close subscription", sub.Close)

	harness.WaitForIPCLinkConnected(t, client, pub, sub)

	require.NoError(t, pub.Close())
	md.AssertProcessAlive(t)
}

// TestExclusivePublicationIPCRoundtrip proves exclusive IPC offer, poll delivery,
// and position advance on live aeronmd.
func TestExclusivePublicationIPCRoundtrip(t *testing.T) {
	md := harness.SharedParallel(t)
	client, hadClientError := harness.ConnectIPCClient(t, md)

	const (
		streamID     = int32(7202)
		messageCount = 4 // smoke: fewer fragments than TestPubSubIPCRoundtrip
		payloadSize  = 64
	)
	payloads := harness.DistinctIPCPayloads(messageCount, payloadSize, 0xE0)

	pub, sub := harness.ConnectExclusiveIPCPubSubClients(t, client, client, streamID)

	pubPosBefore := pub.Position()
	wantAdvance := harness.AlignedOfferAdvance(payloadSize, messageCount)
	for _, payload := range payloads {
		require.Positive(t, pub.Offer(payload))
		client.DoWork()
	}

	received := harness.PollDelivered(t, client, sub, messageCount)
	require.Equal(t, payloads, received)
	harness.AssertPubSubIPCMetadata(t, streamID, pub, sub, pubPosBefore, wantAdvance)
	finishPubSubIPCTest(t, md, hadClientError)
}

// TestExclusivePublicationIPCTwoClientsRoundtrip proves split-client exclusive IPC
// offer, poll delivery, and position advance on live aeronmd.
func TestExclusivePublicationIPCTwoClientsRoundtrip(t *testing.T) {
	md := harness.SharedParallel(t)

	pubClient, hadPubClientError := harness.ConnectIPCClient(t, md)
	subClient, hadSubClientError := harness.ConnectIPCClient(t, md)

	const (
		streamID     = harness.ExclusivePublicationTwoClientsRoundtripStreamID
		messageCount = 4 // smoke: fewer fragments than TestPubSubIPCTwoClientsRoundtrip
		payloadSize  = 64
	)
	payloads := harness.DistinctIPCPayloads(messageCount, payloadSize, 0xE2)

	pub, sub := harness.ConnectExclusiveIPCPubSubClients(t, pubClient, subClient, streamID)

	pubPosBefore := pub.Position()
	wantAdvance := harness.AlignedOfferAdvance(payloadSize, messageCount)
	for _, payload := range payloads {
		require.Positive(t, pub.Offer(payload))
		pubClient.DoWork()
		subClient.DoWork()
	}

	received := harness.PollDeliveredSplit(t, pubClient, subClient, sub, messageCount)
	require.Equal(t, payloads, received)

	harness.AssertPubSubIPCMetadata(t, streamID, pub, sub, pubPosBefore, wantAdvance)
	finishPubSubIPCTest(t, md, hadPubClientError, hadSubClientError)
}

// TestClientExclusivePublicationLookupByRegID proves lookup hits while registered and
// misses after Close removes the handle from the client map.
func TestClientExclusivePublicationLookupByRegID(t *testing.T) {
	md := harness.SharedParallel(t)

	assertLookupHitThenMissAfterClose(
		t,
		md,
		7204,
		func(client *aeron.Client, streamID int32) (clientRegHandle, func() error) {
			pub, err := client.AddExclusivePublication(streamID, harness.IPCChannel)
			require.NoError(t, err)
			require.NotNil(t, pub)
			return pub, pub.Close
		},
		func(client *aeron.Client, regID int64) (any, bool) {
			return client.ExclusivePublication(regID)
		},
		harness.RequireExclusivePublicationLookupMiss,
	)
}

// TestClientAddExclusivePublicationRejectedAfterClose proves AddExclusivePublication on a
// closed client returns ErrClientClosed without offering a new registration.
func TestClientAddExclusivePublicationRejectedAfterClose(t *testing.T) {
	md := harness.SharedParallel(t)

	assertAddRejectedAfterClose(t, md, 7205, func(client *aeron.Client, streamID int32) (any, error) {
		return client.AddExclusivePublication(streamID, harness.IPCChannel)
	})
}

// TestClientAddSubscriptionIPC proves AddSubscription blocks until ready and
// Subscription(regID) returns the same pointer with stream and channel set.
func TestClientAddSubscriptionIPC(t *testing.T) {
	md := harness.SharedParallel(t)

	client := harness.ConnectClient(t, md, aeron.Config{})

	const streamID int32 = 7103
	sub, err := client.AddSubscription(streamID, harness.IPCChannel)
	require.NoError(t, err)
	require.NotNil(t, sub)
	require.Positive(t, sub.RegistrationID())
	require.Equal(t, streamID, sub.StreamID())
	require.Equal(t, harness.IPCChannel, sub.Channel())

	regID := sub.RegistrationID()
	got, ok := client.Subscription(regID)
	require.True(t, ok)
	require.Same(t, sub, got)

	require.NoError(t, sub.Close())
	md.AssertProcessAlive(t)
}

// TestClientPublicationLookupByRegID proves lookup hits while registered and
// misses after Close removes the handle from the client map.
func TestClientPublicationLookupByRegID(t *testing.T) {
	md := harness.SharedParallel(t)

	assertLookupHitThenMissAfterClose(
		t,
		md,
		7104,
		func(client *aeron.Client, streamID int32) (clientRegHandle, func() error) {
			pub, err := client.AddPublication(streamID, harness.IPCChannel)
			require.NoError(t, err)
			require.NotNil(t, pub)
			return pub, pub.Close
		},
		func(client *aeron.Client, regID int64) (any, bool) {
			return client.Publication(regID)
		},
		harness.RequirePublicationLookupMiss,
	)
}

// TestClientAddPublicationRejectedAfterClose proves AddPublication on a closed
// client returns ErrClientClosed without offering a new registration.
func TestClientAddPublicationRejectedAfterClose(t *testing.T) {
	md := harness.SharedParallel(t)

	assertAddRejectedAfterClose(t, md, 7105, func(client *aeron.Client, streamID int32) (any, error) {
		return client.AddPublication(streamID, harness.IPCChannel)
	})
}

// TestClientAddSubscriptionRejectedAfterClose proves AddSubscription on a closed
// client returns ErrClientClosed without offering a new registration.
func TestClientAddSubscriptionRejectedAfterClose(t *testing.T) {
	md := harness.SharedParallel(t)

	assertAddRejectedAfterClose(t, md, 7106, func(client *aeron.Client, streamID int32) (any, error) {
		return client.AddSubscription(streamID, harness.IPCChannel)
	})
}

// TestClientSubscriptionLookupByRegID proves lookup hits while registered and
// misses after Close removes the handle from the client map.
func TestClientSubscriptionLookupByRegID(t *testing.T) {
	md := harness.SharedParallel(t)

	assertLookupHitThenMissAfterClose(
		t,
		md,
		7107,
		func(client *aeron.Client, streamID int32) (clientRegHandle, func() error) {
			sub, err := client.AddSubscription(streamID, harness.IPCChannel)
			require.NoError(t, err)
			require.NotNil(t, sub)
			return sub, sub.Close
		},
		func(client *aeron.Client, regID int64) (any, bool) {
			return client.Subscription(regID)
		},
		harness.RequireSubscriptionLookupMiss,
	)
}

func TestClientStayAlive(t *testing.T) {
	md := harness.SharedParallel(t)

	client := harness.ConnectClient(t, md, aeron.Config{
		ErrorHandler: func(err error) {
			require.FailNow(t, "unexpected client error during stay-alive", "%v", err)
		},
	})
	require.GreaterOrEqual(t, client.ClientId(), int64(0))
	meta, err := client.CncMetadata()
	require.NoError(t, err)
	require.Equal(
		t,
		time.Duration(meta.ClientLivenessTimeoutNs),
		client.InterServiceTimeout(),
	)

	deadline := time.Now().Add(clientStayAliveDuration)
	sawProgress := false
	for time.Now().Before(deadline) {
		if client.DoWork() > 0 {
			sawProgress = true
		}
		time.Sleep(aeron.DefaultIdleSleepDuration)
	}
	require.True(t, sawProgress, "client never made progress during stay-alive")
}

// TestClientCloseSkipsLingerWait checks that Close does not wait out ResourceLingerDuration.
func TestClientCloseSkipsLingerWait(t *testing.T) {
	md := harness.SharedParallel(t)
	resourceLinger := 15 * time.Second
	client, hadClientError := harness.ConnectIPCClientConfig(t, md, aeron.Config{
		ResourceLingerDuration: &resourceLinger,
	})
	pub, sub := harness.ConnectIPCPubSubClients(
		t, client, client, harness.ClientCloseSkipsLingerWaitStreamID,
	)

	started := time.Now()
	require.NoError(t, client.Close())

	// The Close response drain stops at DriverTimeout (10s), below the linger.
	require.Less(t, time.Since(started), resourceLinger)
	require.True(t, pub.IsClosed())
	require.True(t, sub.IsClosed())
	require.False(t, hadClientError.Load())
}

// TestClientSurvivesStallBelowLivenessTimeout stalls DoWork below the driver liveness timeout.
func TestClientSurvivesStallBelowLivenessTimeout(t *testing.T) {
	md := harness.SharedParallel(t)
	var hadClientError atomic.Bool
	client := harness.ConnectClient(t, md, aeron.Config{
		ErrorHandler: func(err error) {
			hadClientError.Store(true)
		},
	})

	stall := 1500 * time.Millisecond
	require.Greater(t, client.InterServiceTimeout(), stall)

	client.DoWork()
	time.Sleep(stall)
	client.DoWork()

	require.False(t, hadClientError.Load())
	require.False(t, client.IsTerminating())
}

// TestClientDetectsDriverDeath kills an ephemeral driver and expects
// DriverTimeoutError in each conductor mode.
func TestClientDetectsDriverDeath(t *testing.T) {
	t.Run("invoker", func(t *testing.T) {
		md := harness.StartParallel(t)

		driverTimeout := 2 * time.Second
		var gotErr error
		client := harness.ConnectClient(t, md, aeron.Config{
			DriverTimeout: driverTimeout,
			ErrorHandler: func(err error) {
				gotErr = err
			},
		})

		for range 20 {
			client.DoWork()
			time.Sleep(aeron.DefaultIdleSleepDuration)
		}

		require.NoError(t, md.Kill())

		deadline := time.Now().Add(driverTimeout + time.Second)
		for time.Now().Before(deadline) {
			client.DoWork()
			if gotErr != nil {
				var timeoutErr *aeron.DriverTimeoutError
				require.ErrorAs(t, gotErr, &timeoutErr)
				var staleErr *conductor.DriverKeepaliveStaleError
				require.ErrorAs(t, timeoutErr.Unwrap(), &staleErr)
				require.Greater(t, staleErr.AgeMs, staleErr.TimeoutMs)
				require.True(t, client.IsTerminating())
				return
			}
			time.Sleep(aeron.DefaultIdleSleepDuration)
		}

		require.Fail(t, "expected DriverTimeoutError after driver kill")
	})

	t.Run("background", func(t *testing.T) {
		md := harness.StartParallel(t)

		driverTimeout := 2 * time.Second
		var gotErr atomic.Pointer[aeron.DriverTimeoutError]
		client := harness.ConnectBackgroundClient(t, md, aeron.Config{
			DriverTimeout: driverTimeout,
			ErrorHandler: func(err error) {
				var timeoutErr *aeron.DriverTimeoutError
				if errors.As(err, &timeoutErr) {
					gotErr.CompareAndSwap(nil, timeoutErr)
				}
			},
		})

		require.NoError(t, md.Kill())

		require.Eventually(t, func() bool {
			return gotErr.Load() != nil
		}, driverTimeout+time.Second, aeron.DefaultIdleSleepDuration,
			"expected DriverTimeoutError after driver kill")

		require.True(t, client.IsTerminating())

		var timeoutErr *aeron.DriverTimeoutError
		require.ErrorAs(t, client.TerminationCause(), &timeoutErr)
	})
}

func TestClientReadsSystemCounters(t *testing.T) {
	md := harness.SharedParallel(t)

	client := harness.ConnectClient(t, md, aeron.Config{})

	countersReader, err := client.CountersReader()
	require.NoError(t, err)

	got, err := countersReader.CounterValue(conductor.SystemCounterControlProtocolVersion)
	require.NoError(t, err)
	require.Equal(t, int64(conductor.ControlProtocolVersion), got)
}

func TestClientErrorLogReadable(t *testing.T) {
	md := harness.SharedParallel(t)

	client := harness.ConnectClient(t, md, aeron.Config{})

	meta, err := client.CncMetadata()
	require.NoError(t, err)
	errorLog, err := client.ErrorLogBuffer()
	require.NoError(t, err)
	require.Len(t, errorLog, int(meta.ErrorLogBufferLength))
	require.False(t, cnc.ErrorLogExists(errorLog))
}

func TestClientTwoClientsSameDriver(t *testing.T) {
	md := harness.SharedParallel(t)

	clientA := harness.ConnectClient(t, md, aeron.Config{})
	clientB := harness.ConnectClient(t, md, aeron.Config{})

	clientIDA := clientA.ClientId()
	clientIDB := clientB.ClientId()
	require.GreaterOrEqual(t, clientIDA, int64(0))
	require.GreaterOrEqual(t, clientIDB, int64(0))
	require.NotEqual(t, clientIDA, clientIDB)

	for range 5 {
		clientA.DoWork()
		clientB.DoWork()
		time.Sleep(aeron.DefaultIdleSleepDuration)
	}

	md.AssertProcessAlive(t)

	require.NoError(t, clientA.Close())
	require.NoError(t, clientB.Close())
}

func TestTwoClientsConcurrentRingOffers(t *testing.T) {
	md, attach := harness.SharedAttachedCnc(t)
	// Harness attach and each Connect client map the same cnc.dat; ToDriver is one MPSC ring.
	toDriver := attach.Rings.ToDriver

	// Orphan ToClients responses from raw NextAvailableSessionID offers need a short
	// DriverTimeout and explicit DoWork pump before Close; ring drain shares that budget.
	var hadClientError atomic.Bool
	connectCfg := aeron.Config{
		AeronDir:            md.Dir,
		DriverTimeout:       harness.CommandPollDeadline,
		UseConductorInvoker: true,
		ErrorHandler: func(err error) {
			hadClientError.Store(true)
		},
	}

	clientA, err := aeron.Connect(t.Context(), connectCfg)
	require.NoError(t, err)
	clientB, err := aeron.Connect(t.Context(), connectCfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		_ = clientA.Close()
		_ = clientB.Close()
	})

	clientIDA := clientA.ClientId()
	clientIDB := clientB.ClientId()
	require.GreaterOrEqual(t, clientIDA, int64(0))
	require.GreaterOrEqual(t, clientIDB, int64(0))
	require.NotEqual(t, clientIDA, clientIDB)

	consumerBefore, producerBefore := harness.QuiescentToDriverSnapshotTimeout(
		t, toDriver, harness.CommandPollDeadline, clientA, clientB,
	)

	const offersPerClient = 32
	const actionsPerType = offersPerClient / 2
	const orphanResponsePumpRounds = 4 * offersPerClient
	minProducerDelta := int64(2*actionsPerType) * harness.AlignedDriverRecordLength(
		conductor.CorrelatedMessageLength,
	)
	minProducerDelta += int64(2*actionsPerType) * harness.AlignedDriverRecordLength(
		conductor.GetNextAvailableSessionIDBodyLength,
	)

	var wg sync.WaitGroup
	wg.Add(2)
	offerErrs := make(chan error, 2)

	offer := func(clientID, correlationBase int64) {
		defer wg.Done()

		proxy := conductor.NewDriverProxy(toDriver, clientID)
		for i := range offersPerClient {
			var offerErr error
			if i%2 == 0 {
				offerErr = proxy.ClientKeepalive()
			} else {
				offerErr = proxy.NextAvailableSessionID(correlationBase+int64(i), 100)
			}
			if offerErr != nil {
				offerErrs <- offerErr
				return
			}
		}
	}

	go offer(clientIDA, 10_000)
	go offer(clientIDB, 20_000)
	wg.Wait()
	close(offerErrs)
	for err := range offerErrs {
		require.NoError(t, err)
	}

	producerAfter := toDriver.ProducerPosition()
	require.GreaterOrEqual(t, producerAfter-producerBefore, minProducerDelta)

	consumerAfter, _ := harness.QuiescentToDriverSnapshotTimeout(
		t, toDriver, harness.CommandPollDeadline, clientA, clientB,
	)
	require.Greater(t, consumerAfter, consumerBefore)

	require.False(t, hadClientError.Load())
	require.False(t, clientA.IsTerminating())
	require.False(t, clientB.IsTerminating())

	for range orphanResponsePumpRounds {
		workA := clientA.DoWork()
		workB := clientB.DoWork()
		if workA == 0 && workB == 0 {
			break
		}
	}

	require.NoError(t, clientA.Close())
	require.NoError(t, clientB.Close())

	md.AssertProcessAlive(t)
}
