//go:build integration && archive_live

package cluster_archive_test

import (
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/aeron/conductor"
	"github.com/warrenguy/luas/archive"
	"github.com/warrenguy/luas/archive/codecs"
	"github.com/warrenguy/luas/archive/codecs/archivewire"
	"github.com/warrenguy/luas/test/integration/harness"
)

const archiveClientRecordingChannel = "aeron:ipc"

// TestArchiveClient_startAutoStopReleasesSubscription asserts the archive
// releases the recording subscription after STOP only when autoStop is true.
// After the release, AsyncStopRecording returns ErrorUnknownSubscription and
// AsyncListRecording reports the STOP position.
func TestArchiveClient_startAutoStopReleasesSubscription(t *testing.T) {
	t.Parallel()

	md := harness.Start(t)
	harness.StartArchiveNode(t, harness.ArchiveNodeConfig{
		AeronDir:       md.Dir,
		ControlChannel: harness.ArchiveClientControl0,
	})
	aeronClient, hadClientError := harness.ConnectIPCClient(t, md)

	archiveClient := connectArchiveClient(t, aeronClient)

	// Each case owns a stream id. The archive keys recording subscriptions on
	// channel and stream, so a kept subscription would refuse the next case.
	// The async case starts both recordings through AsyncStartRecording.
	cases := []struct {
		name     string
		autoStop bool
		async    bool
		streamID int32
	}{
		{name: "autoStop_false", autoStop: false, streamID: 4001},
		{name: "autoStop_true", autoStop: true, streamID: 4002},
		{name: "autoStop_true_async", autoStop: true, async: true, streamID: 4003},
	}
	channel := []byte(archiveClientRecordingChannel)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Phase: start the first recording. Wait for START.
			pub, err := aeronClient.AddPublication(tc.streamID, archiveClientRecordingChannel)
			require.NoError(t, err)
			harness.CleanupClose(t, "close first record publication", pub.Close)
			recordingID := awaitStartRecording(
				t,
				aeronClient,
				hadClientError,
				archiveClient,
				tc.streamID,
				tc.autoStop,
				tc.async,
			)

			// Phase: record one message so the STOP position is above zero.
			// Only the autoStop rows check it.
			if tc.autoStop {
				require.Positive(t, harness.OfferWithRetry(t, pub, []byte("recorded")))
			}

			// Phase: close the publication and wait for STOP.
			stopped := false
			var stopPosition int64
			archiveClient.SetRecordingSignalHandler(archive.RecordingSignalHandlerFunc(
				func(event codecs.RecordingSignalEventFields) {
					if event.Signal == archivewire.RecordingSignal_STOP &&
						event.RecordingID == recordingID {
						stopped = true
						stopPosition = event.Position
					}
				},
			))
			t.Cleanup(func() { archiveClient.SetRecordingSignalHandler(nil) })
			require.NoError(t, pub.Close())
			harness.PollClientUntilTransportClean(
				t,
				aeronClient,
				hadClientError,
				harness.ConnectedPollTimeout,
				"client transport error during recording stop signal poll",
				func() bool {
					_ = archiveClient.Poll()
					return stopped
				},
				func() string {
					return "archive recording STOP signal not received within " +
						harness.ConnectedPollTimeout.String()
				},
			)

			// Phase: start again on the same channel and stream.
			if !tc.autoStop {
				_, err = archiveClient.StartRecordingAwaitSignal(
					t.Context(),
					tc.streamID,
					archivewire.SourceLocation_LOCAL,
					false,
					channel,
					archive.NewStartSignalTracker(archiveClient.ControlSessionID()),
				)
				code, ok := archive.ExceptionCode(err)
				require.True(t, ok, "start error: %v", err)
				require.Equal(t, int32(archive.ErrorActiveSubscription), code)

				require.NoError(t, archiveClient.StopRecording(t.Context(), tc.streamID, channel))
				return
			}

			// Phase: the released subscription refuses a stop. The descriptor
			// reports the STOP position.
			requireStopUnknownSubscription(
				t,
				aeronClient,
				hadClientError,
				archiveClient,
				tc.streamID,
			)
			descriptor := awaitListRecording(t, aeronClient, hadClientError, archiveClient, recordingID)
			require.Equal(t, recordingID, descriptor.RecordingID)
			require.Greater(t, stopPosition, descriptor.StartPosition)
			require.Equal(t, stopPosition, descriptor.StopPosition)

			secondPub, err := aeronClient.AddPublication(tc.streamID, archiveClientRecordingChannel)
			require.NoError(t, err)
			harness.CleanupClose(t, "close second record publication", secondPub.Close)
			secondRecordingID := awaitStartRecording(
				t,
				aeronClient,
				hadClientError,
				archiveClient,
				tc.streamID,
				true,
				tc.async,
			)
			require.NotEqual(t, recordingID, secondRecordingID)
		})
	}
}

// TestArchiveClient_archiveIDMatchesArchive asserts ArchiveID returns the id in
// the archive ready marker.
func TestArchiveClient_archiveIDMatchesArchive(t *testing.T) {
	// No t.Parallel: the Archive binds the control port that
	// TestArchiveClient_startAutoStopReleasesSubscription also binds.
	md := harness.Start(t)
	node := harness.StartArchiveNode(t, harness.ArchiveNodeConfig{
		AeronDir:       md.Dir,
		ControlChannel: harness.ArchiveClientControl0,
	})
	aeronClient, _ := harness.ConnectIPCClient(t, md)

	archiveClient := connectArchiveClient(t, aeronClient)
	require.Equal(t, node.ArchiveID, strconv.FormatInt(archiveClient.ArchiveID(), 10))
}

// TestArchiveClient_errorResponseHandlerAndConnectivity asserts the handler
// receives an archive ERROR. IsConnected returns false after the archive
// stops.
func TestArchiveClient_errorResponseHandlerAndConnectivity(t *testing.T) {
	const unknownStreamID int32 = 4101
	const unknownRecordingID int64 = 424242

	md := harness.Start(t)
	node := harness.StartArchiveNode(t, harness.ArchiveNodeConfig{
		AeronDir:       md.Dir,
		ControlChannel: harness.ArchiveClientControl0,
	})
	aeronClient, hadClientError := harness.ConnectIPCClient(t, md)
	// The response subscription uses its own UDP endpoint. The request
	// publication is not an image on that subscription. IsConnected follows
	// the Archive response image.
	archiveClient := connectArchiveClientOn(
		t,
		aeronClient,
		harness.ArchiveClientControl0,
		archive.DefaultControlResponseChannel,
	)

	var errs []error
	archiveClient.SetErrorResponseHandler(func(err error) {
		errs = append(errs, err)
	})
	requireArchiveCode := func(err error, want int32) {
		t.Helper()

		code, ok := archive.ExceptionCode(err)
		require.True(t, ok, "err=%v", err)
		require.Equal(t, want, code)
	}
	require.True(t, archiveClient.IsConnected())

	offerClosedStop := func() {
		t.Helper()

		handle := archiveClient.NewAsyncStopRecording(
			unknownStreamID,
			[]byte(archiveClientRecordingChannel),
		)
		harness.PollClientUntilTransportClean(
			t,
			aeronClient,
			hadClientError,
			harness.ConnectedPollTimeout,
			"client transport error during stop offer",
			func() bool {
				offered, err := handle.TryOffer()
				require.NoError(t, err)
				return offered
			},
			func() string {
				return "stop offer not accepted within " + harness.ConnectedPollTimeout.String()
			},
		)
		handle.Close()
	}

	// Phase: between requests. A stop for a stream with no recording returns
	// ErrorUnknownSubscription.
	offerClosedStop()
	harness.PollClientUntilTransportClean(
		t,
		aeronClient,
		hadClientError,
		harness.ConnectedPollTimeout,
		"client transport error during error response poll",
		func() bool {
			_ = archiveClient.Poll()
			return len(errs) == 1
		},
		func() string {
			return "error response handler did not receive ErrorUnknownSubscription within " +
				harness.ConnectedPollTimeout.String()
		},
	)
	requireArchiveCode(errs[0], archive.ErrorUnknownSubscription)

	offerClosedStop()
	_, err := archiveClient.ListRecording(t.Context(), unknownRecordingID)
	requireArchiveCode(err, archive.ErrorUnknownRecording)
	require.Len(t, errs, 2)
	requireArchiveCode(errs[1], archive.ErrorUnknownSubscription)

	// Phase: lost archive. The session stays open.
	node.Stop()
	harness.PollClientUntilTransportClean(
		t,
		aeronClient,
		hadClientError,
		30*time.Second,
		"client transport error after archive stop",
		func() bool {
			_ = archiveClient.Poll()
			return !archiveClient.IsConnected()
		},
		func() string {
			return "IsConnected still true 30s after archive stop"
		},
	)
	require.False(t, archiveClient.IsConnected())
	require.False(t, archiveClient.IsClosed())
	require.NoError(t, archiveClient.TransportFault())
}

// TestArchiveClient_directAsyncConnectOwnedClient asserts that a direct
// AsyncConnect with a connect-owned transport client completes on Poll alone.
func TestArchiveClient_directAsyncConnectOwnedClient(t *testing.T) {
	// No t.Parallel: the Archive binds the control port that
	// TestArchiveClient_startAutoStopReleasesSubscription also binds.
	md := harness.Start(t)
	harness.StartArchiveNode(t, harness.ArchiveNodeConfig{
		AeronDir:       md.Dir,
		ControlChannel: harness.ArchiveClientControl0,
	})

	cfg := archive.Config{
		ControlRequestChannel:  archive.DefaultLocalControlChannel,
		ControlResponseChannel: archive.DefaultLocalControlChannel,
		AeronDir:               md.Dir,
	}.WithRemoteControl().Normalize()
	connect, err := archive.NewAsyncConnect(t.Context(), cfg)
	require.NoError(t, err)
	harness.CleanupClose(t, "close async connect", connect.Close)
	require.True(t, connect.OwnsAeronClient())

	// Phase: Poll alone drives the handshake.
	var result *archive.ConnectResult
	var pollErr error
	harness.PollUntilBool(10*time.Second, time.Millisecond, func() bool {
		result, pollErr = connect.Poll()
		return result != nil || pollErr != nil
	})
	require.NoError(t, pollErr)
	require.NotNil(t, result, "async connect did not complete in state %v", connect.State())
	harness.CleanupClose(t, "close owned client", result.Aeron.Close)
	harness.CleanupClose(t, "close control subscription", result.ControlSubscription.Close)
	harness.CleanupClose(t, "close control publication", result.ControlPublication.Close)

	require.True(t, result.OwnsAeronClient)
	require.True(t, result.ControlSubscription.IsConnected())
}

// TestArchiveClient_stopRecordingByIdentity asserts a new session stops an
// orphaned recording.
func TestArchiveClient_stopRecordingByIdentity(t *testing.T) {
	// No t.Parallel: the Archive binds the control port that
	// TestArchiveClient_startAutoStopReleasesSubscription also binds.
	const streamID int32 = 4004

	md := harness.Start(t)
	harness.StartArchiveNode(t, harness.ArchiveNodeConfig{
		AeronDir:       md.Dir,
		ControlChannel: harness.ArchiveClientControl0,
	})

	// Phase: client A starts a recording with autoStop true. Wait until the
	// recording holds one message.
	aeronA, hadClientErrorA := harness.ConnectIPCClient(t, md)
	archiveA := connectArchiveClient(t, aeronA)
	pub, err := aeronA.AddPublication(streamID, archiveClientRecordingChannel)
	require.NoError(t, err)
	harness.CleanupClose(t, "close record publication", pub.Close)
	recordingID := awaitStartRecording(
		t,
		aeronA,
		hadClientErrorA,
		archiveA,
		streamID,
		true,
		false,
	)
	recordedPosition := harness.OfferWithRetry(t, pub, []byte("recorded"))
	require.Positive(t, recordedPosition)
	counters, err := aeronA.CountersReader()
	require.NoError(t, err)
	harness.PollClientUntilTransportClean(
		t,
		aeronA,
		hadClientErrorA,
		harness.ConnectedPollTimeout,
		"client transport error during recording position poll",
		func() bool {
			slot, ok := counters.FindRecordingPositionCounter(recordingID, cnc.NullValue)
			return ok && slot.Get() >= recordedPosition
		},
		func() string {
			return "recording position not at " + strconv.FormatInt(recordedPosition, 10) +
				" within " + harness.ConnectedPollTimeout.String()
		},
	)

	// Phase: client A closes its control session. The publication stays open.
	// Wait until the archive drops the session.
	sessionsCounterID, ok := counters.FindCounterOfType(cnc.ArchiveControlSessionsCounterTypeID)
	require.True(t, ok, "archive control sessions counter not found")
	sessionsWithA, err := counters.CounterValue(sessionsCounterID)
	require.NoError(t, err)
	require.Positive(t, sessionsWithA)
	require.NoError(t, archiveA.Close())
	harness.PollClientUntilTransportClean(
		t,
		aeronA,
		hadClientErrorA,
		harness.ConnectedPollTimeout,
		"client transport error during control session close poll",
		func() bool {
			sessions, err := counters.CounterValue(sessionsCounterID)
			require.NoError(t, err)
			return sessions < sessionsWithA
		},
		func() string {
			return "archive kept the closed control session within " +
				harness.ConnectedPollTimeout.String()
		},
	)

	// Phase: client B lists the recording. The stop position is null: the
	// closed control session left the recording active.
	aeronB, hadClientErrorB := harness.ConnectIPCClient(t, md)
	archiveB := connectArchiveClient(t, aeronB)
	stopSignals := 0
	archiveB.SetRecordingSignalHandler(archive.RecordingSignalHandlerFunc(
		func(event codecs.RecordingSignalEventFields) {
			if event.Signal == archivewire.RecordingSignal_STOP &&
				event.RecordingID == recordingID {
				stopSignals++
			}
		},
	))
	descriptor := awaitListRecording(t, aeronB, hadClientErrorB, archiveB, recordingID)
	require.Equal(t, cnc.NullValue, descriptor.StopPosition)

	// Phase: client B stops the recording by identity.
	handle := archiveB.NewAsyncStopRecordingByIdentity(recordingID)
	pollAsyncRequest(
		t,
		aeronB,
		hadClientErrorB,
		archiveB,
		"async stop recording by identity",
		handle.TryOffer,
		func() bool {
			done, err := handle.PollResponse()
			require.NoError(t, err)
			return done
		},
	)
	require.True(t, handle.IsStopped(), "stop must remove the active recording subscription")
	handle.Close()

	// Phase: the catalog stop appears once the archive closes the recording
	// session. Each list request runs under the one poll deadline.
	var list *archive.AsyncListRecording
	t.Cleanup(func() { list.Close() })
	listOffered := false
	harness.PollClientUntilTransportClean(
		t,
		aeronB,
		hadClientErrorB,
		harness.ConnectedPollTimeout,
		"client transport error during catalog stop poll",
		func() bool {
			if list == nil {
				list = archiveB.NewAsyncListRecording(recordingID)
				listOffered = false
			}
			if !listOffered {
				var err error
				listOffered, err = list.TryOffer()
				require.NoError(t, err)
				if !listOffered {
					return false
				}
			}
			_ = archiveB.Poll()
			done, got, err := list.PollDescriptor()
			require.NoError(t, err)
			if !done {
				return false
			}
			list.Close()
			list = nil
			descriptor = got
			return descriptor.StopPosition != cnc.NullValue
		},
		func() string {
			return "catalog stop position not set within " +
				harness.ConnectedPollTimeout.String()
		},
	)
	require.Greater(t, descriptor.StopPosition, descriptor.StartPosition)
	require.Equal(t, recordedPosition, descriptor.StopPosition)
	require.Zero(t, stopSignals, "the stopping session must get no STOP signal")

	// Phase: a second stop finds no active recording.
	stopped, err := archiveB.StopRecordingByIdentity(t.Context(), recordingID)
	require.NoError(t, err)
	require.False(t, stopped, "stopped recording must report no subscription removed")

	// Phase: an unknown id fails with ErrorUnknownRecording.
	_, err = archiveB.StopRecordingByIdentity(t.Context(), recordingID+1000)
	code, ok := archive.ExceptionCode(err)
	require.True(t, ok, "stop error: %v", err)
	require.Equal(t, int32(archive.ErrorUnknownRecording), code)
	require.Zero(t, stopSignals, "the stopping session must get no STOP signal")
}

// TestArchiveClient_ownedTransportDriverShutdownClosesSession asserts that a
// driver shutdown under a Connect-owned transport client closes the session.
// The shutdown also ends an in-flight await with the driver timeout cause.
func TestArchiveClient_ownedTransportDriverShutdownClosesSession(t *testing.T) {
	t.Parallel()

	t.Run("poll", func(t *testing.T) {
		archiveClient, clientDriver := connectOwnedFaultClient(t)
		require.False(t, archiveClient.IsClosed())
		aeronClient := archiveClient.Config().Aeron
		require.NotNil(t, aeronClient)

		// Phase: shut down the client driver. Poll closes the session.
		require.NoError(t, clientDriver.Shutdown())
		closed := harness.PollUntilBool(5*time.Second, aeron.DefaultIdleSleepDuration, func() bool {
			aeronClient.DoWork()
			archiveClient.Poll()
			return archiveClient.IsClosed()
		})
		require.True(t, closed, "session did not close within 5s")
		requireOwnedDriverShutdownFault(t, archiveClient.TransportFault())
	})

	t.Run("in_flight_await", func(t *testing.T) {
		archiveClient, clientDriver := connectOwnedFaultClient(t)
		var shutdownAt time.Time
		shutdownErr := make(chan error, 1)
		go func() {
			time.Sleep(500 * time.Millisecond)
			shutdownAt = time.Now()
			shutdownErr <- clientDriver.Shutdown()
		}()

		// The archive sends START only after an image appears. This test does not
		// publish, so the call blocks.
		_, err := archiveClient.StartRecordingAwaitSignal(
			t.Context(),
			4101,
			archivewire.SourceLocation_LOCAL,
			true,
			[]byte(archiveClientRecordingChannel),
			archive.NewStartSignalTracker(archiveClient.ControlSessionID()),
		)
		returnedAt := time.Now()
		require.NoError(t, <-shutdownErr)
		require.False(t, returnedAt.Before(shutdownAt))
		require.Less(t, returnedAt.Sub(shutdownAt), 5*time.Second)
		requireOwnedDriverShutdownFault(t, err)
		require.True(t, archiveClient.IsClosed())
	})
}

func connectOwnedFaultClient(t testing.TB) (*archive.Client, *harness.MediaDriver) {
	t.Helper()

	archiveDriver := harness.Start(t)
	harness.StartArchiveNode(t, harness.ArchiveNodeConfig{
		AeronDir:       archiveDriver.Dir,
		ControlChannel: harness.ArchiveClientFaultControl0,
	})
	clientDriver := harness.Start(t)
	cfg := archive.Config{
		ControlRequestChannel:  harness.ArchiveClientFaultControl0,
		ControlResponseChannel: "aeron:udp?endpoint=127.0.0.1:0",
		AeronDir:               clientDriver.Dir,
		ControlResponseTimeout: 10 * time.Second,
	}.WithRemoteControl().Normalize()
	archiveClient, err := archive.Connect(t.Context(), cfg)
	require.NoError(t, err, "connect over UDP control")
	harness.CleanupClose(t, "close archive client", archiveClient.Close)
	return archiveClient, clientDriver
}

func requireOwnedDriverShutdownFault(t testing.TB, fault error) {
	t.Helper()

	require.ErrorIs(t, fault, archive.ErrTransportTerminating)
	_, ok := errors.AsType[*aeron.DriverTimeoutError](fault)
	require.True(t, ok, "fault: %v", fault)
	require.ErrorIs(t, fault, conductor.ErrDriverShutdown)
}

// TestArchiveClient_responseChannelConnect asserts connect succeeds with a
// control-mode=response response channel over UDP and over IPC.
func TestArchiveClient_responseChannelConnect(t *testing.T) {
	md := harness.StartParallel(t)
	ports := harness.FreeUDPPorts(t, 2)
	controlPort, responsePort := ports[0], ports[1]
	node := harness.StartArchiveNode(t, harness.ArchiveNodeConfig{
		AeronDir:       md.Dir,
		ControlChannel: harness.ArchiveControlChannel(controlPort),
	})
	aeronClient, _ := harness.ConnectIPCClient(t, md)

	tests := []struct {
		name            string
		requestChannel  string
		responseChannel string
	}{
		{
			name:           "udp",
			requestChannel: harness.ArchiveControlChannel(controlPort),
			responseChannel: "aeron:udp?control-mode=response|control=" +
				harness.LoopbackUDPEndpoint(responsePort),
		},
		{
			name:            "ipc",
			requestChannel:  archive.DefaultLocalControlChannel,
			responseChannel: "aeron:ipc?control-mode=response",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := archive.Config{
				ControlRequestChannel:  tc.requestChannel,
				ControlResponseChannel: tc.responseChannel,
				Aeron:                  aeronClient,
			}.WithRemoteControl().Normalize()
			archiveClient, err := archive.Connect(t.Context(), cfg)
			require.NoError(t, err)
			harness.CleanupClose(t, "close archive client", archiveClient.Close)

			require.Equal(t, node.ArchiveID, strconv.FormatInt(archiveClient.ArchiveID(), 10))
			effective := archiveClient.Config()
			requestChannel := effective.EffectiveControlRequestChannel()
			responseChannel := effective.EffectiveControlResponseChannel()
			require.Contains(t, requestChannel, aeron.ChannelResponseCorrelationIDParam)
			require.NotContains(t, requestChannel, aeron.ChannelSessionIDParam)
			require.NotContains(t, responseChannel, aeron.ChannelSessionIDParam)
		})
	}
}

// TestArchiveClient_controlChannelSessionID asserts both control channels carry
// one session-id.
func TestArchiveClient_controlChannelSessionID(t *testing.T) {
	md := harness.StartParallel(t)
	controlPort := harness.FreeUDPPorts(t, 1)[0]
	harness.StartArchiveNode(t, harness.ArchiveNodeConfig{
		AeronDir:       md.Dir,
		ControlChannel: harness.ArchiveControlChannel(controlPort),
	})
	aeronClient, _ := harness.ConnectIPCClient(t, md)

	tests := []struct {
		name          string
		cfg           archive.Config
		channelPrefix string
	}{
		{
			name:          "ipc_first",
			cfg:           archive.Config{Aeron: aeronClient}.Normalize(),
			channelPrefix: "aeron:ipc?",
		},
		{
			name: "udp",
			cfg: archive.Config{
				ControlRequestChannel:  harness.ArchiveControlChannel(controlPort),
				ControlResponseChannel: "aeron:udp?endpoint=127.0.0.1:0",
				Aeron:                  aeronClient,
			}.WithRemoteControl().Normalize(),
			channelPrefix: "aeron:udp?",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Phase: connect two sessions on one Archive.
			sessions := []*archive.ConnectResult{
				connectControlSession(t, aeronClient, tc.cfg),
				connectControlSession(t, aeronClient, tc.cfg),
			}
			sessionIDs := make([]int32, len(sessions))
			for i, session := range sessions {
				cfg := session.Config
				sessionIDs[i] = requireControlChannelSessionID(t, cfg)
				requestChannel := cfg.EffectiveControlRequestChannel()
				responseChannel := cfg.EffectiveControlResponseChannel()
				require.True(t, strings.HasPrefix(requestChannel, tc.channelPrefix), requestChannel)
				require.True(t, strings.HasPrefix(responseChannel, tc.channelPrefix), responseChannel)
				require.Equal(t, int32(10), cfg.EffectiveControlRequestStreamID())
				require.Equal(t, int32(20), cfg.EffectiveControlResponseStreamID())
			}
			require.NotEqual(t, sessionIDs[0], sessionIDs[1])

			// Phase: each control subscription holds one image from its own
			// session.
			harness.RunSoak(t, 200*time.Millisecond, time.Millisecond, func() {
				aeronClient.DoWork()
			})
			for i, session := range sessions {
				sub := session.ControlSubscription
				require.Equal(t, 1, sub.ImageCount(), "session %d images", i)
				image, ok := sub.ImageAt(0)
				require.True(t, ok)
				require.Equal(t, sessionIDs[i], image.SessionID())
			}
		})
	}
}

// TestArchiveClient_blockingAwaitEndsOnLostArchive asserts a blocking await
// returns ErrNotConnected once the stopped Archive's response image closes.
func TestArchiveClient_blockingAwaitEndsOnLostArchive(t *testing.T) {
	md := harness.StartParallel(t)
	node := harness.StartArchiveNode(t, harness.ArchiveNodeConfig{
		AeronDir:       md.Dir,
		ControlChannel: harness.ArchiveControlChannel(harness.FreeUDPPort(t)),
	})
	aeronClient, _ := harness.ConnectIPCClient(t, md)
	archiveClient := connectArchiveClientConfig(t, archive.Config{
		ControlRequestChannel:  archive.DefaultLocalControlChannel,
		ControlResponseChannel: archive.DefaultLocalControlChannel,
		ControlResponseTimeout: 30 * time.Second,
		Aeron:                  aeronClient,
	})

	// Phase: kill the Archive while the call waits for the START signal.
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		time.Sleep(500 * time.Millisecond)
		node.Stop()
	}()

	_, err := archiveClient.StartRecordingAwaitSignal(
		t.Context(),
		4711,
		archivewire.SourceLocation_LOCAL,
		false,
		[]byte(archiveClientRecordingChannel),
		archive.NewStartSignalTracker(archiveClient.ControlSessionID()),
	)
	<-stopped

	// Phase: the await ends on the lost response image. The session stays
	// open.
	require.ErrorIs(t, err, archive.ErrNotConnected)
	require.False(t, archiveClient.IsClosed())
	require.NoError(t, archiveClient.TransportFault())
}

// connectControlSession runs one AsyncConnect on cfg. It calls
// aeronClient.DoWork before each poll. Cleanup closes the control publication
// and subscription.
func connectControlSession(
	t *testing.T,
	aeronClient *aeron.Client,
	cfg archive.Config,
) *archive.ConnectResult {
	t.Helper()

	connect, err := archive.NewAsyncConnect(t.Context(), cfg)
	require.NoError(t, err)
	harness.CleanupClose(t, "close async connect", connect.Close)

	var result *archive.ConnectResult
	var pollErr error
	harness.PollUntilBool(10*time.Second, time.Millisecond, func() bool {
		aeronClient.DoWork()
		result, pollErr = connect.Poll()
		return result != nil || pollErr != nil
	})
	require.NoError(t, pollErr)
	require.NotNil(t, result, "async connect did not complete in state %v", connect.State())
	harness.CleanupClose(t, "close control subscription", result.ControlSubscription.Close)
	harness.CleanupClose(t, "close control publication", result.ControlPublication.Close)
	return result
}

// requireControlChannelSessionID asserts each effective control channel of cfg
// holds one session-id. Both channels hold the same value. It returns that value.
func requireControlChannelSessionID(t *testing.T, cfg archive.Config) int32 {
	t.Helper()

	channelSessionID := func(channel string) string {
		t.Helper()

		require.Equal(t, 1, strings.Count(channel, aeron.ChannelSessionIDParam), channel)
		paramIdx := aeron.ChannelParamIndex(channel, aeron.ChannelSessionIDParam, true)
		require.GreaterOrEqual(t, paramIdx, 0, channel)
		return aeron.ChannelParamValue(channel, paramIdx, aeron.ChannelSessionIDParam)
	}
	requestValue := channelSessionID(cfg.EffectiveControlRequestChannel())
	responseValue := channelSessionID(cfg.EffectiveControlResponseChannel())
	require.Equal(t, requestValue, responseValue)
	sessionID, err := strconv.ParseInt(requestValue, 10, 32)
	require.NoError(t, err)
	return int32(sessionID)
}

// connectArchiveClient connects an archive client over the local control
// channel on aeronClient. Cleanup closes the client.
func connectArchiveClient(t *testing.T, aeronClient *aeron.Client) *archive.Client {
	t.Helper()

	return connectArchiveClientOn(
		t,
		aeronClient,
		archive.DefaultLocalControlChannel,
		archive.DefaultLocalControlChannel,
	)
}

// connectArchiveClientOn connects an archive client on the given control
// channels. Cleanup closes the client.
func connectArchiveClientOn(
	t *testing.T,
	aeronClient *aeron.Client,
	requestChannel string,
	responseChannel string,
) *archive.Client {
	t.Helper()

	return connectArchiveClientConfig(t, archive.Config{
		ControlRequestChannel:  requestChannel,
		ControlResponseChannel: responseChannel,
		Aeron:                  aeronClient,
	})
}

// connectArchiveClientConfig connects an archive client on cfg with remote
// control. Cleanup closes the client.
func connectArchiveClientConfig(t testing.TB, cfg archive.Config) *archive.Client {
	t.Helper()

	archiveClient, err := archive.Connect(t.Context(), cfg.WithRemoteControl().Normalize())
	require.NoError(t, err)
	harness.CleanupClose(t, "close archive client", archiveClient.Close)
	return archiveClient
}

// awaitStartRecording starts a recording on streamID and returns the catalog
// id from the START signal. async drives AsyncStartRecording through
// Client.Poll. Otherwise StartRecordingAwaitSignal blocks until START.
func awaitStartRecording(
	t *testing.T,
	aeronClient *aeron.Client,
	hadClientError *atomic.Bool,
	archiveClient *archive.Client,
	streamID int32,
	autoStop bool,
	async bool,
) int64 {
	t.Helper()

	channel := []byte(archiveClientRecordingChannel)
	if !async {
		tracker := archive.NewStartSignalTracker(archiveClient.ControlSessionID())
		recordingID, err := archiveClient.StartRecordingAwaitSignal(
			t.Context(),
			streamID,
			archivewire.SourceLocation_LOCAL,
			autoStop,
			channel,
			tracker,
		)
		require.NoError(t, err)
		require.True(t, tracker.Started())
		return recordingID
	}

	handle := archiveClient.NewAsyncStartRecording(
		streamID,
		archivewire.SourceLocation_LOCAL,
		autoStop,
		channel,
	)
	defer handle.Close()
	pollAsyncRequest(
		t,
		aeronClient,
		hadClientError,
		archiveClient,
		"async start recording",
		handle.TryOffer,
		func() bool {
			done, err := handle.PollSignal()
			require.NoError(t, err)
			return done
		},
	)
	return handle.RecordingID()
}

// pollAsyncRequest offers through tryOffer until accepted, then runs
// Client.Poll and poll until poll reports done. name labels failures.
func pollAsyncRequest(
	t *testing.T,
	aeronClient *aeron.Client,
	hadClientError *atomic.Bool,
	archiveClient *archive.Client,
	name string,
	tryOffer func() (bool, error),
	poll func() bool,
) {
	t.Helper()

	offered := false
	harness.PollClientUntilTransportClean(
		t,
		aeronClient,
		hadClientError,
		harness.ConnectedPollTimeout,
		"client transport error during "+name,
		func() bool {
			if !offered {
				var err error
				offered, err = tryOffer()
				require.NoError(t, err)
				if !offered {
					return false
				}
			}
			_ = archiveClient.Poll()
			return poll()
		},
		func() string {
			return name + " not complete within " +
				harness.ConnectedPollTimeout.String()
		},
	)
}

// requireStopUnknownSubscription stops the recording on streamID through
// AsyncStopRecording and requires ErrorUnknownSubscription.
func requireStopUnknownSubscription(
	t *testing.T,
	aeronClient *aeron.Client,
	hadClientError *atomic.Bool,
	archiveClient *archive.Client,
	streamID int32,
) {
	t.Helper()

	handle := archiveClient.NewAsyncStopRecording(
		streamID,
		[]byte(archiveClientRecordingChannel),
	)
	defer handle.Close()
	var stopErr error
	pollAsyncRequest(
		t,
		aeronClient,
		hadClientError,
		archiveClient,
		"async stop recording",
		handle.TryOffer,
		func() bool {
			var responded bool
			responded, stopErr = handle.PollResponse()
			return responded
		},
	)
	code, ok := archive.ExceptionCode(stopErr)
	require.True(t, ok, "stop error: %v", stopErr)
	require.Equal(t, int32(archive.ErrorUnknownSubscription), code)
}

// awaitListRecording lists recordingID through AsyncListRecording and
// returns its descriptor.
func awaitListRecording(
	t *testing.T,
	aeronClient *aeron.Client,
	hadClientError *atomic.Bool,
	archiveClient *archive.Client,
	recordingID int64,
) codecs.RecordingDescriptorFields {
	t.Helper()

	handle := archiveClient.NewAsyncListRecording(recordingID)
	defer handle.Close()
	var descriptor codecs.RecordingDescriptorFields
	pollAsyncRequest(
		t,
		aeronClient,
		hadClientError,
		archiveClient,
		"async list recording",
		handle.TryOffer,
		func() bool {
			done, got, err := handle.PollDescriptor()
			require.NoError(t, err)
			descriptor = got
			return done
		},
	)
	return descriptor
}

// TestArchiveClient_backgroundTransportRecordingSignal records an IPC
// stream on a background conductor. The test does not call DoWork.
func TestArchiveClient_backgroundTransportRecordingSignal(t *testing.T) {
	t.Parallel()

	const streamID int32 = 4005

	md := harness.Start(t)
	harness.StartArchiveNode(t, harness.ArchiveNodeConfig{
		AeronDir:       md.Dir,
		ControlChannel: harness.ArchiveBackgroundRecordingControl,
	})
	transport := harness.ConnectBackgroundClient(t, md, aeron.Config{})
	require.False(t, transport.HasConductorInvoker())

	connect, err := archive.NewAsyncConnect(t.Context(), archive.Config{
		ControlRequestChannel:  harness.ArchiveBackgroundRecordingControl,
		ControlResponseChannel: "aeron:udp?endpoint=127.0.0.1:0",
		Aeron:                  transport,
	}.WithRemoteControl().Normalize())
	require.NoError(t, err)
	defer func() { _ = connect.Close() }()

	var result *archive.ConnectResult
	var pollErr error
	harness.PollUntilBool(harness.ConnectedPollTimeout, time.Millisecond, func() bool {
		result, pollErr = connect.Poll()
		return result != nil || pollErr != nil
	})
	require.NoError(t, pollErr)
	require.NotNil(t, result, "async connect did not complete in state %v", connect.State())

	session := archive.NewClient(result)
	harness.CleanupClose(t, "close archive session", session.Close)

	// The archive emits START once the recording subscription has an image.
	pub, err := transport.AddPublication(streamID, archiveClientRecordingChannel)
	require.NoError(t, err)
	harness.CleanupClose(t, "close record publication", pub.Close)

	handle := session.NewAsyncStartRecording(
		streamID,
		archivewire.SourceLocation_LOCAL,
		true,
		[]byte(archiveClientRecordingChannel),
	)
	defer handle.Close()

	offered := false
	started := harness.PollUntilBool(harness.ConnectedPollTimeout, time.Millisecond, func() bool {
		if !offered {
			var offerErr error
			offered, offerErr = handle.TryOffer()
			require.NoError(t, offerErr)
		}
		_ = session.Poll()
		done, signalErr := handle.PollSignal()
		require.NoError(t, signalErr)
		return done
	})
	require.True(t, started, "START did not arrive")
	recordingID := handle.RecordingID()

	published := harness.PollUntilBool(harness.ConnectedPollTimeout, time.Millisecond, func() bool {
		if pub.Offer([]byte("recorded")) > 0 {
			return true
		}
		_ = session.Poll()
		return false
	})
	require.True(t, published, "publication did not accept the payload")

	stopped := false
	session.SetRecordingSignalHandler(archive.RecordingSignalHandlerFunc(
		func(event codecs.RecordingSignalEventFields) {
			if event.Signal == archivewire.RecordingSignal_STOP &&
				event.RecordingID == recordingID {
				stopped = true
			}
		},
	))
	require.NoError(t, pub.Close())

	harness.PollUntilBool(harness.ConnectedPollTimeout, time.Millisecond, func() bool {
		_ = session.Poll()
		return stopped
	})
	require.True(t, stopped, "STOP did not arrive")
}
