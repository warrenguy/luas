//go:build integration

package integration_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/aeron/logbuffer"
	"github.com/warrenguy/luas/test/integration/harness"
)

func TestAeronHelpers_unfragmentedRoundtrip(t *testing.T) {
	md := harness.SharedParallel(t)
	client, hadClientError := harness.ConnectIPCClient(t, md)

	ctx := t.Context()
	payload := []byte("aeron-helpers-smoke")

	pub, sub := harness.ConnectIPCPubSubClients(t, client, client, harness.AeronHelpersSmokeStreamID)

	pubPosBefore := pub.Position()
	wantAdvance := harness.AlignedOfferAdvance(len(payload), 1)

	position, err := pub.OfferWithRetry(ctx, payload)
	require.NoError(t, err)
	require.Positive(t, position)

	got, err := sub.PollOneMessage(ctx)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	harness.AssertPubSubPositionsAdvanced(t, pub, sub, pubPosBefore, wantAdvance)
	finishPubSubIPCTest(t, md, hadClientError)
}

func TestAeronHelpers_fragmentedRoundtrip(t *testing.T) {
	md := harness.SharedParallel(t)
	client, hadClientError := harness.ConnectIPCClient(t, md)

	ctx := t.Context()

	pub, sub := harness.ConnectIPCPubSubClients(
		t,
		client,
		client,
		harness.AeronHelpersFragmentedSmokeStreamID,
	)

	maxPayload := pub.MaxPayloadLength()
	payload := harness.FragmentedRoundtripPayload(maxPayload, harness.FragmentedRoundtripExtraBytes)
	buf := make([]byte, len(payload))

	pubPosBefore := pub.Position()
	wantAdvance := int64(logbuffer.ComputeFragmentedFrameLength(len(payload), maxPayload))

	position, err := pub.OfferWithRetry(ctx, payload)
	require.NoError(t, err)
	require.Positive(t, position)

	n, err := sub.PollReassembled(ctx, buf)
	require.NoError(t, err)
	require.Equal(t, payload, buf[:n])

	harness.AssertPubSubPositionsAdvanced(t, pub, sub, pubPosBefore, wantAdvance)
	finishPubSubIPCTest(t, md, hadClientError)
}

// TestAeronHelpers_backgroundConductor runs the helpers with a background conductor.
func TestAeronHelpers_backgroundConductor(t *testing.T) {
	md := harness.SharedParallel(t)

	tests := []struct {
		name    string
		channel func(t testing.TB) string
	}{
		{
			name:    "ipc",
			channel: func(testing.TB) string { return harness.IPCChannel },
		},
		{
			name: "udp",
			channel: func(t testing.TB) string {
				return "aeron:udp?endpoint=" + harness.LoopbackUDPEndpoint(harness.FreeUDPPort(t))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			channel := tt.channel(t)
			client := harness.ConnectBackgroundClient(t, md, aeron.Config{
				ErrorHandler: func(err error) {
					t.Errorf("client error: %v", err)
				},
			})
			require.False(t, client.HasConductorInvoker())

			pub, err := client.AddPublication(harness.AeronHelpersBackgroundStreamID, channel)
			require.NoError(t, err)
			sub, err := client.AddSubscription(harness.AeronHelpersBackgroundStreamID, channel)
			require.NoError(t, err)
			requireHelperRoundtrip(t, pub, sub, []byte("shared"))

			exclusivePub, err := client.AddExclusivePublication(
				harness.AeronHelpersBackgroundExclusiveStreamID,
				channel,
			)
			require.NoError(t, err)
			exclusiveSub, err := client.AddSubscription(
				harness.AeronHelpersBackgroundExclusiveStreamID,
				channel,
			)
			require.NoError(t, err)
			requireHelperRoundtrip(t, exclusivePub, exclusiveSub, []byte("exclusive"))
		})
	}
}

// helperPublication is a Publication or an ExclusivePublication.
type helperPublication interface {
	WaitUntilConnected(ctx context.Context) error
	OfferWithRetry(ctx context.Context, payload []byte) (int64, error)
}

// requireHelperRoundtrip sends payload from pub to sub. It never calls DoWork.
func requireHelperRoundtrip(
	t testing.TB,
	pub helperPublication,
	sub *aeron.Subscription,
	payload []byte,
) {
	t.Helper()

	ctx := t.Context()
	require.NoError(t, pub.WaitUntilConnected(ctx))
	require.NoError(t, sub.WaitUntilImageCount(ctx, 1))

	_, err := pub.OfferWithRetry(ctx, payload)
	require.NoError(t, err)

	got, err := sub.PollOneMessage(ctx)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}
