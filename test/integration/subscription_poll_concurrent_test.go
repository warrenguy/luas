//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/atomicbuffer"
	"github.com/warrenguy/luas/test/integration/harness"
)

// TestSubscriptionPollBesideDoWorkLoop polls an IPC subscription while
// another goroutine runs DoWork.
func TestSubscriptionPollBesideDoWorkLoop(t *testing.T) {
	md := harness.Start(t)

	const (
		streamID int32 = 1001
		cycles   int   = 3
	)

	var arrivals atomic.Int32
	var departures atomic.Int32
	subClient, subErr := harness.ConnectIPCClientConfig(t, md, aeron.Config{
		UseConductorInvoker: new(true),
		AvailableImageHandler: func(*aeron.Image) {
			arrivals.Add(1)
		},
		UnavailableImageHandler: func(*aeron.Image) {
			departures.Add(1)
		},
	})

	pubClient, pubErr := harness.ConnectIPCClientConfig(t, md, aeron.Config{
		UseConductorInvoker:    new(true),
		ResourceLingerDuration: new(time.Duration(0)),
	})

	var workWG sync.WaitGroup
	workWG.Go(func() {
		for t.Context().Err() == nil {
			if subClient.DoWork() == 0 {
				time.Sleep(aeron.DefaultIdleSleepDuration)
			}
		}
	})
	t.Cleanup(workWG.Wait)

	sub, err := subClient.AddSubscription(streamID, harness.IPCChannel)
	require.NoError(t, err)

	payloads := harness.PayloadsWithPrefix("p", cycles, 2)
	var log subscriptionPollLog

	cycleCtx, cycleCancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cycleCancel()

	pubDone := make(chan error, 1)
	go func() {
		pubDone <- offerAndClosePublicationCycles(
			t,
			cycleCtx,
			pubClient,
			streamID,
			payloads,
			&arrivals,
			&departures,
			&log,
		)
	}()

	var pubResult error
poll:
	for {
		sub.Poll(func(
			buffer atomicbuffer.AtomicBuffer,
			offset int,
			length int,
			_ *aeron.Header,
		) {
			log.append(buffer.Bytes()[offset : offset+length])
		}, 8)

		select {
		case pubResult = <-pubDone:
			break poll
		default:
		}
	}

	require.NoError(t, pubResult)
	require.Equal(t, payloads, log.snapshot())
	require.Equal(t, int32(cycles), arrivals.Load())
	require.Equal(t, int32(cycles), departures.Load())
	require.False(t, subErr.Load())
	require.False(t, pubErr.Load())
	md.AssertProcessAlive(t)
}

type subscriptionPollLog struct {
	mu  sync.Mutex
	got [][]byte
}

func (log *subscriptionPollLog) append(payload []byte) {
	log.mu.Lock()
	log.got = append(log.got, bytes.Clone(payload))
	log.mu.Unlock()
}

func (log *subscriptionPollLog) has(payload []byte) bool {
	log.mu.Lock()
	defer log.mu.Unlock()

	return slices.ContainsFunc(log.got, func(item []byte) bool {
		return bytes.Equal(item, payload)
	})
}

func (log *subscriptionPollLog) snapshot() [][]byte {
	log.mu.Lock()
	defer log.mu.Unlock()

	return slices.Clone(log.got)
}

func offerAndClosePublicationCycles(
	t testing.TB,
	ctx context.Context,
	pubClient *aeron.Client,
	streamID int32,
	payloads [][]byte,
	arrivals *atomic.Int32,
	departures *atomic.Int32,
	log *subscriptionPollLog,
) error {
	t.Helper()

	for index, payload := range payloads {
		pub, err := pubClient.AddPublication(streamID, harness.IPCChannel)
		if err != nil {
			return err
		}

		want := int32(index + 1)
		arrived := func() bool { return arrivals.Load() >= want }
		if err := pumpUntil(t, ctx, pubClient, arrived); err != nil {
			return fmt.Errorf("available image %d: %w", want, err)
		}

		if _, err := pub.OfferWithRetry(ctx, payload); err != nil {
			return fmt.Errorf("offer %d: %w", want, err)
		}

		received := func() bool { return log.has(payload) }
		if err := pumpUntil(t, ctx, pubClient, received); err != nil {
			return fmt.Errorf("payload %d: %w", want, err)
		}

		if err := pub.Close(); err != nil {
			return err
		}

		departed := func() bool { return departures.Load() >= want }
		if err := pumpUntil(t, ctx, pubClient, departed); err != nil {
			return fmt.Errorf("unavailable image %d: %w", want, err)
		}
	}

	return nil
}

func pumpUntil(
	t testing.TB,
	ctx context.Context,
	client *aeron.Client,
	ok func() bool,
) error {
	t.Helper()

	for !ok() {
		if err := ctx.Err(); err != nil {
			return err
		}

		client.DoWork()
		time.Sleep(time.Millisecond)
	}

	return nil
}
