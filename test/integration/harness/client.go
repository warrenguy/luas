//go:build integration

package harness

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
)

const (
	// IPCChannel is the default channel for live-driver integration tests.
	IPCChannel = "aeron:ipc"

	// ConnectedPollTimeout matches aeron.DefaultPollTimeout.
	ConnectedPollTimeout = aeron.DefaultPollTimeout
)

// ConnectClient connects to md and registers client.Close on t.Cleanup.
//
// ConnectClient sets UseConductorInvoker, so the caller runs the conductor.
func ConnectClient(t testing.TB, md *MediaDriver, aeronCfg aeron.Config) *aeron.Client {
	t.Helper()

	aeronCfg.UseConductorInvoker = true
	return connectOwnedClient(t, md, aeronCfg)
}

// ConnectBackgroundClient connects to md and registers client.Close on t.Cleanup.
func ConnectBackgroundClient(t testing.TB, md *MediaDriver, aeronCfg aeron.Config) *aeron.Client {
	t.Helper()

	aeronCfg.UseConductorInvoker = false
	return connectOwnedClient(t, md, aeronCfg)
}

func connectOwnedClient(t testing.TB, md *MediaDriver, aeronCfg aeron.Config) *aeron.Client {
	t.Helper()

	if aeronCfg.AeronDir == "" {
		aeronCfg.AeronDir = md.Dir
	}
	client, err := aeron.Connect(t.Context(), aeronCfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
	})
	return client
}

// ConnectIPCClient connects to md and records client errors in the returned flag.
func ConnectIPCClient(t testing.TB, md *MediaDriver) (*aeron.Client, *atomic.Bool) {
	t.Helper()

	return ConnectIPCClientConfig(t, md, aeron.Config{})
}

// ConnectIPCClientConfig connects with config and records client errors in the returned flag.
func ConnectIPCClientConfig(
	t testing.TB,
	md *MediaDriver,
	aeronCfg aeron.Config,
) (*aeron.Client, *atomic.Bool) {
	t.Helper()

	var hadClientError atomic.Bool
	callerHandler := aeronCfg.ErrorHandler
	aeronCfg.ErrorHandler = func(err error) {
		hadClientError.Store(true)
		if callerHandler != nil {
			callerHandler(err)
		} else {
			t.Errorf("client error: %v", err)
		}
	}

	return ConnectClient(t, md, aeronCfg), &hadClientError
}

// CleanupClose registers closeFn on t.Cleanup; records errors without masking test failure.
func CleanupClose(t testing.TB, label string, closeFn func() error) {
	t.Helper()
	t.Cleanup(func() {
		if err := closeFn(); err != nil {
			t.Errorf("%s: %v", label, err)
		}
	})
}

// Loop exit: ok after work; only context.Canceled short-circuits via failPollTimeout;
// natural deadline emits harness-local "%s within %s" (not DeadlineExceeded routing).
// Work and ok always run at least once, even on an already-expired bound.
func pollUntilDynamicCtx(
	t testing.TB,
	ctx context.Context,
	bound time.Duration,
	work func(),
	ok func() bool,
	failMsg func() string,
) {
	t.Helper()

	now := time.Now()
	effective := aeron.EffectiveRetryBound(ctx, bound, now)
	deadline := now.Add(effective)

	for {
		work()
		if ok() {
			return
		}
		if errors.Is(ctx.Err(), context.Canceled) {
			failPollTimeout(t, ctx.Err())
			return
		}
		if !time.Now().Before(deadline) {
			t.Errorf("%s within %s", failMsg(), effective)
			t.FailNow()
			return
		}
		time.Sleep(aeron.DefaultIdleSleepDuration)
	}
}

func pollClientUntilDynamicCtx(
	t testing.TB,
	ctx context.Context,
	client *aeron.Client,
	bound time.Duration,
	ok func() bool,
	failMsg func() string,
) {
	t.Helper()

	pollUntilDynamicCtx(t, ctx, bound, func() { client.DoWork() }, ok, failMsg)
}

// PollClientUntilTransportClean polls client.DoWork until ok returns true, failing
// immediately on transport errors recorded in hadClientError.
func PollClientUntilTransportClean(
	t testing.TB,
	client *aeron.Client,
	hadClientError *atomic.Bool,
	timeout time.Duration,
	transportErrLabel string,
	ok func() bool,
	timeoutMsg func() string,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		require.False(t, hadClientError.Load(), transportErrLabel)
		client.DoWork()
		if ok() {
			return
		}
		time.Sleep(aeron.DefaultIdleSleepDuration)
	}
	require.FailNow(t, timeoutMsg())
}

// PollClientDuring runs fn on a goroutine and polls client.DoWork until fn returns or
// timeout elapses. Fails immediately on transport errors recorded in hadClientError.
func PollClientDuring(
	t testing.TB,
	client *aeron.Client,
	hadClientError *atomic.Bool,
	timeout time.Duration,
	transportErrLabel string,
	timeoutMsg func() string,
	fn func() error,
) {
	t.Helper()

	done := make(chan error, 1)
	go func() {
		done <- fn()
	}()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if hadClientError.Load() {
			t.Errorf("%s", transportErrLabel)
			t.FailNow()
			return
		}
		client.DoWork()
		select {
		case fnErr := <-done:
			if fnErr != nil {
				if failDelegatedContext(t, fnErr, "poll") {
					return
				}
				t.Errorf("%v", fnErr)
				t.FailNow()
			}
			return
		default:
		}
		time.Sleep(aeron.DefaultIdleSleepDuration)
	}
	t.Errorf("%s", timeoutMsg())
	t.FailNow()
}

// WaitForSubscriptionConnected polls DoWork until the subscription reports connected.
func WaitForSubscriptionConnected(
	t testing.TB,
	client *aeron.Client,
	sub *aeron.Subscription,
) {
	t.Helper()

	waitForSubscriptionConnectedCtx(t, t.Context(), client, sub, ConnectedPollTimeout)
}

func waitForSubscriptionConnectedCtx(
	t testing.TB,
	ctx context.Context,
	client *aeron.Client,
	sub *aeron.Subscription,
	bound time.Duration,
) {
	t.Helper()

	pollClientUntilDynamicCtx(
		t,
		ctx,
		client,
		bound,
		sub.IsConnected,
		func() string {
			return fmt.Sprintf("subscription %d never connected", sub.RegistrationID())
		},
	)
}

// channelStatusReporter is the method set that WaitForChannelStatusActive
// polls.
type channelStatusReporter interface {
	ChannelStatus() (int64, error)
}

// WaitForChannelStatusActive polls DoWork until reporter holds
// ChannelEndpointStatusActive.
func WaitForChannelStatusActive(
	t testing.TB,
	client *aeron.Client,
	reporter channelStatusReporter,
) {
	t.Helper()

	pollClientUntilDynamicCtx(
		t,
		t.Context(),
		client,
		ConnectedPollTimeout,
		func() bool {
			status, err := reporter.ChannelStatus()
			require.NoError(t, err)
			return status == aeron.ChannelEndpointStatusActive
		},
		func() string { return "channel status never active" },
	)
}

// WaitForIPCLinkConnected polls subscription image attach, then publication metadata.
// Live IPC: image arrives via ON_AVAILABLE_IMAGE before pub log is_connected flips.
func WaitForIPCLinkConnected(
	t testing.TB,
	client *aeron.Client,
	pub boundPublication,
	sub *aeron.Subscription,
) {
	t.Helper()

	waitForIPCLinkConnectedCtx(t, t.Context(), client, pub, sub)
}

func waitForIPCLinkConnectedCtx(
	t testing.TB,
	ctx context.Context,
	client *aeron.Client,
	pub boundPublication,
	sub *aeron.Subscription,
) {
	t.Helper()

	// Shared linkCtx caps total wait across subscription + publication phases.
	linkCtx, cancel := context.WithTimeout(ctx, ConnectedPollTimeout)
	defer cancel()

	waitForSubscriptionConnectedCtx(t, linkCtx, client, sub, ConnectedPollTimeout)
	if failedCaptureTB(t) {
		return
	}
	waitUntilConnectedCtx(t, linkCtx, pub)
}

func requireLookupMissCtx[T any](
	t testing.TB,
	ctx context.Context,
	client *aeron.Client,
	bound time.Duration,
	regID int64,
	resourceKind string,
	lookup func(int64) (T, bool),
) {
	t.Helper()

	pollClientUntilDynamicCtx(
		t,
		ctx,
		client,
		bound,
		func() bool {
			_, ok := lookup(regID)
			return !ok
		},
		func() string {
			return fmt.Sprintf("%s %d still registered after close", resourceKind, regID)
		},
	)
}

// RequirePublicationLookupMiss polls DoWork until Publication(regID) misses.
func RequirePublicationLookupMiss(
	t testing.TB,
	client *aeron.Client,
	regID int64,
) {
	t.Helper()

	requireLookupMissCtx(
		t,
		t.Context(),
		client,
		CommandPollDeadline,
		regID,
		"publication",
		client.Publication,
	)
}

// RequireExclusivePublicationLookupMiss polls DoWork until ExclusivePublication(regID) misses.
func RequireExclusivePublicationLookupMiss(
	t testing.TB,
	client *aeron.Client,
	regID int64,
) {
	t.Helper()

	requireLookupMissCtx(
		t,
		t.Context(),
		client,
		CommandPollDeadline,
		regID,
		"exclusive publication",
		client.ExclusivePublication,
	)
}

// RequireSubscriptionLookupMiss polls DoWork until Subscription(regID) misses.
func RequireSubscriptionLookupMiss(
	t testing.TB,
	client *aeron.Client,
	regID int64,
) {
	t.Helper()

	requireLookupMissCtx(
		t,
		t.Context(),
		client,
		CommandPollDeadline,
		regID,
		"subscription",
		client.Subscription,
	)
}
