package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/idlestrategy"
)

var (
	errNilIngressAeronClient = errors.New("cluster client: nil aeron client")
	errNilIngressSession     = errors.New("cluster client: nil session")
	errNilIngressOfferFn     = errors.New("cluster client: nil offer function")
)

// IngressTerminalError reports a non-retriable ingress offer result from a retry loop.
type IngressTerminalError struct {
	Status int64
}

func (err *IngressTerminalError) Error() string {
	return fmt.Sprintf(
		"cluster client: ingress offer failed: status=%s",
		aeron.PositionCodeName(err.Status),
	)
}

// IngressTimeoutError reports ingress retry deadline expiry after retriable codes.
type IngressTimeoutError struct {
	Status  int64
	Timeout time.Duration
}

func (err *IngressTimeoutError) Error() string {
	return fmt.Sprintf(
		"cluster client: ingress offer timed out within %s: status=%s",
		err.Timeout,
		aeron.PositionCodeName(err.Status),
	)
}

// IngressOfferConfig holds retry pacing for IngressOfferUntilPositive.
type IngressOfferConfig struct {
	// IdleStrategy paces BackPressured retries. Nil uses session.Config().IdleStrategy
	// when set; if that is also nil, NewDefaultIdleStrategy(). Requires a non-nil
	// session. Use one instance across connect, SpinOnce, and ingress retry.
	// idle.Reset() runs on all exit paths so a shared strategy is not left in PARKING.
	// For fixed-sleep pacing, pass idlestrategy.NewSleeping(d).
	IdleStrategy idlestrategy.IdleStrategy

	// NanoClock supplies nanosecond timestamps for deadline checks.
	// Nil uses session.nanoClock(), which defaults to wall-clock
	// time.Now().UnixNano(). The clock must advance for IngressRetryTimeout to
	// bound retries; a frozen clock causes unbounded retry.
	NanoClock func() int64
}

func ingressOfferIdleStrategy(
	cfg IngressOfferConfig,
	session *AeronCluster,
) idlestrategy.IdleStrategy {
	if cfg.IdleStrategy != nil {
		return cfg.IdleStrategy
	}
	if idle := session.cfg.IdleStrategy; idle != nil {
		return idle
	}
	return NewDefaultIdleStrategy()
}

func ingressOfferUntilPositive(
	ctx context.Context,
	session *AeronCluster,
	cfg IngressOfferConfig,
	offerFn func() int64,
) (int64, error) {
	switch {
	case session == nil:
		return 0, errNilIngressSession
	case offerFn == nil:
		return 0, errNilIngressOfferFn
	}
	aeronClient := session.Aeron()
	if aeronClient == nil {
		return 0, errNilIngressAeronClient
	}
	idle := ingressOfferIdleStrategy(cfg, session)
	defer idle.Reset()
	clock := cfg.NanoClock
	if clock == nil {
		clock = session.nanoClock
	}
	startNs := clock()
	deadlineNs, bound := aeron.RetryDeadlineNs(ctx, startNs, session.cfg.IngressRetryTimeout)
	doTransportWork := func() {
		if aeronClient.HasConductorInvoker() {
			aeronClient.DoWork()
			recordIngressRetryDoWork()
		}
	}
	var pos int64
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		doTransportWork()
		pos = offerFn()
		if pos > 0 {
			return pos, nil
		}
		if !aeron.IsIngressRetriable(pos) {
			return 0, aeron.PreferContextError(ctx, &IngressTerminalError{Status: pos})
		}
		session.PollEgress()
		if pos == aeron.BackPressured {
			doTransportWork()
			idle.Idle(0)
		}
		if clock() >= deadlineNs {
			return 0, aeron.PreferContextError(ctx, &IngressTimeoutError{Status: pos, Timeout: bound})
		}
	}
}

// IngressOfferUntilPositive retries offerFn while aeron.IsIngressRetriable
// accepts its result. It stops on success, timeout, or a terminal code.
//
// Behaviour:
//
//   - Before each offerFn call, calls session.Aeron().DoWork() when the client
//     has a conductor invoker.
//   - After a retriable code, calls PollEgress. On BackPressured, calls DoWork
//     again under the same condition, then calls cfg.IdleStrategy.Idle(0).
//     AdminAction retries at once without pacing, as Publication offer does.
//   - Stops on any non-retriable code. This includes a session-guard
//     NotConnected after PollEgress advances recovery or egress teardown.
//   - offerFn must use app-guarded APIs (Offer, SendKeepAlive,
//     SendSnapshotRequest). Do not wrap recovery-bypass paths such as
//     offerSessionKeepAlive.
//
// Returns:
//
//   - The stream position (>0) and nil on success.
//   - 0 and ctx.Err() when ctx is done before success.
//   - 0 and IngressTerminalError or IngressTimeoutError while ctx is active.
//   - 0 and a nil-argument error.
//
// Use single-shot Offer or SendKeepAlive when the caller needs the raw Aeron
// status code.
//
// The retry bound is Config.IngressRetryTimeout, or ctx.Deadline() when that
// is earlier.
//
// Zero-alloc tests do not cover this method. Use it for idle keep-alive and
// integration-style sends, not per-message hot paths.
func (ac *AeronCluster) IngressOfferUntilPositive(
	ctx context.Context,
	cfg IngressOfferConfig,
	offerFn func() int64,
) (int64, error) {
	if ac == nil {
		return 0, errNilIngressSession
	}
	return ingressOfferUntilPositive(ctx, ac, cfg, offerFn)
}

// SendKeepAliveUntilPositive retries SendKeepAlive via IngressOfferUntilPositive.
// Inherits that method's contract. Intended for idle keep-alive duty cycles, not
// per-message hot paths.
func (ac *AeronCluster) SendKeepAliveUntilPositive(
	ctx context.Context,
	cfg IngressOfferConfig,
) (int64, error) {
	return ac.IngressOfferUntilPositive(ctx, cfg, ac.SendKeepAlive)
}
