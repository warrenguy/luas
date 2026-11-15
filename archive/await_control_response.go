package archive

import (
	"context"
	"time"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/archive/codecs"
)

// TryMatchControlResponse reports whether poller already holds a ControlResponse
// matching controlSessionID and correlationID from its most recent poll.
// Reads the last latched response only. Does not poll. Does not call DoWork.
//
// Returns:
//   - (fields, nil, true) on an OK match
//   - (zero, err, true) when the matched response has a non-OK code
//   - (zero, nil, false) when no matching response is captured
//
// Error codes map through MapControlResponse.
func TryMatchControlResponse(
	poller *ControlResponsePoller,
	controlSessionID int64,
	correlationID int64,
) (codecs.ControlResponseFields, error, bool) {
	if poller == nil {
		return codecs.ControlResponseFields{}, nil, false
	}
	fields, ok := poller.Response()
	if !ok {
		return codecs.ControlResponseFields{}, nil, false
	}
	return matchControlResponseFields(fields, controlSessionID, correlationID)
}

// The session runs one outstanding request at a time. A non-matching terminal is
// stale. A latched non-matching terminal keeps the awaited response in the image.
//
// A non-matching terminal can latch in these cases:
//   - The correlation filter accepts any correlation.
//   - The correlation filter accepts a Challenge with the armed correlation id.
//   - The correlation filter does not check the control session id.
func tryMatchControlResponseDiscardStale(
	poller *ControlResponsePoller,
	controlSessionID int64,
	correlationID int64,
) (codecs.ControlResponseFields, error, bool) {
	fields, err, ok := TryMatchControlResponse(poller, controlSessionID, correlationID)
	if !ok && poller != nil && poller.IsPollComplete() {
		poller.ResetCapture()
	}
	return fields, err, ok
}

// AwaitControlResponse loops a termination check and pollFn until
// poller captures the ControlResponse matching controlSessionID and
// correlationID.
//
// Behaviour:
//   - The await calls DoWork on aeronClient only when
//     aeronClient.HasConductorInvoker reports true.
//   - cfg.IdleStrategy paces empty polls.
//   - The await resets cfg.IdleStrategy on every exit path.
//   - Nil cfg.IdleStrategy uses NewDefaultIdleStrategy.
//
// Waits up to Config.ControlResponseTimeout, or until ctx.Deadline() when that
// is earlier.
//
// Returns:
//   - matched fields with a nil error
//   - mapped error for a non-OK response
//   - ControlResponseTimeoutError on timeout
//   - ErrTransportTerminating when aeronClient reports termination
//   - ctx.Err() when the context is done
//
// The function does not check the subscription connection. Client methods do.
func AwaitControlResponse(
	ctx context.Context,
	aeronClient *aeron.Client,
	cfg Config,
	pollFn func() int,
	poller *ControlResponsePoller,
	controlSessionID int64,
	correlationID int64,
) (codecs.ControlResponseFields, error) {
	return awaitControlResponse(
		ctx,
		aeronClient,
		cfg,
		pollFn,
		awaitChecks{},
		poller,
		controlSessionID,
		correlationID,
	)
}

func awaitControlResponse(
	ctx context.Context,
	aeronClient *aeron.Client,
	cfg Config,
	pollFn func() int,
	checks awaitChecks,
	poller *ControlResponsePoller,
	controlSessionID int64,
	correlationID int64,
) (codecs.ControlResponseFields, error) {
	cfg = cfg.Normalize()
	if poller != nil {
		poller.ArmAwait(correlationID)
		defer poller.ArmAwait(0)
	}
	return awaitMatchedControlResponse(
		ctx,
		aeronClient,
		cfg,
		pollFn,
		checks,
		cfg.ControlResponseTimeout,
		correlationID,
		ControlResponseAwaitOperation,
		func() (codecs.ControlResponseFields, error, bool) {
			return tryMatchControlResponseDiscardStale(poller, controlSessionID, correlationID)
		},
	)
}

func awaitMatchedControlResponse(
	ctx context.Context,
	aeronClient *aeron.Client,
	cfg Config,
	pollFn func() int,
	checks awaitChecks,
	responseTimeout time.Duration,
	correlationID int64,
	operation string,
	tryMatch func() (codecs.ControlResponseFields, error, bool),
) (codecs.ControlResponseFields, error) {
	var matched codecs.ControlResponseFields
	var matchErr error
	// A response buffered during the offer phase must be read before the first pollFn:
	// ControlResponsePoller.Poll resets the capture once pollComplete is set, which would
	// otherwise discard it. runControlAwaitLoop matches once before polling.
	err := runControlAwaitLoop(
		ctx,
		aeronClient,
		cfg,
		pollFn,
		checks,
		responseTimeout,
		correlationID,
		operation,
		func() (bool, error) {
			fields, err, ok := tryMatch()
			if !ok {
				return false, nil
			}
			matched = fields
			matchErr = err
			return true, nil
		},
	)
	if err != nil {
		return codecs.ControlResponseFields{}, err
	}
	return matched, matchErr
}

// awaitChecks holds the session checks a blocking await runs each
// iteration. A nil isConnected skips the disconnect check.
type awaitChecks struct {
	transportFault func() error
	isConnected    func() bool
}

// checkTransportFault returns the error from transportFault. A nil
// transportFault tests aeronClient for termination. A done ctx wins over a
// fault.
func checkTransportFault(
	ctx context.Context,
	aeronClient *aeron.Client,
	transportFault func() error,
) error {
	var err error

	if transportFault != nil {
		err = transportFault()
	} else {
		err = transportTerminationSentinel(aeronClient)
	}

	if err != nil {
		return aeron.PreferContextError(ctx, err)
	}
	return nil
}

// runControlAwaitLoop requires a normalized cfg. It does not replace a nil
// IdleStrategy.
//
// Exit order per iteration:
//  1. ctx is done
//  2. the transport fault
//  3. a match
//  4. the disconnect check, after an empty poll that did not match. A
//     transport fault raised during the poll wins.
//
// The loop returns ControlResponseTimeoutError once the deadline passes.
func runControlAwaitLoop(
	ctx context.Context,
	aeronClient *aeron.Client,
	cfg Config,
	pollFn func() int,
	checks awaitChecks,
	responseTimeout time.Duration,
	correlationID int64,
	operation string,
	matchFn func() (done bool, err error),
) error {
	if aeronClient == nil {
		return errNilControlAeronClient
	}
	clock := resolveNanoClock(cfg.AeronConfig.NanoClock)
	idle := cfg.IdleStrategy
	defer idle.Reset()

	startNs := clock()
	deadlineNs, bound := aeron.RetryDeadlineNs(ctx, startNs, responseTimeout)
	if done, err := matchFn(); done {
		return err
	}
	for clock() < deadlineNs {
		if err := ctx.Err(); err != nil {
			return err
		}

		if aeronClient.HasConductorInvoker() {
			aeronClient.DoWork()
		}

		if err := checkTransportFault(ctx, aeronClient, checks.transportFault); err != nil {
			return err
		}

		work := pollFn()
		if done, err := matchFn(); done {
			return err
		}

		isConnected := checks.isConnected
		if work == 0 && isConnected != nil && !isConnected() {
			// A fault closes the subscription. The fault is the cause.
			if err := checkTransportFault(ctx, aeronClient, checks.transportFault); err != nil {
				return err
			}
			return aeron.PreferContextError(ctx, &ControlResponseNotConnectedError{
				Operation:     operation,
				CorrelationID: correlationID,
			})
		}

		idle.Idle(work)
	}
	return aeron.PreferContextError(ctx, &ControlResponseTimeoutError{
		Operation:     operation,
		CorrelationID: correlationID,
		Timeout:       bound,
	})
}

// offerAndAwaitControlRequest offers a control request until the publication
// accepts it, then awaits the matching ControlResponse.
//
// ResetAwait arms the poller before offer so offer-retry polls filter by correlationID.
// The offer retry uses ControlOfferRetryTimeout. Once the offer succeeds, the response
// await starts a fresh ControlResponseTimeout window so a slow offer cannot starve the
// response wait.
func offerAndAwaitControlRequest(
	ctx context.Context,
	aeronClient *aeron.Client,
	cfg Config,
	pollFn func() int,
	checks awaitChecks,
	poller *ControlResponsePoller,
	offerFn func() (int64, bool),
	controlSessionID int64,
	correlationID int64,
) (codecs.ControlResponseFields, error) {
	if poller != nil {
		poller.ResetAwait(correlationID)
		defer poller.ArmAwait(0)
	}
	if err := offerControlRequest(
		ctx, aeronClient, cfg, pollFn, checks.transportFault, offerFn,
	); err != nil {
		return codecs.ControlResponseFields{}, err
	}

	return awaitControlResponse(
		ctx,
		aeronClient,
		cfg,
		pollFn,
		checks,
		poller,
		controlSessionID,
		correlationID,
	)
}

func offerControlRequest(
	ctx context.Context,
	aeronClient *aeron.Client,
	cfg Config,
	pollFn func() int,
	transportFault func() error,
	offerFn func() (int64, bool),
) error {
	_, err := controlOfferUntilPositive(
		ctx,
		aeronClient,
		cfg,
		ControlOfferConfig{
			PollFn:         pollFn,
			transportFault: transportFault,
		},
		func() int64 {
			// Pass the raw position through: a retriable negative code (BackPressured,
			// AdminAction) must reach controlOfferUntilPositive to trigger a retry.
			// Collapsing !ok to 0 would make retriable failures look terminal.
			position, _ := offerFn()
			return position
		},
	)
	return err
}
