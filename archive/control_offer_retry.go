package archive

import (
	"context"
	"errors"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/idlestrategy"
)

var (
	errNilSession            = errors.New("archive: nil session")
	errNilControlAeronClient = errors.New("archive: nil aeron client")
	errNilControlOfferFn     = errors.New("archive: nil offer function")
)

// ControlOfferConfig holds retry pacing for archive control offer retries.
type ControlOfferConfig struct {
	// IdleStrategy paces retriable offers. Nil uses Config.IdleStrategy
	// when set; if that is also nil, NewDefaultIdleStrategy().
	IdleStrategy idlestrategy.IdleStrategy

	// NanoClock supplies nanosecond timestamps for deadline checks.
	// Nil uses Config.AeronConfig.NanoClock when set, else wall-clock
	// time.Now().UnixNano(). The clock must advance for ControlOfferRetryTimeout
	// to bound retries. With a frozen clock, Config.MessageRetryAttempts ends
	// the retry.
	NanoClock func() int64

	// PollFn optionally polls the control response subscription during retriable
	// offer failure (for example poller.Poll). Nil skips response polling.
	PollFn func() int

	// transportFault is the session fault check. Nil tests the transport client
	// for termination.
	transportFault func() error
}

// controlOfferUntilPositive implements [Client.ControlOfferUntilPositive] for
// in-package callers without a session wrapper.
//
// Config.MessageRetryAttempts caps the offers. Zero uses
// DefaultMessageRetryAttempts. A negative value allows one offer.
func controlOfferUntilPositive(
	ctx context.Context,
	aeronClient *aeron.Client,
	cfg Config,
	offerCfg ControlOfferConfig,
	offerFn func() int64,
) (int64, error) {
	switch {
	case aeronClient == nil:
		return 0, errNilControlAeronClient
	case offerFn == nil:
		return 0, errNilControlOfferFn
	}
	timeout := cfg.ControlOfferRetryTimeout
	if timeout == 0 {
		timeout = DefaultControlOfferRetryTimeout
	}
	attemptLimit := cfg.MessageRetryAttempts
	if attemptLimit == 0 {
		attemptLimit = DefaultMessageRetryAttempts
	}
	idle := offerCfg.IdleStrategy
	if idle == nil {
		idle = cfg.IdleStrategy
	}
	if idle == nil {
		idle = NewDefaultIdleStrategy()
	}
	clock := offerCfg.NanoClock
	if clock == nil {
		clock = cfg.AeronConfig.NanoClock
	}
	clock = resolveNanoClock(clock)
	startNs := clock()
	deadlineNs, bound := aeron.RetryDeadlineNs(ctx, startNs, timeout)
	defer idle.Reset()
	doTransportWork := func() error {
		if aeronClient.HasConductorInvoker() {
			aeronClient.DoWork()
			recordControlOfferRetryDoWork(aeronClient)
		}

		return checkTransportFault(ctx, aeronClient, offerCfg.transportFault)
	}
	var pos int64
	attempts := 0
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		if err := doTransportWork(); err != nil {
			return 0, err
		}

		pos = offerFn()
		if pos > 0 {
			return pos, nil
		}
		if !aeron.IsIngressRetriable(pos) {
			return 0, aeron.PreferContextError(ctx, &ControlTerminalError{Status: pos})
		}

		// The cap check precedes the deadline check, so the cap wins when both expire.
		attempts++
		if attempts >= attemptLimit {
			return 0, aeron.PreferContextError(ctx, &ControlOfferAttemptsError{
				Status:   pos,
				Attempts: attempts,
			})
		}
		if offerCfg.PollFn != nil {
			offerCfg.PollFn()
		}
		if pos == aeron.BackPressured {
			if err := doTransportWork(); err != nil {
				return 0, err
			}
		}

		idle.Idle(0)
		if clock() >= deadlineNs {
			return 0, aeron.PreferContextError(ctx, &ControlOfferTimeoutError{Status: pos, Timeout: bound})
		}
	}
}

// ControlOfferUntilPositive retries offerFn on aeron.IsIngressRetriable until
// success, timeout, the attempt cap, a terminal publication code, or a
// transport fault.
//
// Calls the session's Aeron client DoWork() before each offerFn call when
// that client has a conductor invoker.
// Calls cfg.PollFn after a retriable code below the attempt cap.
//
// Returns:
//
//   - Success: stream position (>0), nil error.
//   - Nil session, nil offerFn, or nil client (including after Close on an owned
//     client): 0 and a nil-argument error, before any offer.
//   - Terminal publication code: 0, ControlTerminalError.
//   - [Config.MessageRetryAttempts] offers from the connected session: 0,
//     ControlOfferAttemptsError.
//   - [Config.ControlOfferRetryTimeout] elapsed: 0, ControlOfferTimeoutError.
//   - Transport fault: 0 and the stored cause on an owned client, or
//     ErrTransportTerminating on a caller-supplied client.
//   - Done ctx, including an earlier ctx.Deadline(): 0, ctx.Err(). Wins over
//     every error above except a nil-argument error.
//
// Cap and deadline expired on one attempt: ControlOfferAttemptsError wins.
//
// Concurrency: call from the goroutine that runs [Client.Poll]. Not safe from
// other goroutines.
//
// Polling the response subscription during offer retry unblocks publication
// backpressure when the archive replies on the response stream.
//
// Example:
//
//	pos, err := archive.ControlOfferUntilPositive(ctx, ControlOfferConfig{
//	    PollFn: archive.Poll, // operation-specific
//	}, offerFn)
func (client *Client) ControlOfferUntilPositive(
	ctx context.Context,
	cfg ControlOfferConfig,
	offerFn func() int64,
) (int64, error) {
	if client == nil {
		return 0, errNilSession
	}
	cfg.transportFault = client.transportFaultError
	return controlOfferUntilPositive(
		ctx,
		client.aeronClient,
		client.cfg,
		cfg,
		offerFn,
	)
}
