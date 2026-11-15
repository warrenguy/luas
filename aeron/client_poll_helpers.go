package aeron

import (
	"context"
	"time"
)

// RunDuringClientWork runs fn on a goroutine and waits until fn returns, ctx
// is canceled, or [Config.PollTimeout] elapses. It calls DoWork while it
// waits in invoker mode.
//
// Timeout or cancel returns promptly without waiting for fn to exit.
//
// A call from a handler returns [ErrReentrantCall] before fn starts.
//
// Does not install an ErrorHandler; wrap [Config.ErrorHandler] if transport
// errors must fail the wait.
func (client *Client) RunDuringClientWork(
	ctx context.Context,
	fn func() error,
) error {
	if err := client.checkReentrant(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() {
		done <- fn()
	}()

	var fnErr error
	finished := func() bool {
		select {
		case fnErr = <-done:
			return true
		default:
			return false
		}
	}
	err := client.pollUntil(ctx, finished, func(timeout time.Duration) error {
		return &RunDuringClientWorkTimeoutError{Timeout: timeout}
	})
	if err != nil {
		return err
	}
	return fnErr
}

// pollUntil drives pollWorkLoop under [Config.PollTimeout] until ready is true.
//
// Expiry maps to [PreferContextError] over timeoutErr(effective).
func (client *Client) pollUntil(
	ctx context.Context,
	ready func() bool,
	timeoutErr func(timeout time.Duration) error,
) error {
	if err := client.checkReentrant(); err != nil {
		return err
	}
	deadline, effective, err := retryDeadline(ctx, client.pollTimeout)
	if err != nil {
		return err
	}
	ok, err := pollWorkLoop(ctx, client, deadline, ready)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	return PreferContextError(ctx, timeoutErr(effective))
}
