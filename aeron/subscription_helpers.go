package aeron

import (
	"context"
	"time"
)

// WaitUntilImageCount waits until at least want active images exist, or
// [Config.PollTimeout] elapses.
//
// Calls client DoWork between checks in invoker mode.
//
// Non-positive want returns [ErrInvalidWant].
//
// Returns [ErrClientNotBound] when unbound.
func (subscription *Subscription) WaitUntilImageCount(ctx context.Context, want int) error {
	if want <= 0 {
		return ErrInvalidWant
	}
	if subscription.client == nil {
		return ErrClientNotBound
	}

	ready := func() bool { return SubscriptionActiveImageCountAtLeast(subscription, want) }
	return subscription.client.pollUntil(ctx, ready, func(timeout time.Duration) error {
		active, detail := SubscriptionImageSummary(subscription)
		return &WaitUntilImageCountTimeoutError{
			RegistrationID: subscription.RegistrationID(),
			StreamID:       subscription.StreamID(),
			Want:           want,
			Have:           active,
			Timeout:        timeout,
			Detail:         detail,
		}
	})
}

// PollOneMessage receives one unfragmented payload (no [FragmentAssembler]).
//
// First fragment of the first Poll tick with work (fragmentLimit 1). Clones
// non-empty payloads; zero-length returns nil ([FragmentHandlerBytes]).
//
// Does not fail fast on zero images; wait with [Subscription.WaitUntilImageCount]
// first. Fragmented payloads need [Subscription.PollReassembled].
//
// Waits up to [Config.PollTimeout]. Calls client DoWork between polls in
// invoker mode.
//
// Returns [ErrClientNotBound] when unbound.
func (subscription *Subscription) PollOneMessage(ctx context.Context) ([]byte, error) {
	if subscription.client == nil {
		return nil, ErrClientNotBound
	}

	var (
		msg      []byte
		received bool
	)
	handler := AdaptFragmentHandlerBytesRetain(func(payload []byte, _ *Header) {
		if received {
			return
		}
		msg = payload
		received = true
	})
	ready := func() bool { return subscription.Poll(handler, 1) > 0 && received }

	err := subscription.client.pollUntil(ctx, ready, func(timeout time.Duration) error {
		_, detail := SubscriptionImageSummary(subscription)
		return &PollOneMessageTimeoutError{
			RegistrationID: subscription.RegistrationID(),
			StreamID:       subscription.StreamID(),
			Timeout:        timeout,
			Detail:         detail,
		}
	})
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// PollReassembled receives one logical message via [FragmentAssembler].
//
// Zero active images returns [PollReassembledPreconditionError]; prefer
// [Subscription.WaitUntilImageCount] first.
//
// Undersized buf returns [PollBufferTooSmallError] after the message is
// consumed (not re-readable). Success returns payload length copied into buf.
//
// Waits up to [Config.PollTimeout]. Calls client DoWork between polls in
// invoker mode.
//
// Returns [ErrClientNotBound] when unbound.
func (subscription *Subscription) PollReassembled(ctx context.Context, buf []byte) (int, error) {
	if subscription.client == nil {
		return 0, ErrClientNotBound
	}

	// ctx.Err before zero-image precondition, matching other poll helpers.
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if !SubscriptionActiveImageCountAtLeast(subscription, 1) {
		return 0, &PollReassembledPreconditionError{
			RegistrationID: subscription.RegistrationID(),
			StreamID:       subscription.StreamID(),
			Detail:         "no active images",
		}
	}

	var (
		assembled  bool
		copied     bool
		payloadLen int
	)
	assembler := NewFragmentAssembler(AdaptFragmentHandlerBytes(
		func(payload []byte, _ *Header) {
			if assembled {
				return
			}
			payloadLen = len(payload)
			assembled = true
			if cap(buf) >= payloadLen {
				copy(buf[:payloadLen], payload)
				copied = true
			}
		},
	))

	handler := assembler.Handler()
	ready := func() bool {
		return subscription.Poll(handler, 1) > 0 && assembled
	}

	err := subscription.client.pollUntil(ctx, ready, func(timeout time.Duration) error {
		_, detail := SubscriptionImageSummary(subscription)
		return &PollReassembledTimeoutError{
			RegistrationID: subscription.RegistrationID(),
			StreamID:       subscription.StreamID(),
			Timeout:        timeout,
			Detail:         detail,
		}
	})
	if err != nil {
		return 0, err
	}
	if !copied {
		return 0, &PollBufferTooSmallError{Required: payloadLen, Have: cap(buf)}
	}
	return payloadLen, nil
}
