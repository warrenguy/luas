package aeron

import (
	"context"
	"time"
)

// WaitUntilConnected waits until IsConnected or [Config.PollTimeout].
//
// Calls client DoWork between checks in invoker mode.
//
// Returns [ErrClientNotBound] when unbound.
func (pub *Publication) WaitUntilConnected(ctx context.Context) error {
	if pub.client == nil {
		return ErrClientNotBound
	}
	return waitUntilConnected(ctx, pub.client, pub)
}

// OfferWithRetry is [Publication.OfferWithRetryObserved] with a nil observer.
func (pub *Publication) OfferWithRetry(
	ctx context.Context,
	payload []byte,
) (int64, error) {
	return pub.OfferWithRetryObserved(ctx, payload, nil)
}

// OfferWithRetryObserved offers payload until success (position > 0), timeout,
// stall, or terminal code.
//
// Retries up to [Config.PublicationOfferTimeout]. Calls client DoWork
// between offers in invoker mode.
//
// Retries while [IsOfferRetriable]: BackPressured, AdminAction, NotConnected.
// Sleeps [DefaultIdleSleepDuration] on BackPressured and NotConnected.
// Invokes onRetriable on BackPressured or AdminAction before retry.
//
// Stall without position advance for [OfferStallTimeout] of the effective bound
// maps to [OfferStallError]. Non-retriable codes map to [OfferTerminalError]
// ([PositionCodeName] in Error()). Expiry maps to [OfferTimeoutError]. All use
// [PreferContextError] against ctx.
//
// Not for cluster ingress or archive control loops ([IsIngressRetriable]).
//
// Returns [ErrClientNotBound] when unbound.
func (pub *Publication) OfferWithRetryObserved(
	ctx context.Context,
	payload []byte,
	onRetriable OfferRetriableObserver,
) (int64, error) {
	if pub.client == nil {
		return 0, ErrClientNotBound
	}
	return offerWithRetryObserved(ctx, pub.client, pub, payload, onRetriable)
}

// WaitUntilConnected matches [Publication.WaitUntilConnected].
func (ep *ExclusivePublication) WaitUntilConnected(ctx context.Context) error {
	if ep.client == nil {
		return ErrClientNotBound
	}
	return waitUntilConnected(ctx, ep.client, ep)
}

// OfferWithRetry is [ExclusivePublication.OfferWithRetryObserved] with a nil observer.
func (ep *ExclusivePublication) OfferWithRetry(
	ctx context.Context,
	payload []byte,
) (int64, error) {
	return ep.OfferWithRetryObserved(ctx, payload, nil)
}

// OfferWithRetryObserved matches [Publication.OfferWithRetryObserved].
func (ep *ExclusivePublication) OfferWithRetryObserved(
	ctx context.Context,
	payload []byte,
	onRetriable OfferRetriableObserver,
) (int64, error) {
	if ep.client == nil {
		return 0, ErrClientNotBound
	}
	return offerWithRetryObserved(ctx, ep.client, ep, payload, onRetriable)
}

func waitUntilConnected(
	ctx context.Context,
	client *Client,
	pub PublicationOfferer,
) error {
	return client.pollUntil(ctx, pub.IsConnected, func(timeout time.Duration) error {
		regID, streamID, sessionID := publicationIdentity(pub)
		return &WaitUntilConnectedTimeoutError{
			PublicationRegID: regID,
			StreamID:         streamID,
			SessionID:        sessionID,
			Timeout:          timeout,
			Position:         pub.Position(),
		}
	})
}
