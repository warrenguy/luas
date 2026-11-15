package aeron

import (
	"context"
	"time"
)

// PublicationOfferer is implemented by Publication and ExclusivePublication.
//
// Offer-retry bodies and export_test fakes use this surface.
type PublicationOfferer interface {
	IsConnected() bool
	RegistrationID() int64
	StreamID() int32
	SessionID() int32
	Position() int64
	Offer([]byte) int64
}

// isOfferStallCandidate reports persistent BackPressured or AdminAction without
// position advance. NotConnected is excluded; transport wait uses OfferTimeoutError.
func isOfferStallCandidate(status int64) bool {
	return status == BackPressured || status == AdminAction
}

// OfferStallTimeout derives stall detection window from overall retry timeout.
//
// Matches harness OfferStallTimeout.
func OfferStallTimeout(timeout time.Duration) time.Duration {
	stall := timeout / 5
	if stall < time.Second && timeout >= 5*time.Second {
		return time.Second
	}
	return stall
}

func publicationIdentity(pub PublicationOfferer) (regID int64, streamID, sessionID int32) {
	return pub.RegistrationID(), pub.StreamID(), pub.SessionID()
}

func offerTerminalError(pub PublicationOfferer, status, position int64) *OfferTerminalError {
	regID, streamID, sessionID := publicationIdentity(pub)
	return &OfferTerminalError{
		Status:           status,
		PublicationRegID: regID,
		StreamID:         streamID,
		SessionID:        sessionID,
		Position:         position,
	}
}

func offerStallError(
	pub PublicationOfferer,
	status int64,
	position int64,
	stallTimeout time.Duration,
) *OfferStallError {
	regID, streamID, sessionID := publicationIdentity(pub)
	return &OfferStallError{
		Status:           status,
		PublicationRegID: regID,
		StreamID:         streamID,
		SessionID:        sessionID,
		Position:         position,
		StallTimeout:     stallTimeout,
	}
}

func offerTimeoutError(
	pub PublicationOfferer,
	status int64,
	timeout time.Duration,
) *OfferTimeoutError {
	regID, streamID, sessionID := publicationIdentity(pub)
	return &OfferTimeoutError{
		Status:           status,
		PublicationRegID: regID,
		StreamID:         streamID,
		SessionID:        sessionID,
		Position:         pub.Position(),
		Timeout:          timeout,
	}
}

// OfferRetriableObserver runs on BackPressured or AdminAction before retry.
//
// Not invoked for NotConnected.
type OfferRetriableObserver func(status int64)

// offerWithRetryObserved is the shared OfferWithRetryObserved body.
func offerWithRetryObserved(
	ctx context.Context,
	client *Client,
	pub PublicationOfferer,
	payload []byte,
	onRetriable OfferRetriableObserver,
) (int64, error) {
	if err := client.checkReentrant(); err != nil {
		return 0, err
	}
	deadline, effective, err := retryDeadline(ctx, client.publicationOfferTimeout)
	if err != nil {
		return 0, err
	}
	stallTimeout := OfferStallTimeout(effective)

	var (
		lastStatus    int64
		retryPosition = pub.Position()
		stallMarked   time.Time
	)

	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		result := pub.Offer(payload)
		if result > 0 {
			return result, nil
		}
		lastStatus = result

		position := pub.Position()
		if position > retryPosition {
			retryPosition = position
			stallMarked = time.Time{}
		} else if isOfferStallCandidate(result) {
			if stallMarked.IsZero() {
				stallMarked = time.Now()
			} else if time.Since(stallMarked) >= stallTimeout {
				return 0, PreferContextError(ctx, offerStallError(pub, lastStatus, position, stallTimeout))
			}
		} else if result == NotConnected {
			stallMarked = time.Time{}
		}

		if !IsOfferRetriable(result) {
			return 0, PreferContextError(ctx, offerTerminalError(pub, result, position))
		}

		if onRetriable != nil && (result == BackPressured || result == AdminAction) {
			onRetriable(result)
		}

		if client.HasConductorInvoker() {
			client.DoWork()
		}

		if result == BackPressured || result == NotConnected {
			time.Sleep(DefaultIdleSleepDuration)
		}
	}

	return 0, PreferContextError(ctx, offerTimeoutError(pub, lastStatus, effective))
}
