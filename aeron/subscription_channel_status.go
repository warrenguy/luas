package aeron

import (
	"errors"

	"github.com/warrenguy/luas/aeron/cnc"
)

var errSubscriptionCountersNil = errors.New("aeron: subscription counters view is nil")

// ChannelStatus reads the driver receive-channel-status counter.
//
// Subscriptions without a channel-status counter report
// ChannelEndpointStatusActive.
func (subscription *Subscription) ChannelStatus() (int64, error) {
	if subscription.IsClosed() {
		return 0, errors.New("aeron: subscription is closed")
	}

	return subscription.channelStatusFor(subscription.channelStatusCounterID.Load())
}

func (subscription *Subscription) channelStatusFor(counterID int32) (int64, error) {
	if counterID == cnc.NullCounterID {
		return ChannelEndpointStatusActive, nil
	}

	if subscription.counters == nil {
		return 0, errSubscriptionCountersNil
	}
	return subscription.counters.CounterValue(counterID)
}

// TryResolveChannelEndpointPort returns the wire channel URI with an ephemeral
// endpoint=:0 port resolved from the local socket address counter.
//
// Behaviour: a channel without endpoint=:0 returns its URI unchanged. A
// control-mode=manual channel does not carry an endpoint= param, so it also
// returns unchanged. This method does not resolve a wildcard port inside a
// destination URI.
//
// Returns ("", false) when wildcard resolution is required but the receive
// endpoint is not yet active.
func (subscription *Subscription) TryResolveChannelEndpointPort() (string, bool) {
	if subscription == nil || subscription.IsClosed() {
		return "", false
	}

	channel := subscription.Channel()
	if !ChannelEndpointNeedsWildcardPortResolution(channel) {
		return channel, true
	}

	// channelStatusFor reports NullCounterID as active. Bind resolution needs a
	// real counter.
	channelStatusCounterID := subscription.channelStatusCounterID.Load()
	if channelStatusCounterID == cnc.NullCounterID {
		return "", false
	}

	status, err := subscription.channelStatusFor(channelStatusCounterID)
	if err != nil || status != ChannelEndpointStatusActive {
		return "", false
	}

	endpoint, ok := subscription.counters.FindLocalSocketAddress(
		ChannelEndpointStatusActive,
		channelStatusCounterID,
	)
	if !ok {
		return "", false
	}
	return ReplaceChannelEndpoint(channel, endpoint), true
}
